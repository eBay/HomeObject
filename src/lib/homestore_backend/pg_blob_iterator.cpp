#include "hs_homeobject.hpp"
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/settings/settings.hpp>
#include <sisl/metrics/metrics.hpp>
#include "generated/resync_blob_data_generated.h"
#include "generated/resync_pg_data_generated.h"
#include "generated/resync_shard_data_generated.h"
#include "replication_message.hpp"
#include "lib/homeobject_impl.hpp"
#include "hs_backend_config.hpp"

namespace homeobject {
HSHomeObject::PGBlobIterator::PGBlobIterator(HSHomeObject& home_obj, homestore::group_id_t group_id,
                                             uint64_t upto_lsn) :
        group_id(group_id), home_obj_(home_obj), snp_start_lsn_(upto_lsn) {
    auto pg = get_pg_metadata();
    pg_id = pg->pg_info_.id;
    repl_dev_ = static_cast< HS_PG* >(pg)->repl_dev_;
    metrics_ = make_unique< DonerSnapshotMetrics >(pg_id);
    max_batch_size_ = HS_BACKEND_DYNAMIC_CONFIG(max_snapshot_batch_size_mb) * Mi;
    if (max_batch_size_ == 0) { max_batch_size_ = DEFAULT_MAX_BATCH_SIZE_MB * Mi; }

    if (upto_lsn != 0) {
        // Iterate all shards and its blobs which have lsn <= upto_lsn
        for (auto& shard : pg->shards_) {
            auto shard_info = shard->info;
            if (shard_info.create_lsn <= upto_lsn) {
                auto v_chunk_id = home_obj_.get_shard_v_chunk_id(shard_info.id);
                RELEASE_ASSERT(v_chunk_id.has_value(), "v_chunk_id not found for shard_id={}", shard_info.id);

                /*
                 * Snapshot metadata may reflect the leader's latest shard state, even when that
                 * state transition happened after the snapshot LSN cutoff. If such a shard is
                 * treated as sealed during snapshot apply, the receiver may release the backing
                 * chunk too early. That chunk can then be reclaimed or relocated before log replay
                 * catches up, leaving later writes to use stale physical-chunk information and
                 * eventually fail during commit.
                 *
                 * To keep snapshot state consistent with the requested cutoff, convert any shard
                 * whose sealed_lsn is beyond upto_lsn back to OPEN for iteration purposes and follower will received
                 * and commit the seal_shard log for this shard after the snapshot apply is completed, where the shard
                 * state and sealed_lsn will be updated to the correct value.
                 */

                if (shard_info.state == ShardInfo::State::SEALED && shard_info.sealed_lsn > upto_lsn) {
                    shard_info.state = ShardInfo::State::OPEN;
                    shard_info.sealed_lsn = INT64_MAX;
                }

                shard_list_.emplace_back(shard_info, v_chunk_id.value());
            }
        }
        // Sort shard list by <vchunkid, lsn> to ensure open shards positioned after sealed shards within each chunk
        std::ranges::sort(shard_list_, [](const ShardEntry& a, const ShardEntry& b) {
            return a.v_chunk_num != b.v_chunk_num ? a.v_chunk_num < b.v_chunk_num
                                                  : a.info.create_lsn < b.info.create_lsn;
        });
    }
    LOGI("Created PGBlobIterator: pg={}, snapshot_lsn={}, shards={}, batch_size={}", pg_id, snp_start_lsn_,
         shard_list_.size(), max_batch_size_);
}

// result represents if the objId is valid and the cursors are updated
bool HSHomeObject::PGBlobIterator::update_cursor(const objId& id) {
    std::lock_guard lock(op_mut_);
    if (stopped_) {
        LOGD("Rejecting cursor update on stopped PGBlobIterator: pg={}, requested_obj={}", pg_id, id.to_string());
        return false;
    }

    if (cur_batch_start_time_ != Clock::time_point{}) {
        HISTOGRAM_OBSERVE(*metrics_, snp_dnr_batch_e2e_latency, get_elapsed_time_ms(cur_batch_start_time_));
    }
    cur_batch_start_time_ = Clock::now();

    if (id.value == LAST_OBJ_ID) { return true; }

    // Resend batch
    if (id.value == cur_obj_id.value) {
        LOGI("Resending resync object: pg={}, obj={}", pg_id, id.to_string());
        COUNTER_INCREMENT(*metrics_, snp_dnr_resend_count, 1);
        return true;
    }

    // If cur_obj_id_ == 0|0 (PG meta), this may be a request for resuming from specific shard
    if (cur_obj_id.shard_seq_num == 0 && id.shard_seq_num != 0 && id.batch_id == 0) {
        bool found = false;
        for (size_t i = 0; i < shard_list_.size(); i++) {
            if (get_sequence_num_from_shard_id(shard_list_[i].info.id) == id.shard_seq_num) {
                found = true;
                cur_shard_idx_ = i;
                cur_start_blob_idx_ = 0;
                cur_batch_blob_count_ = 0;
                break;
            }
        }
        if (found) {
            cur_obj_id = id;
            LOGI("Resuming resync at shard: pg={}, shard_seq=0x{:x}, shard_index={}", pg_id, id.shard_seq_num,
                 cur_shard_idx_);
        } else {
            LOGE("Cannot resume resync: pg={}, requested_obj={} is not in the snapshot shard list", pg_id,
                 id.to_string());
        }
        return found;
    }

    auto next_obj_id = expected_next_obj_id();
    if (id.value != next_obj_id.value) {
        LOGE("Invalid resync cursor: pg={}, current_obj={}, expected_obj={}, requested_obj={}", pg_id,
             cur_obj_id.to_string(), next_obj_id.to_string(), id.to_string());
        return false;
    }
    // next shard
    if (cur_obj_id.shard_seq_num != next_obj_id.shard_seq_num) {
        cur_shard_idx_++;
        cur_start_blob_idx_ = 0;
        cur_batch_blob_count_ = 0;
    } else {
        // next batch
        cur_start_blob_idx_ = cur_start_blob_idx_ + cur_batch_blob_count_;
        cur_batch_blob_count_ = 0;
    }
    cur_obj_id = id;
    LOGD("Advanced resync cursor: pg={}, obj={}, shard_index={}, blob_index={}", pg_id, id.to_string(),
         cur_shard_idx_, cur_start_blob_idx_);
    return true;
}

void HSHomeObject::PGBlobIterator::reset_cursor() {
    std::lock_guard lock(op_mut_);
    if (stopped_) {
        LOGD("Ignoring cursor reset on stopped PGBlobIterator: pg={}", pg_id);
        return;
    }

    cur_obj_id = {0, 0};
    cur_shard_idx_ = -1;
    std::vector< BlobInfo > cur_blob_list_{0};
    cur_start_blob_idx_ = 0;
    cur_batch_blob_count_ = 0;
    cur_batch_start_time_ = Clock::time_point{};
    LOGI("Reset resync cursor: pg={}", pg_id);
}

objId HSHomeObject::PGBlobIterator::expected_next_obj_id() const {
    // next batch
    if (cur_start_blob_idx_ + cur_batch_blob_count_ < cur_blob_list_.size()) {
        return objId(cur_obj_id.shard_seq_num, cur_obj_id.batch_id + 1);
    }
    // handle empty shard
    if (cur_obj_id.batch_id == 0 && cur_blob_list_.empty()) {
        return objId(cur_obj_id.shard_seq_num, cur_obj_id.batch_id + 1);
    }
    // next shard
    if (cur_shard_idx_ < static_cast< int64_t >(shard_list_.size() - 1)) {
        auto next_shard_seq_num = shard_list_[cur_shard_idx_ + 1].info.id & 0xFFFFFFFFFFFF;
        return objId(next_shard_seq_num, 0);
    }
    return objId(LAST_OBJ_ID);
}

PG* HSHomeObject::PGBlobIterator::get_pg_metadata() const {
    std::scoped_lock lock_guard(home_obj_._pg_lock);
    auto iter = home_obj_._pg_map.begin();
    for (; iter != home_obj_._pg_map.end(); iter++) {
        if (iter->second->pg_info_.replica_set_uuid == group_id) { break; }
    }

    RELEASE_ASSERT(iter != home_obj_._pg_map.end(), "PG not found replica_set_uuid={}",
                   boost::uuids::to_string(group_id));
    return iter->second.get();
}

bool HSHomeObject::PGBlobIterator::create_pg_snapshot_data(sisl::io_blob_safe& meta_blob) {
    std::lock_guard lock(op_mut_);
    if (stopped_) {
        LOGD("Rejecting PG metadata request on stopped PGBlobIterator: pg={}", pg_id);
        return false;
    }

#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("pg_blob_iterator_create_snapshot_data_error")) {
        LOGD("Simulating resync PG metadata creation failure: pg={}", pg_id);
        return false;
    }
#endif
    auto pg = home_obj_._pg_map[pg_id].get();
    if (pg == nullptr) {
        LOGE("Cannot create resync PG metadata: pg={} not found", pg_id);
        return false;
    }
    auto pg_info = pg->pg_info_;

    std::vector< std::uint8_t > uuid(pg_info.replica_set_uuid.begin(), pg_info.replica_set_uuid.end());

    std::vector< ::flatbuffers::Offset< homeobject::Member > > members;
    for (auto& member : pg_info.members) {
        auto id = std::vector< std::uint8_t >(member.id.begin(), member.id.end());
        members.push_back(CreateMemberDirect(builder_, &id, member.name.c_str(), member.priority));
    }
    std::vector< uint64_t > shard_ids;
    auto total_bytes =
        pg->durable_entities().total_occupied_blk_count.load(std::memory_order_relaxed) * repl_dev_->get_blk_size();
    auto total_blobs = pg->durable_entities().active_blob_count.load(std::memory_order_relaxed);
    for (auto& shard : shard_list_) {
        shard_ids.push_back(shard.info.id);
    }

    auto pg_entry =
        CreateResyncPGMetaDataDirect(builder_, pg_info.id, &uuid, pg_info.size, pg_info.expected_member_num,
                                     pg_info.chunk_size, pg->durable_entities().blob_sequence_num,
                                     pg->shard_sequence_num_, &members, &shard_ids, total_blobs, total_bytes);
    builder_.FinishSizePrefixed(pg_entry);

    pack_resync_message(meta_blob, SyncMessageType::PG_META);
    LOGI("Created resync PG metadata: pg={}, shards={}, active_blobs={}, occupied_bytes={}", pg_id,
         shard_ids.size(), total_blobs, total_bytes);
    return true;
}

bool HSHomeObject::PGBlobIterator::generate_shard_blob_list() {
    std::lock_guard lock(op_mut_);
    if (stopped_) {
        LOGD("Rejecting shard blob-list request on stopped PGBlobIterator: pg={}", pg_id);
        return false;
    }

#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("pg_blob_iterator_generate_shard_blob_list_error")) {
        LOGD("Simulating resync shard blob-list query failure: pg={}, shard_seq=0x{:x}", pg_id,
             cur_obj_id.shard_seq_num);
        return false;
    }
#endif
    auto r = home_obj_.query_blobs_in_shard(pg_id, cur_obj_id.shard_seq_num, 0, UINT64_MAX);
    if (!r) {
        LOGE("Failed to query resync shard blobs: pg={}, shard_seq=0x{:x}, error={}", pg_id,
             cur_obj_id.shard_seq_num, r.error());
        return false;
    }
    cur_blob_list_ = r.value();
    LOGD("Prepared resync shard blob list: pg={}, shard_seq=0x{:x}, blobs={}", pg_id, cur_obj_id.shard_seq_num,
         cur_blob_list_.size());
    return true;
}

bool HSHomeObject::PGBlobIterator::create_shard_snapshot_data(sisl::io_blob_safe& meta_blob) {
    std::lock_guard lock(op_mut_);
    if (stopped_) {
        LOGD("Rejecting shard metadata request on stopped PGBlobIterator: pg={}", pg_id);
        return false;
    }

    auto shard = shard_list_[cur_shard_idx_];
    std::vector< uint8_t > meta_bytes(shard.info.meta, shard.info.meta + ShardInfo::meta_length);
    auto shard_entry = CreateResyncShardMetaDataDirect(
        builder_, shard.info.id, pg_id, static_cast< uint8_t >(shard.info.state), shard.info.create_lsn,
        shard.info.created_time, shard.info.last_modified_time, shard.info.total_capacity_bytes, shard.v_chunk_num,
        &meta_bytes, shard.info.sealed_lsn);

    builder_.FinishSizePrefixed(shard_entry);

    pack_resync_message(meta_blob, SyncMessageType::SHARD_META);
    LOGI("Created resync shard metadata: pg={}, shard_id=0x{:x}, shard_seq=0x{:x}, state={}, blobs={}", pg_id,
         shard.info.id, cur_obj_id.shard_seq_num, static_cast< uint8_t >(shard.info.state), cur_blob_list_.size());
    prefetch_blobs_snapshot_data();
    return true;
}

typedef HSHomeObject::PGBlobIterator::blob_read_result blob_read_result;
BlobManager::AsyncResult< blob_read_result > HSHomeObject::PGBlobIterator::load_blob_data(const BlobInfo& blob_info) {
    return load_blob_data_with_blkid(blob_info.shard_id, blob_info.blob_id, blob_info.pbas);
}

BlobManager::AsyncResult< blob_read_result >
HSHomeObject::PGBlobIterator::load_blob_data_with_blkid(shard_id_t shard_id, blob_id_t blob_id,
                                                        homestore::MultiBlkId blkid) {
    auto const total_size = blkid.blk_count() * repl_dev_->get_blk_size();
    sisl::io_blob_safe read_buf{total_size, io_align};

    sisl::sg_list sgs;
    sgs.size = total_size;
    sgs.iovs.emplace_back(iovec{.iov_base = read_buf.bytes(), .iov_len = read_buf.size()});

    LOGT("Reading resync blob: pg={}, shard=0x{:x}, blob={}, blkid={}, bytes={}",
         (shard_id >> homeobject::shard_width), (shard_id & homeobject::shard_mask), blob_id, blkid.to_string(),
         total_size);
    return repl_dev_->async_read(blkid, sgs, total_size)
        .thenValue([this, blob_id, shard_id, blkid, read_buf = std::move(read_buf)](
                       auto&& result) mutable -> BlobManager::AsyncResult< blob_read_result > {
            if (result) {
                LOGE("Failed to read resync blob: pg={}, shard=0x{:x}, blob={}, blkid={}, error={}",
                     (shard_id >> homeobject::shard_width), (shard_id & homeobject::shard_mask), blob_id,
                     blkid.to_string(), result.value());
                return folly::makeUnexpected(BlobError(BlobErrorCode::READ_FAILED));
            }

            if (home_obj_.verify_blob(read_buf.cbytes(), shard_id, blob_id)) {
                LOGT("Read resync blob: pg={}, shard=0x{:x}, blob={}", (shard_id >> homeobject::shard_width),
                     (shard_id & homeobject::shard_mask), blob_id);
                return blob_read_result(blob_id, std::move(read_buf), ResyncBlobState::NORMAL);
            }

            // verify_blob failed — check if GC moved the blob to a new blkid since cur_blob_list_ was captured.
            auto index_table = home_obj_.get_index_table(pg_id);
            if (!index_table) {
                LOGE(
                    "Cannot resolve resync blob verification failure: pg={} no longer exists, shard_id=0x{:x}, blob={}",
                    pg_id, shard_id, blob_id);
                return folly::makeUnexpected(BlobError(BlobErrorCode::READ_FAILED));
            }
            auto current_pbas = home_obj_.get_blob_from_index_table(index_table, shard_id, blob_id);
            if (!current_pbas) {
                // Blob was deleted concurrently after generate_shard_blob_list captured its pbas.
                // Do not send stale bytes as CORRUPTED — signal READ_FAILED so the snapshot restarts
                // and generate_shard_blob_list picks up tombstone_pbas, skipping the blob cleanly.
                LOGW("Resync blob was deleted during read; restarting snapshot: pg={}, shard_id=0x{:x}, blob={}",
                     pg_id, shard_id, blob_id);
                return folly::makeUnexpected(BlobError(BlobErrorCode::READ_FAILED));
            }
            if (current_pbas.value() == blkid) {
                // blkid unchanged — genuinely corrupted data at this location.
                // Metrics for corrupted blobs are handled on the follower side.
                LOGE("Resync blob verification failed: pg={}, shard=0x{:x}, blob={}, blkid={}",
                     (shard_id >> homeobject::shard_width), (shard_id & homeobject::shard_mask), blob_id,
                     blkid.to_string());
                return blob_read_result(blob_id, std::move(read_buf), ResyncBlobState::CORRUPTED);
            }

            // GC moved the blob — retry with the updated blkid. Folly flattens the returned future.
            LOGI("Resync blob relocated by GC during read; retrying: pg={}, shard_id=0x{:x}, blob={}, old_blkid={}, new_blkid={}",
                 pg_id, shard_id, blob_id, blkid.to_string(), current_pbas.value().to_string());
            return load_blob_data_with_blkid(shard_id, blob_id, current_pbas.value());
        });
}

bool HSHomeObject::PGBlobIterator::prefetch_blobs_snapshot_data() {
    auto total_blobs = 0;
    auto skipped_blobs = 0;
    // limit inflight prefect data to 2x of max_batch_size.
    std::vector< BlobInfo > prefetch_list;
    auto idx = cur_start_blob_idx_;
    // On batch resend, retained look-ahead may already consume part of the 2x budget. Allow missing blobs before the
    // earliest retained blob to bypass the limit so the current batch can always be rebuilt.
    const auto prefetch_frontier = prefetched_blobs_.empty() ? blob_id_t{0} : prefetched_blobs_.begin()->first;
    LOGT("Prefetching blobs: pg={}, shard_seq=0x{:x}, cursor_blob={}, frontier={}, inflight_bytes={}",
         pg_id, cur_obj_id.shard_seq_num, cur_start_blob_idx_, prefetch_frontier, inflight_prefetch_bytes_);
    while (idx < cur_blob_list_.size() &&
           (inflight_prefetch_bytes_ < max_batch_size_ * 2 || cur_blob_list_[idx].blob_id < prefetch_frontier)) {
        auto info = cur_blob_list_[idx++];
        total_blobs++;
        // handle deleted object
        if (info.pbas == tombstone_pbas) {
            LOGT("Skipping deleted resync blob: pg={}, shard=0x{:x}, blob={}",
                 (info.shard_id >> homeobject::shard_width), (info.shard_id & homeobject::shard_mask), info.blob_id);
            // ignore
            skipped_blobs++;
            continue;
        }
        if (prefetched_blobs_.contains(info.blob_id)) {
            LOGT("Retaining prefetched resync blob: pg={}, blob={}", pg_id, info.blob_id);
            skipped_blobs++;
            continue;
        }
        inflight_prefetch_bytes_ += info.pbas.blk_count() * repl_dev_->get_blk_size();
        prefetch_list.emplace_back(info);
    }
    // POC: sort the prefetch_list by pbas, trying to let IO submitted to disk more sequential.
    std::sort(prefetch_list.begin(), prefetch_list.end(),
              [](const BlobInfo& a, const BlobInfo& b) { return a.pbas < b.pbas; });
    for (auto info : prefetch_list) {
#ifdef _PRERELEASE
        if (iomgr_flip::instance()->test_flip("pg_blob_iterator_load_blob_data_error")) {
            LOGD("Simulating resync blob prefetch failure: pg={}, blob={}", pg_id, info.blob_id);
            prefetched_blobs_.emplace(info.blob_id, folly::makeUnexpected(BlobError(BlobErrorCode::READ_FAILED)));
            continue;
        }
        auto delay = iomgr_flip::instance()->get_test_flip< long >("simulate_read_snapshot_load_blob_delay",
                                                                   static_cast< long >(info.blob_id));
        if (delay) {
            LOGD("Simulating resync blob read delay: pg={}, blob={}, delay_ms={}", pg_id, info.blob_id, delay.get());
            std::this_thread::sleep_for(std::chrono::milliseconds(delay.get()));
        }
#endif
        auto blob_start = Clock::now();

        LOGT("Submitting resync blob read: pg={}, shard_id=0x{:x}, blob={}", pg_id, info.shard_id, info.blob_id);
        // Fixme: Re-enable retries uint8_t retries = HS_BACKEND_DYNAMIC_CONFIG(snapshot_blob_load_retry);
        prefetched_blobs_.emplace(
            info.blob_id,
            std::move(load_blob_data(info))
                .via(folly::getKeepAliveToken(folly::InlineExecutor::instance()))
                .thenValue(
                    [&, info, blob_start](auto&& result) mutable -> BlobManager::AsyncResult< blob_read_result > {
                        if (result.hasError() && result.error().code == BlobErrorCode::READ_FAILED) {
                            LOGE("Failed to prefetch resync blob: pg={}, shard=0x{:x}, blob={}, blkid={}",
                                 info.shard_id >> homeobject::shard_width, info.shard_id & homeobject::shard_mask,
                                 info.blob_id, info.pbas.to_string());
                            COUNTER_INCREMENT(*metrics_, snp_dnr_error_count, 1);
                        } else {
                            LOGT("Prefetched resync blob: pg={}, blob={}, blkid={}", pg_id, info.blob_id,
                                 info.pbas.to_string());
                            HISTOGRAM_OBSERVE(*metrics_, snp_dnr_blob_process_latency, get_elapsed_time_us(blob_start));
                        }
                        return result;
                    }));
    }
    LOGD("Resync prefetch window: pg={}, shard_seq=0x{:x}, cursor_blob={}, frontier={}, submitted_blobs={}, skipped_blobs={}, inflight_bytes={}, limit_bytes={}",
         pg_id, cur_obj_id.shard_seq_num, cur_start_blob_idx_, prefetch_frontier, prefetch_list.size(), skipped_blobs,
         inflight_prefetch_bytes_, max_batch_size_ * 2);
    return true;
}

bool HSHomeObject::PGBlobIterator::create_blobs_snapshot_data(sisl::io_blob_safe& data_blob) {
    std::lock_guard lock(op_mut_);
    if (stopped_) {
        LOGD("Rejecting blob batch request on stopped PGBlobIterator: pg={}", pg_id);
        return false;
    }

    auto batch_start = Clock::now();
    std::vector< ::flatbuffers::Offset< ResyncBlobData > > blob_entries;
    bool end_of_shard = false;
    uint64_t total_bytes = 0;
    auto idx = cur_start_blob_idx_;
    auto total_blobs = 0;
    auto skipped_blobs = 0;
    auto fetched_blobs = 0;
    bool hit_error = false;

    // Prefetch and load blobs data
    {
        prefetch_blobs_snapshot_data();

        while (total_bytes < max_batch_size_ && idx < cur_blob_list_.size()) {
            auto info = cur_blob_list_[idx++];
            total_blobs++;
            // handle deleted object
            if (info.pbas == tombstone_pbas) {
                LOGT("Skipping deleted resync blob: pg={}, shard=0x{:x}, blob={}",
                    info.shard_id >> homeobject::shard_width, info.shard_id & homeobject::shard_mask, info.blob_id);
                // ignore
                skipped_blobs++;
                continue;
            }

            auto it = prefetched_blobs_.find(info.blob_id);
            if (it == prefetched_blobs_.end()) {
                hit_error = true;
                LOGE("Resync batch cannot find prefetched blob: pg={}, shard_seq=0x{:x}, batch={}, blob={}, cursor_blob={}, inflight_bytes={}, prefetched_blobs={}",
                     pg_id, cur_obj_id.shard_seq_num, cur_obj_id.batch_id, info.blob_id, cur_start_blob_idx_,
                     inflight_prefetch_bytes_, prefetched_blobs_.size());
                break;
            }
            auto const expect_blob_size = info.pbas.blk_count() * repl_dev_->get_blk_size();
            auto res = std::move(it->second).get();
            prefetched_blobs_.erase(it);
            inflight_prefetch_bytes_ -= expect_blob_size;

            if (res.hasError()) {
                LOGE("Resync batch failed to retrieve blob: pg={}, shard_seq=0x{:x}, batch={}, blob={}, error={}",
                     pg_id, cur_obj_id.shard_seq_num, cur_obj_id.batch_id, info.blob_id, res.error());
                hit_error = true;
                break;
            }
            std::vector< uint8_t > data(res->blob_.cbytes(), res->blob_.cbytes() + res->blob_.size());
            blob_entries.push_back(CreateResyncBlobDataDirect(builder_, res->blob_id_, (uint8_t)res->state_, &data));
            total_bytes += expect_blob_size;
            fetched_blobs++;
        }
    }

    if (skipped_blobs + fetched_blobs != total_blobs) {
        LOGE("Incomplete resync batch: pg={}, shard_seq=0x{:x}, batch={}, examined_blobs={}, skipped_blobs={}, expected_blobs={}, fetched_blobs={}",
             pg_id, cur_obj_id.shard_seq_num, cur_obj_id.batch_id, total_blobs, skipped_blobs,
             total_blobs - skipped_blobs, fetched_blobs);
        hit_error = true;
    }

    if (hit_error) {
        builder_.Clear();
        return false;
    }

    // should include the deleted blobs
    cur_batch_blob_count_ = idx - cur_start_blob_idx_;
    if (idx == cur_blob_list_.size()) { end_of_shard = true; }
    builder_.FinishSizePrefixed(CreateResyncBlobDataBatchDirect(builder_, &blob_entries, end_of_shard));

    LOGI("Created resync shard batch: pg={}, shard_seq=0x{:x}, batch={}, blobs={}, skipped_blobs={}, bytes={}, end_of_shard={}, next_blob={}",
         pg_id, cur_obj_id.shard_seq_num, cur_obj_id.batch_id, blob_entries.size(), skipped_blobs, total_bytes,
         end_of_shard, idx);

    COUNTER_INCREMENT(*metrics_, snp_dnr_load_blob, blob_entries.size());
    COUNTER_INCREMENT(*metrics_, snp_dnr_load_bytes, total_bytes);
    pack_resync_message(data_blob, SyncMessageType::SHARD_BATCH);
    HISTOGRAM_OBSERVE(*metrics_, snp_dnr_batch_process_latency, get_elapsed_time_ms(batch_start));
    return true;
}

void HSHomeObject::PGBlobIterator::pack_resync_message(sisl::io_blob_safe& dest_blob, SyncMessageType type) {
    SyncMessageHeader header;
    header.msg_type = type;
    header.payload_size = builder_.GetSize();
    header.payload_crc = crc32_ieee(init_crc32, builder_.GetBufferPointer(), builder_.GetSize());
    header.seal();
    LOGT("Packed resync message: pg={}, type={}, payload_bytes={}, payload_crc=0x{:x}", pg_id,
         static_cast< uint32_t >(type), header.payload_size, header.payload_crc);

    dest_blob = sisl::io_blob_safe{static_cast< unsigned int >(builder_.GetSize() + sizeof(SyncMessageHeader))};
    std::memcpy(dest_blob.bytes(), &header, sizeof(SyncMessageHeader));
    std::memcpy(dest_blob.bytes() + sizeof(SyncMessageHeader), builder_.GetBufferPointer(), builder_.GetSize());

    // reset builder for next message
    builder_.Clear();
}

void HSHomeObject::PGBlobIterator::stop() {
    // Acquire operation lock to ensure no operations are in progress
    // Mutex is sufficient here as operations of a PGBlobIterator are called sequentially
    std::lock_guard lock(op_mut_);

    LOGI("Stopping PGBlobIterator: pg={}, prefetched_blobs={}, inflight_bytes={}", pg_id, prefetched_blobs_.size(),
         inflight_prefetch_bytes_);
    // Set stopped flag to prevent future operations
    stopped_ = true;

    // Wait for all inflight prefetch blobs to finish and drain the data
    for (auto& blob : prefetched_blobs_) {
        LOGT("Draining prefetched resync blob: pg={}, blob={}", pg_id, blob.first);
        std::move(blob.second).get();
    }
    prefetched_blobs_.clear();
    inflight_prefetch_bytes_ = 0;

    // Clear the builder to ensure no partial data remains
    builder_.Clear();

    LOGI("Stopped PGBlobIterator: pg={}", pg_id);
}

} // namespace homeobject
