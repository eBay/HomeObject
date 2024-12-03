#include "hs_homeobject.hpp"
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/settings/settings.hpp>
#include "generated/resync_blob_data_generated.h"
#include "generated/resync_pg_data_generated.h"
#include "generated/resync_shard_data_generated.h"
#include "replication_message.hpp"
#include "lib/homeobject_impl.hpp"
#include "hs_backend_config.hpp"

namespace homeobject {
HSHomeObject::PGBlobIterator::PGBlobIterator(HSHomeObject& home_obj, homestore::group_id_t group_id,
                                             uint64_t upto_lsn) :
        home_obj_(home_obj), group_id_(group_id), snp_start_lsn_(upto_lsn) {
    auto pg = get_pg_metadata();
    pg_id_ = pg->pg_info_.id;
    repl_dev_ = static_cast< HS_PG* >(pg)->repl_dev_;
    max_batch_size_ = HS_BACKEND_DYNAMIC_CONFIG(max_snapshot_batch_size_mb) * Mi;
    if (max_batch_size_ == 0) { max_batch_size_ = DEFAULT_MAX_BATCH_SIZE_MB * Mi; }

    if (upto_lsn != 0) {
        // Iterate all shards and its blobs which have lsn <= upto_lsn
        for (auto& shard : pg->shards_) {
            if (shard->info.lsn <= upto_lsn) {
                auto v_chunk_id = home_obj_.get_shard_v_chunk_id(shard->info.id);
                shard_list_.emplace_back(shard->info, v_chunk_id.value());
            }
        }
        // sort shard list by <vchunkid, lsn> to ensure open shards positioned after sealed shards
        std::ranges::sort(shard_list_, [](ShardEntry& a, ShardEntry& b) {
            return a.v_chunk_num != b.v_chunk_num ? a.v_chunk_num < b.v_chunk_num : a.info.lsn < b.info.lsn;
        });
    }
}

//result represents if the objId is valid and the cursors are updated
bool HSHomeObject::PGBlobIterator::update_cursor(objId id) {
    if (id.value == LAST_OBJ_ID) { return true; }
    //resend batch
    if (id.value == cur_obj_id_.value) { return true; }
    auto next_obj_id = expected_next_obj_id();
    if (id.value != next_obj_id.value) {
        LOGE("invalid objId, expected={}, actual={}", next_obj_id.to_string(), id.to_string());
        return false;
    }
    //next shard
    if (cur_obj_id_.shard_seq_num != next_obj_id.shard_seq_num) {
        cur_shard_idx_++;
        cur_start_blob_idx_ = 0;
        cur_batch_blob_count_ = 0;
    } else {
        //next batch
        cur_start_blob_idx_ = cur_start_blob_idx_ + cur_batch_blob_count_;
        cur_batch_blob_count_ = 0;
    }
    cur_obj_id_ = id;
    return true;
}

objId HSHomeObject::PGBlobIterator::expected_next_obj_id() {
    //next batch
    if (cur_start_blob_idx_ + cur_batch_blob_count_ < cur_blob_list_.size()) {
        return objId(cur_obj_id_.shard_seq_num, cur_obj_id_.batch_id + 1);
    }
    //next shard
    if (cur_shard_idx_ < shard_list_.size() - 1) {
        auto next_shard_seq_num = shard_list_[cur_shard_idx_ + 1].info.id & 0xFFFFFFFFFFFF;
        return objId(next_shard_seq_num, 0);
    }
    return objId(LAST_OBJ_ID);
}

PG* HSHomeObject::PGBlobIterator::get_pg_metadata() {
    std::scoped_lock lock_guard(home_obj_._pg_lock);
    auto iter = home_obj_._pg_map.begin();
    for (; iter != home_obj_._pg_map.end(); iter++) {
        if (iter->second->pg_info_.replica_set_uuid == group_id_) { break; }
    }

    RELEASE_ASSERT(iter != home_obj_._pg_map.end(), "PG not found replica_set_uuid={}",
                   boost::uuids::to_string(group_id_));
    return iter->second.get();
}

bool HSHomeObject::PGBlobIterator::create_pg_snapshot_data(sisl::io_blob_safe& meta_blob) {
    auto pg = home_obj_._pg_map[pg_id_].get();
    if (pg == nullptr) {
        LOGE("PG not found in pg_map, pg_id={}", pg_id_);
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
    for (auto& shard : shard_list_) {
        shard_ids.push_back(shard.info.id);
    }

    auto pg_entry = CreateResyncPGMetaDataDirect(builder_, pg_info.id, &uuid, pg->durable_entities().blob_sequence_num,
                                                 pg->shard_sequence_num_, &members, &shard_ids);
    builder_.FinishSizePrefixed(pg_entry);

    pack_resync_message(meta_blob, SyncMessageType::PG_META);
    return true;
}

bool HSHomeObject::PGBlobIterator::generate_shard_blob_list() {
    auto r = home_obj_.query_blobs_in_shard(pg_id_, cur_obj_id_.shard_seq_num, 0, UINT64_MAX);
    if (!r) { return false; }
    cur_blob_list_ = r.value();
    return true;
}

bool HSHomeObject::PGBlobIterator::create_shard_snapshot_data(sisl::io_blob_safe& meta_blob) {
    auto shard = shard_list_[cur_shard_idx_];
    auto shard_entry = CreateResyncShardMetaData(
        builder_, shard.info.id, pg_id_, static_cast< uint8_t >(shard.info.state), shard.info.lsn,
        shard.info.created_time, shard.info.last_modified_time, shard.info.total_capacity_bytes, shard.v_chunk_num);

    builder_.FinishSizePrefixed(shard_entry);

    pack_resync_message(meta_blob, SyncMessageType::SHARD_META);
    return true;
}

BlobManager::AsyncResult< sisl::io_blob_safe > HSHomeObject::PGBlobIterator::load_blob_data(
    const BlobInfo& blob_info, ResyncBlobState& state) {
    auto shard_id = blob_info.shard_id;
    auto blob_id = blob_info.blob_id;
    auto blkid = blob_info.pbas;
    auto const total_size = blob_info.pbas.blk_count() * repl_dev_->get_blk_size();
    sisl::io_blob_safe read_buf{total_size, io_align};

    sisl::sg_list sgs;
    sgs.size = total_size;
    sgs.iovs.emplace_back(iovec{.iov_base = read_buf.bytes(), .iov_len = read_buf.size()});

    LOGD("Blob get request: shard_id={}, blob_id={}, blkid={}", shard_id, blob_id, blkid.to_string());
    return repl_dev_->async_read(blkid, sgs, total_size)
                    .thenValue([this, blob_id, shard_id, &state, read_buf = std::move(read_buf)]
                    (auto&& result) mutable -> BlobManager::AsyncResult< sisl::io_blob_safe > {
                            if (result) {
                                LOGE("Failed to get blob, shard_id={}, blob_id={}, err={}", shard_id, blob_id,
                                     result.value());
                                return folly::makeUnexpected(BlobError(BlobErrorCode::READ_FAILED));
                            }

                            BlobHeader const* header = r_cast< BlobHeader const* >(read_buf.cbytes());
                            if (!header->valid()) {
                                //TODO add metrics
                                LOGE("Invalid header found, shard_id={}, blob_id={}, [header={}]", shard_id, blob_id,
                                     header->to_string());
                                state = ResyncBlobState::CORRUPTED;
                                return std::move(read_buf);
                            }

                            if (header->shard_id != shard_id) {
                                //TODO add metrics
                                LOGE("Invalid shard_id in header, shard_id={}, blob_id={}, [header={}]", shard_id,
                                     blob_id, header->to_string());
                                state = ResyncBlobState::CORRUPTED;
                                return std::move(read_buf);
                            }

                            std::string user_key = header->user_key_size
                                ? std::string((const char*)(read_buf.bytes() + sizeof(BlobHeader)),
                                              (size_t)header->user_key_size)
                                : std::string{};

                            uint8_t const* blob_bytes = read_buf.bytes() + header->data_offset;
                            uint8_t computed_hash[BlobHeader::blob_max_hash_len]{};
                            home_obj_.compute_blob_payload_hash(header->hash_algorithm, blob_bytes, header->blob_size,
                                                                uintptr_cast(user_key.data()), header->user_key_size,
                                                                computed_hash,
                                                                BlobHeader::blob_max_hash_len);
                            if (std::memcmp(computed_hash, header->hash, BlobHeader::blob_max_hash_len) != 0) {
                                LOGE(
                                    "corrupted blob found, shard_id: {}, blob_id: {}, hash mismatch header [{}] [computed={:np}]",
                                    shard_id, blob_id, header->to_string(),
                                    spdlog::to_hex(computed_hash, computed_hash + BlobHeader::blob_max_hash_len));
                                state = ResyncBlobState::CORRUPTED;
                            }

                            LOGD("Blob get success: shard_id={}, blob_id={}", shard_id, blob_id);
                            return std::move(read_buf);
                        });
}

bool HSHomeObject::PGBlobIterator::create_blobs_snapshot_data(sisl::io_blob_safe& data_blob) {
    std::vector< ::flatbuffers::Offset< ResyncBlobData > > blob_entries;

    bool end_of_shard = false;
    uint64_t total_bytes = 0;
    auto idx = cur_start_blob_idx_;

    while (total_bytes < max_batch_size_ && idx < cur_blob_list_.size()) {
        auto info = cur_blob_list_[idx++];
        ResyncBlobState state = ResyncBlobState::NORMAL;
        //handle deleted object
        if (info.pbas == tombstone_pbas) {
            LOGT("Blob is deleted: shard_id={}, blob_id={}, blkid={}", info.shard_id, info.blob_id,
                 info.pbas.to_string());
            // ignore
            continue;
        }

        sisl::io_blob_safe blob;
        auto retries = HS_BACKEND_DYNAMIC_CONFIG(snapshot_blob_load_retry);
        for (int i = 0; i < retries; i++) {
            auto result = load_blob_data(info, state).get();
            if (result.hasError() && result.error().code == BlobErrorCode::READ_FAILED) {
                LOGW("Failed to retrieve blob for shard={} blob={} pbas={}, err={}, attempt={}", info.shard_id,
                     info.blob_id,
                     info.pbas.to_string(), result.error(), i);
            } else {
                blob = std::move(result.value());
                break;
            }
        }
        if (blob.size() == 0) {
            LOGE("Failed to retrieve blob for shard={} blob={} pbas={}", info.shard_id, info.blob_id,
                 info.pbas.to_string());
            //TODO add metrics
            return false;
        }

        std::vector< uint8_t > data(blob.cbytes(), blob.cbytes() + blob.size());
        blob_entries.push_back(CreateResyncBlobDataDirect(builder_, info.blob_id, (uint8_t)state, &data));
        total_bytes += blob.size();
    }
    //should include the deleted blobs
    cur_batch_blob_count_ = idx - cur_start_blob_idx_;
    if (idx == cur_blob_list_.size()) { end_of_shard = true; }
    builder_.FinishSizePrefixed(
        CreateResyncBlobDataBatchDirect(builder_, &blob_entries, end_of_shard));

    LOGD("create blobs snapshot data batch: shard_id={}, batch_num={}, total_bytes={}, blob_num={}, end_of_shard={}",
         cur_obj_id_.shard_seq_num, cur_obj_id_.batch_id, total_bytes, blob_entries.size(), end_of_shard);

    pack_resync_message(data_blob, SyncMessageType::SHARD_BATCH);
    return true;
}

void HSHomeObject::PGBlobIterator::pack_resync_message(sisl::io_blob_safe& dest_blob, SyncMessageType type) {
    SyncMessageHeader header;
    header.msg_type = type;
    header.payload_size = builder_.GetSize();
    header.payload_crc = crc32_ieee(init_crc32, builder_.GetBufferPointer(), builder_.GetSize());
    header.seal();
    LOGD("Creating resync message in pg[{}] with header={} ", pg_id_, header.to_string());

    dest_blob = sisl::io_blob_safe{static_cast< unsigned int >(builder_.GetSize() + sizeof(SyncMessageHeader))};
    std::memcpy(dest_blob.bytes(), &header, sizeof(SyncMessageHeader));
    std::memcpy(dest_blob.bytes() + sizeof(SyncMessageHeader), builder_.GetBufferPointer(), builder_.GetSize());

    //reset builder for next message
    builder_.Clear();
}

} // namespace homeobject