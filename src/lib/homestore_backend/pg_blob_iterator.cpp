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
    home_obj_(home_obj),
    group_id_(group_id),
    snp_start_lsn(upto_lsn) {
    auto pg = get_pg_metadata();
    pg_id_ = pg->pg_info_.id;
    repl_dev_ = static_cast< HS_PG* >(pg)->repl_dev_;
    max_batch_size = HS_BACKEND_DYNAMIC_CONFIG(max_snapshot_batch_size_mb) * Mi;
    if (max_batch_size == 0) { max_batch_size = DEFAULT_MAX_BATCH_SIZE_MB * Mi; }

    if (upto_lsn != 0) {
        // Iterate all shards and its blob which have lsn <= upto_lsn
        for (auto& shard : pg->shards_) { if (shard->info.lsn <= upto_lsn) { shard_list.emplace_back(shard->info); } }
        //sort shard list by <vchunkid, lsn>
        std::sort(shard_list.begin(), shard_list.end(), [](const ShardInfo& a, const ShardInfo& b) {
            //TODO compare vchunk id
            return a.lsn < b.lsn;
        });
    }
}

//result represents if the objId is valid and the cursors are updated
bool HSHomeObject::PGBlobIterator::updateCursor(objId id) {
    bool next_shard = false;
    auto target_shard_id = make_new_shard_id(pg_id_, id.shard_seq_num);
    auto cur_shard_id = shard_list[cur_shard_idx].id;
    //check shard id
    if (cur_shard_idx < shard_list.size() - 1 && target_shard_id == shard_list[cur_shard_idx + 1].id) {
        next_shard = true;
    }
    if (target_shard_id != cur_shard_id && !next_shard) {
        shard_id_t next = cur_shard_idx == shard_list.size() - 1 ? 0 : shard_list[cur_shard_idx + 1].id;
        LOGW(
            "invalid asked shardId in snapshot read, required shard_seq_num={}, current shard_seq_num={}, current shard_idx={}, shard_list size={}, next shard in shard_list={}",
            id.shard_seq_num, cur_shard_seq_num, cur_shard_idx, shard_list.size(), next);
        return false;
    }

    // check batch id
    if (next_shard && id.shard_seq_num == 0) {
        cur_shard_idx++;
        cur_batch_num = 0;
        last_end_blob_idx = -1;
        cur_shard_seq_num = shard_list[cur_shard_idx].id & 0xFFFFFFFFFFFF;
        return true;
    }
    //resend batch
    if (!next_shard && id.batch_id == cur_batch_num) { return true; }
    //next batch
    if (!next_shard && id.batch_id == cur_batch_num + 1) {
        cur_batch_num = id.batch_id;
        last_end_blob_idx = last_end_blob_idx + cur_batch_blob_count;
        cur_batch_blob_count = 0;
        return true;
    }

    return false;
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

    flatbuffers::FlatBufferBuilder builder;
    std::vector< std::uint8_t > uuid(pg_info.replica_set_uuid.begin(), pg_info.replica_set_uuid.end());

    std::vector< ::flatbuffers::Offset< homeobject::Member > > members(pg_info.members.size());
    for (auto& member : pg_info.members) {
        auto id = std::vector< std::uint8_t >(member.id.begin(), member.id.end());
        members.push_back(CreateMemberDirect(builder, &id, member.name.c_str(), member.priority));
    }
    std::vector< uint64_t > shard_ids;
    for (auto& shard : pg->shards_) { shard_ids.push_back(shard->info.id); }

    auto pg_entry = CreateResyncPGMetaDataDirect(builder, pg_info.id, &uuid, pg->durable_entities().blob_sequence_num,
                                                 pg->shard_sequence_num_, &members, &shard_ids);
    builder.FinishSizePrefixed(pg_entry);
    auto payload = sisl::io_blob_safe{builder.GetSize()};
    std::memcpy(payload.bytes(), builder.GetBufferPointer(), builder.GetSize());

    pack_resync_message(meta_blob, SyncMessageType::PG_META, payload);
    return true;
}

bool HSHomeObject::PGBlobIterator::generate_shard_blob_list() {
    //TODO do we need pagination query?
    auto r = home_obj_.query_blobs_in_shard(pg_id_, cur_shard_seq_num, 0, UINT64_MAX);
    if (!r) { return false; }
    cur_blob_list = r.value();
    return true;
}

bool HSHomeObject::PGBlobIterator::create_shard_snapshot_data(sisl::io_blob_safe& meta_blob) {
    auto shard = shard_list[cur_shard_idx];
    flatbuffers::FlatBufferBuilder builder;

    //TODO fill vchunk
    auto shard_entry = CreateResyncShardMetaData(builder, shard.id, pg_id_, uint8_t(shard.state), shard.lsn,
                                                 shard.created_time, shard.last_modified_time,
                                                 shard.total_capacity_bytes, 0);

    builder.FinishSizePrefixed(shard_entry);
    auto payload = sisl::io_blob_safe{builder.GetSize()};
    std::memcpy(payload.bytes(), builder.GetBufferPointer(), builder.GetSize());

    pack_resync_message(meta_blob, SyncMessageType::SHARD_META, payload);
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
                                LOGE("Invalid header found, shard_id={}, blob_id={}, [header={}]", shard_id, blob_id,
                                     header->to_string());
                                return folly::makeUnexpected(BlobError(BlobErrorCode::READ_FAILED));
                            }

                            if (header->shard_id != shard_id) {
                                LOGE("Invalid shard_id in header, shard_id={}, blob_id={}, [header={}]", shard_id,
                                     blob_id, header->to_string());
                                return folly::makeUnexpected(BlobError(BlobErrorCode::READ_FAILED));
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
    flatbuffers::FlatBufferBuilder builder;

    bool end_of_shard = false;
    uint64_t total_bytes = 0;
    auto idx = (uint64_t)(last_end_blob_idx + 1);

    while (total_bytes < max_batch_size && idx < cur_blob_list.size()) {
        auto info = cur_blob_list[idx];
        ResyncBlobState state = ResyncBlobState::NORMAL;
        //handle deleted object
        if (info.pbas == tombstone_pbas) {
            state = ResyncBlobState::DELETED;
            LOGD("Blob is deleted: shard_id={}, blob_id={}, blkid={}", info.shard_id, info.blob_id,
                 info.pbas.to_string());
            blob_entries.push_back(CreateResyncBlobDataDirect(builder, info.blob_id, (uint8_t)state, nullptr));
            continue;
        }
        auto result = load_blob_data(info, state).get();
        if (result.hasError() && result.error().code == BlobErrorCode::READ_FAILED) {
            LOGW("Failed to retrieve blob for shard={} blob={} pbas={}, err={}", info.shard_id, info.blob_id,
                 info.pbas.to_string(), result.error());
            //TODO add metrics or events to track this
            return false;
        }

        auto& blob = result.value();
        std::vector< uint8_t > data(blob.cbytes(), blob.cbytes() + blob.size());
        blob_entries.push_back(CreateResyncBlobDataDirect(builder, info.blob_id, (uint8_t)state, &data));
        total_bytes += blob.size();
    }
    idx++;

    if (idx == cur_blob_list.size()) { end_of_shard = true; }
    cur_batch_blob_count = blob_entries.size();
    builder.FinishSizePrefixed(
        CreateResyncBlobDataBatchDirect(builder, &blob_entries, end_of_shard));
    auto payload = sisl::io_blob_safe{builder.GetSize()};
    std::memcpy(payload.bytes(), builder.GetBufferPointer(), builder.GetSize());
    LOGD("create blobs snapshot data batch: shard_id={}, batch_num={}, total_bytes={}, blob_num={}, end_of_shard={}",
         cur_shard_seq_num, cur_batch_num, total_bytes, blob_entries.size(), end_of_shard);

    pack_resync_message(data_blob, SyncMessageType::SHARD_BATCH, payload);
    return true;
}

void HSHomeObject::PGBlobIterator::pack_resync_message(sisl::io_blob_safe& dest_blob, SyncMessageType type,
                                                       sisl::io_blob_safe& payload) {
    SyncMessageHeader header;
    header.msg_type = type;
    header.payload_size = payload.size();
    header.payload_crc = crc32_ieee(init_crc32, payload.cbytes(), payload.size());
    header.seal();
    LOGD("Creating resync message in pg[{}] with header={} ", pg_id_, header.to_string());

    dest_blob = sisl::io_blob_safe{static_cast< unsigned int >(payload.size() + sizeof(SyncMessageHeader))};
    std::memcpy(dest_blob.bytes(), &header, sizeof(SyncMessageHeader));
    std::memcpy(dest_blob.bytes() + sizeof(SyncMessageHeader), payload.cbytes(), payload.size());
}

} // namespace homeobject