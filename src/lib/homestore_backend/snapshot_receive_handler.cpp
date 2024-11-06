#include <utility>

#include "hs_homeobject.hpp"

#include <homestore/blkdata_service.hpp>

namespace homeobject {
HSHomeObject::SnapshotReceiveHandler::SnapshotReceiveHandler(HSHomeObject& home_obj, pg_id_t pg_id_,
                                                             homestore::group_id_t group_id, int64_t lsn,
                                                             shared< homestore::ReplDev > repl_dev) :
        snp_lsn_(lsn), home_obj_(home_obj), group_id_(group_id), pg_id_(pg_id_), repl_dev_(std::move(repl_dev)) {}

void HSHomeObject::SnapshotReceiveHandler::process_pg_snapshot_data(ResyncPGMetaData const& pg_meta) {
    LOGI("process_pg_snapshot_data pg_id:{}", pg_meta.pg_id());

    // Init shard list
    shard_list_.clear();
    const auto ids = pg_meta.shard_ids();
    for (unsigned int i = 0; i < ids->size(); i++) {
        shard_list_.push_back(ids->Get(i));
    }

    // Create local PG
    PGInfo pg_info(pg_meta.pg_id());
    std::copy_n(pg_meta.replica_set_uuid()->data(), 16, pg_info.replica_set_uuid.begin());
    pg_info.size = pg_meta.members()->size();
    for (unsigned int i = 0; i < pg_meta.members()->size(); i++) {
        const auto member = pg_meta.members()->Get(i);
        uuids::uuid id{};
        std::copy_n(member->uuid()->data(), 16, id.begin());
        PGMember pg_member(id);
        pg_member.name = GetString(member->name());
        pg_member.priority = member->priority();
        pg_info.members.insert(pg_member);
    }
    const auto hs_pg = home_obj_.local_create_pg(repl_dev_, pg_info);

    // Init a base set of pg blob & shard sequence num. Will catch up later on shard/blob creation if not up-to-date
    hs_pg->shard_sequence_num_ = pg_meta.shard_seq_num();
    hs_pg->durable_entities_update(
        [&pg_meta](auto& de) { de.blob_sequence_num.store(pg_meta.blob_seq_num(), std::memory_order_relaxed); });
}

int HSHomeObject::SnapshotReceiveHandler::process_shard_snapshot_data(ResyncShardMetaData const& shard_meta) {
    LOGI("process_shard_snapshot_data shard_id:{}", shard_meta.shard_id());

    // Persist shard meta on chunk data
    homestore::chunk_num_t chunk_id = shard_meta.vchunk_id(); // FIXME: vchunk id to chunk id

    shard_info_superblk shard_sb;
    shard_sb.info.id = shard_meta.shard_id();
    shard_sb.info.placement_group = shard_meta.pg_id();
    shard_sb.info.state = static_cast< ShardInfo::State >(shard_meta.state());
    shard_sb.info.lsn = shard_meta.created_lsn();
    shard_sb.info.created_time = shard_meta.created_time();
    shard_sb.info.last_modified_time = shard_meta.last_modified_time();
    shard_sb.info.available_capacity_bytes = shard_meta.total_capacity_bytes();
    shard_sb.info.total_capacity_bytes = shard_meta.total_capacity_bytes();
    shard_sb.info.deleted_capacity_bytes = 0;
    shard_sb.chunk_id = chunk_id;

    homestore::MultiBlkId blk_id;
    const auto hints = home_obj_.chunk_selector()->chunk_to_hints(chunk_id);
    auto status = homestore::data_service().alloc_blks(
        sisl::round_up(sizeof(shard_sb), homestore::data_service().get_blk_size()), hints, blk_id);
    if (status != homestore::BlkAllocStatus::SUCCESS) {
        LOGE("Failed to allocate blocks for shard {}", shard_meta.shard_id());
        return ALLOC_BLK_ERR;
    }

    const auto ret = homestore::data_service()
                         .async_write(r_cast< char const* >(&shard_sb), sizeof(shard_sb), blk_id)
                         .thenValue([&blk_id](auto&& err) -> BlobManager::AsyncResult< blob_id_t > {
                             // TODO: do we need to update repl_dev metrics?
                             if (err) {
                                 LOGE("Failed to write shard info to blk_id: {}", blk_id.to_string());
                                 return folly::makeUnexpected(BlobError(BlobErrorCode::REPLICATION_ERROR));
                             }
                             LOGD("Shard info written to blk_id:{}", blk_id.to_string());
                             return 0;
                         })
                         .get();
    if (ret) {
        LOGE("Failed to write shard info of shard_id {} to blk_id:{}", shard_meta.shard_id(), blk_id.to_string());
        return WRITE_DATA_ERR;
    }

    // Now let's create local shard
    home_obj_.local_create_shard(shard_sb.info, chunk_id, blk_id.blk_count());
    shard_cursor_ = shard_meta.shard_id();
    blob_cursor_ = 0;
    cur_batch_num_ = 0;
    return 0;
}

int HSHomeObject::SnapshotReceiveHandler::process_blobs_snapshot_data(ResyncBlobDataBatch const& data_blobs,
                                                                      const snp_batch_id_t batch_num) {
    cur_batch_num_ = batch_num;
    for (unsigned int i = 0; i < data_blobs.blob_list()->size(); i++) {
        const auto blob = data_blobs.blob_list()->Get(i);

        // Skip deleted blobs
        if (blob->state() == static_cast< uint8_t >(ResyncBlobState::DELETED)) {
            LOGD("Skip deleted blob_id:{}", blob->blob_id());
            continue;
        }

        // Check duplication to avoid reprocessing
        shared< BlobIndexTable > index_table;
        {
            std::shared_lock lock_guard(home_obj_._pg_lock);
            auto iter = home_obj_._pg_map.find(pg_id_);
            RELEASE_ASSERT(iter != home_obj_._pg_map.end(), "PG not found");
            index_table = dynamic_cast< HS_PG* >(iter->second.get())->index_table_;
        }
        RELEASE_ASSERT(index_table != nullptr, "Index table instance null");
        if (home_obj_.get_blob_from_index_table(index_table, shard_cursor_, blob->blob_id())) {
            LOGD("Skip already persisted blob_id:{}", blob->blob_id());
            continue;
        }

        auto blob_data = blob->data()->Data();
        auto header = r_cast< BlobHeader const* >(blob_data);
        if (header->valid()) {
            LOGE("Invalid header found for blob_id {}: [header={}]", blob->blob_id(), header->to_string());
            return INVALID_BLOB_HEADER;
        }
        std::string user_key = header->user_key_size
            ? std::string(r_cast< const char* >(blob_data + sizeof(BlobHeader)), header->user_key_size)
            : std::string{};

        // Check integrity of normal blobs
        if (blob->state() != static_cast< uint8_t >(ResyncBlobState::CORRUPTED)) {
            uint8_t computed_hash[BlobHeader::blob_max_hash_len]{};
            home_obj_.compute_blob_payload_hash(header->hash_algorithm, blob_data + header->data_offset,
                                                header->blob_size, uintptr_cast(user_key.data()), header->user_key_size,
                                                computed_hash, BlobHeader::blob_max_hash_len);
            if (std::memcmp(computed_hash, header->hash, BlobHeader::blob_max_hash_len) != 0) {
                LOGE("Hash mismatch for blob_id {}: header [{}] [computed={:np}]", blob->blob_id(), header->to_string(),
                     spdlog::to_hex(computed_hash, computed_hash + BlobHeader::blob_max_hash_len));
                return BLOB_DATA_CORRUPTED;
            }
        }

        // Alloc & persist blob data
        auto chunk_id = home_obj_.get_shard_chunk(shard_cursor_);
        RELEASE_ASSERT(chunk_id.has_value(), "Failed to load chunk of current shard_cursor:{}", shard_cursor_);
        homestore::blk_alloc_hints hints;
        hints.chunk_id_hint = *chunk_id;

        auto data_size = blob->data()->size();
        homestore::MultiBlkId blk_id;
        auto status = homestore::data_service().alloc_blks(
            sisl::round_up(data_size, homestore::data_service().get_blk_size()), hints, blk_id);
        if (status != homestore::BlkAllocStatus::SUCCESS) {
            LOGE("Failed to allocate blocks for shard {} blob {}", shard_cursor_, blob->blob_id());
            return ALLOC_BLK_ERR;
        }

        auto free_allocated_blks = [blk_id]() {
            homestore::data_service().async_free_blk(blk_id).thenValue([blk_id](auto&& err) {
                LOGD("Freed blk_id:{} due to failure in adding blob info, err {}", blk_id.to_string(),
                     err ? err.message() : "nil");
            });
        };

        const auto ret = homestore::data_service()
                             .async_write(r_cast< char const* >(blob_data), data_size, blk_id)
                             .thenValue([&blk_id](auto&& err) -> BlobManager::AsyncResult< blob_id_t > {
                                 // TODO: do we need to update repl_dev metrics?
                                 if (err) {
                                     LOGE("Failed to write shard info to blk_id: {}", blk_id.to_string());
                                     return folly::makeUnexpected(BlobError(BlobErrorCode::REPLICATION_ERROR));
                                 }
                                 LOGD("Shard info written to blk_id:{}", blk_id.to_string());
                                 return 0;
                             })
                             .get();
        if (ret) {
            LOGE("Failed to write shard info of blob_id {} to blk_id:{}", blob->blob_id(), blk_id.to_string());
            free_allocated_blks();
            return WRITE_DATA_ERR;
        }

        // Add local blob info to index & PG
        bool success = home_obj_.local_add_blob_info(pg_id_, BlobInfo{shard_cursor_, blob->blob_id(), {0, 0, 0}});
        if (!success) {
            LOGE("Failed to add blob info for blob_id:{}", blob->blob_id());
            free_allocated_blks();
            return ADD_BLOB_INDEX_ERR;
        }

        blob_cursor_ = blob->blob_id();
    }

    return 0;
}

shard_id_t HSHomeObject::SnapshotReceiveHandler::get_next_shard() const {
    if (shard_list_.empty()) { return shard_list_end_marker; }

    if (shard_cursor_ == 0) { return shard_list_[0]; }

    for (size_t i = 0; i < shard_list_.size(); ++i) {
        if (shard_list_[i] == shard_cursor_) {
            return (i + 1 < shard_list_.size()) ? shard_list_[i + 1] : shard_list_end_marker;
        }
    }

    return invalid_shard_id;
}

} // namespace homeobject
