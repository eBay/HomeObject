#include <utility>

#include "hs_homeobject.hpp"

#include <homestore/blkdata_service.hpp>

namespace homeobject {
HSHomeObject::SnapshotReceiveHandler::SnapshotReceiveHandler(HSHomeObject& home_obj,
                                                             shared< homestore::ReplDev > repl_dev) :
        home_obj_(home_obj), repl_dev_(std::move(repl_dev)) {}

int HSHomeObject::SnapshotReceiveHandler::process_pg_snapshot_data(ResyncPGMetaData const& pg_meta) {
    LOGI("process_pg_snapshot_data pg_id:{}", pg_meta.pg_id());

    // Init shard list
    ctx_->shard_list.clear();
    const auto ids = pg_meta.shard_ids();
    for (unsigned int i = 0; i < ids->size(); i++) {
        ctx_->shard_list.push_back(ids->Get(i));
    }

    // Create local PG
    PGInfo pg_info(pg_meta.pg_id());
    pg_info.size = pg_meta.pg_size();
    pg_info.chunk_size =  pg_meta.chunk_size();
    std::copy_n(pg_meta.replica_set_uuid()->data(), 16, pg_info.replica_set_uuid.begin());
    for (unsigned int i = 0; i < pg_meta.members()->size(); i++) {
        const auto member = pg_meta.members()->Get(i);
        uuids::uuid id{};
        std::copy_n(member->uuid()->data(), 16, id.begin());
        PGMember pg_member(id);
        pg_member.name = GetString(member->name());
        pg_member.priority = member->priority();
        pg_info.members.insert(pg_member);
    }
    auto ret = home_obj_.local_create_pg(repl_dev_, pg_info);
    if (ret.hasError()) {
        LOGE("Failed to create PG {}, err {}", pg_meta.pg_id(), ret.error());
        return CREATE_PG_ERR;
    }
    auto hs_pg = ret.value();

    // Init a base set of pg blob & shard sequence num. Will catch up later on shard/blob creation if not up-to-date
    hs_pg->shard_sequence_num_ = pg_meta.shard_seq_num();
    hs_pg->durable_entities_update(
        [&pg_meta](auto& de) { de.blob_sequence_num.store(pg_meta.blob_seq_num(), std::memory_order_relaxed); });
    return 0;
}

int HSHomeObject::SnapshotReceiveHandler::process_shard_snapshot_data(ResyncShardMetaData const& shard_meta) {
    LOGI("process_shard_snapshot_data shard_id:{}", shard_meta.shard_id());

    // Persist shard meta on chunk data
    sisl::io_blob_safe aligned_buf(sisl::round_up(sizeof(shard_info_superblk), io_align), io_align);
    shard_info_superblk* shard_sb = r_cast< shard_info_superblk* >(aligned_buf.bytes());
    shard_sb->info.id = shard_meta.shard_id();
    shard_sb->info.placement_group = shard_meta.pg_id();
    shard_sb->info.state = static_cast< ShardInfo::State >(shard_meta.state());
    shard_sb->info.lsn = shard_meta.created_lsn();
    shard_sb->info.created_time = shard_meta.created_time();
    shard_sb->info.last_modified_time = shard_meta.last_modified_time();
    shard_sb->info.available_capacity_bytes = shard_meta.total_capacity_bytes();
    shard_sb->info.total_capacity_bytes = shard_meta.total_capacity_bytes();
    shard_sb->info.deleted_capacity_bytes = 0;
    shard_sb->v_chunk_id = shard_meta.vchunk_id();

    homestore::blk_alloc_hints hints;
    hints.application_hint = static_cast< uint64_t >(ctx_->pg_id) << 16 | shard_sb->v_chunk_id;

    homestore::MultiBlkId blk_id;
    auto status = homestore::data_service().alloc_blks(
        sisl::round_up(aligned_buf.size(), homestore::data_service().get_blk_size()), hints, blk_id);
    if (status != homestore::BlkAllocStatus::SUCCESS) {
        LOGE("Failed to allocate blocks for shard {}", shard_meta.shard_id());
        return ALLOC_BLK_ERR;
    }
    shard_sb->p_chunk_id = blk_id.to_single_blkid().chunk_num();

    const auto ret = homestore::data_service()
                         .async_write(r_cast< char const* >(aligned_buf.cbytes()), aligned_buf.size(), blk_id)
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
    if (ret.hasError()) {
        LOGE("Failed to write shard info of shard_id {} to blk_id:{}", shard_meta.shard_id(), blk_id.to_string());
        return WRITE_DATA_ERR;
    }

    // Now let's create local shard
    home_obj_.local_create_shard(shard_sb->info, shard_sb->v_chunk_id, shard_sb->p_chunk_id, blk_id.blk_count());
    ctx_->shard_cursor = shard_meta.shard_id();
    ctx_->cur_batch_num = 0;
    return 0;
}

int HSHomeObject::SnapshotReceiveHandler::process_blobs_snapshot_data(ResyncBlobDataBatch const& data_blobs,
                                                                      const snp_batch_id_t batch_num,
                                                                      bool is_last_batch) {
    ctx_->cur_batch_num = batch_num;

    // Find physical chunk id for current shard
    auto v_chunk_id = home_obj_.get_shard_v_chunk_id(ctx_->shard_cursor);
    auto p_chunk_id = home_obj_.get_shard_p_chunk_id(ctx_->shard_cursor);
    RELEASE_ASSERT(p_chunk_id.has_value(), "Failed to load chunk of current shard_cursor:{}", ctx_->shard_cursor);
    homestore::blk_alloc_hints hints;
    hints.chunk_id_hint = *p_chunk_id;

    for (unsigned int i = 0; i < data_blobs.blob_list()->size(); i++) {
        const auto blob = data_blobs.blob_list()->Get(i);

        // Skip deleted blobs
        if (blob->state() == static_cast< uint8_t >(ResyncBlobState::DELETED)) {
            LOGD("Skip deleted blob_id:{}", blob->blob_id());
            continue;
        }

        // Check duplication to avoid reprocessing. This may happen on resent blob batches.
        if (!ctx_->index_table) {
            std::shared_lock lock_guard(home_obj_._pg_lock);
            auto iter = home_obj_._pg_map.find(ctx_->pg_id);
            RELEASE_ASSERT(iter != home_obj_._pg_map.end(), "PG not found");
            ctx_->index_table = dynamic_cast< HS_PG* >(iter->second.get())->index_table_;
        }
        RELEASE_ASSERT(ctx_->index_table != nullptr, "Index table instance null");
        if (home_obj_.get_blob_from_index_table(ctx_->index_table, ctx_->shard_cursor, blob->blob_id())) {
            LOGD("Skip already persisted blob_id:{}", blob->blob_id());
            continue;
        }

        auto blob_data = blob->data()->Data();

        // Check integrity of normal blobs
        if (blob->state() != static_cast< uint8_t >(ResyncBlobState::CORRUPTED)) {
            auto header = r_cast< BlobHeader const* >(blob_data);
            if (!header->valid()) {
                LOGE("Invalid header found for blob_id {}: [header={}]", blob->blob_id(), header->to_string());
                return INVALID_BLOB_HEADER;
            }
            std::string user_key = header->user_key_size
                ? std::string(r_cast< const char* >(blob_data + sizeof(BlobHeader)), header->user_key_size)
                : std::string{};

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
        auto data_size = blob->data()->size();
        sisl::io_blob_safe aligned_buf(sisl::round_up(data_size, io_align), io_align);
        std::memcpy(aligned_buf.bytes(), blob_data, data_size);

        homestore::MultiBlkId blk_id;
        auto status = homestore::data_service().alloc_blks(
            sisl::round_up(aligned_buf.size(), homestore::data_service().get_blk_size()), hints, blk_id);
        if (status != homestore::BlkAllocStatus::SUCCESS) {
            LOGE("Failed to allocate blocks for shard {} blob {}", ctx_->shard_cursor, blob->blob_id());
            return ALLOC_BLK_ERR;
        }

        auto free_allocated_blks = [blk_id]() {
            homestore::data_service().async_free_blk(blk_id).thenValue([blk_id](auto&& err) {
                LOGD("Freed blk_id:{} due to failure in adding blob info, err {}", blk_id.to_string(),
                     err ? err.message() : "nil");
            });
        };

        const auto ret = homestore::data_service()
                             .async_write(r_cast< char const* >(aligned_buf.cbytes()), aligned_buf.size(), blk_id)
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
        if (ret.hasError()) {
            LOGE("Failed to write shard info of blob_id {} to blk_id:{}", blob->blob_id(), blk_id.to_string());
            free_allocated_blks();
            return WRITE_DATA_ERR;
        }

        // Add local blob info to index & PG
        bool success =
            home_obj_.local_add_blob_info(ctx_->pg_id, BlobInfo{ctx_->shard_cursor, blob->blob_id(), blk_id});
        if (!success) {
            LOGE("Failed to add blob info for blob_id:{}", blob->blob_id());
            free_allocated_blks();
            return ADD_BLOB_INDEX_ERR;
        }
    }

    if (is_last_batch) {
        // Release chunk for sealed shard
        ShardInfo::State state;
        {
            std::scoped_lock lock_guard(home_obj_._shard_lock);
            auto iter = home_obj_._shard_map.find(ctx_->shard_cursor);
            state = (*iter->second)->info.state;
        }
        if (state == ShardInfo::State::SEALED) {
            home_obj_.chunk_selector()->release_chunk(ctx_->pg_id, v_chunk_id.value());
        }
    }

    return 0;
}

int64_t HSHomeObject::SnapshotReceiveHandler::get_context_lsn() const { return ctx_ ? ctx_->snp_lsn : -1; }

void HSHomeObject::SnapshotReceiveHandler::reset_context(int64_t lsn, pg_id_t pg_id) {
    ctx_ = std::make_unique< SnapshotContext >(lsn, pg_id);
}

shard_id_t HSHomeObject::SnapshotReceiveHandler::get_shard_cursor() const { return ctx_->shard_cursor; }

shard_id_t HSHomeObject::SnapshotReceiveHandler::get_next_shard() const {
    if (ctx_->shard_list.empty()) { return shard_list_end_marker; }

    if (ctx_->shard_cursor == 0) { return ctx_->shard_list[0]; }

    for (size_t i = 0; i < ctx_->shard_list.size(); ++i) {
        if (ctx_->shard_list[i] == ctx_->shard_cursor) {
            return (i + 1 < ctx_->shard_list.size()) ? ctx_->shard_list[i + 1] : shard_list_end_marker;
        }
    }

    return invalid_shard_id;
}

} // namespace homeobject
