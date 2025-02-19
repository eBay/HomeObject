#include <utility>

#include "hs_homeobject.hpp"
#include "replication_state_machine.hpp"

#include <boost/uuid/random_generator.hpp>
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
    pg_info.chunk_size = pg_meta.chunk_size();
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

#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("snapshot_receiver_pg_error")) {
        LOGW("Simulating PG snapshot error");
        return CREATE_PG_ERR;
    }
#endif
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

    // No need to persist snp info superblock since it's almost meaningless to resume from this point.
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

    auto free_allocated_blks = [blk_id]() {
        homestore::data_service().async_free_blk(blk_id).thenValue([blk_id](auto&& err) {
            LOGD("Freed blk_id:{} due to failure in persisting shard info, err {}", blk_id.to_string(),
                 err ? err.message() : "nil");
        });
    };

#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("snapshot_receiver_shard_write_data_error")) {
        LOGW("Simulating shard snapshot write data error");
        free_allocated_blks();
        return WRITE_DATA_ERR;
    }
#endif
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
        free_allocated_blks();
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

#ifdef _PRERELEASE
        auto delay = iomgr_flip::instance()->get_test_flip< long >("simulate_write_snapshot_save_blob_delay",
                                                                   static_cast< long >(blob->blob_id()));
        LOGD("simulate_write_snapshot_save_blob_delay flip, triggered: {}, blob: {}", delay.has_value(), blob->blob_id());
        if (delay) {
            LOGI("Simulating pg snapshot receive data with delay, delay:{}, blob_id:{}", delay.get(), blob->blob_id());
            std::this_thread::sleep_for(std::chrono::milliseconds(delay.get()));
        }
#endif

        // Check duplication to avoid reprocessing. This may happen on resent blob batches.
        if (!ctx_->index_table) {
            auto hs_pg = home_obj_.get_hs_pg(ctx_->pg_id);
            RELEASE_ASSERT(hs_pg != nullptr, "PG not found for pg_id:{}", ctx_->pg_id);
            ctx_->index_table = hs_pg->index_table_;
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

#ifdef _PRERELEASE
        if (iomgr_flip::instance()->test_flip("snapshot_receiver_blob_write_data_error")) {
            LOGW("Simulating blob snapshot write data error");
            free_allocated_blks();
            return WRITE_DATA_ERR;
        }
#endif
        const auto ret = homestore::data_service()
                             .async_write(r_cast< char const* >(aligned_buf.cbytes()), aligned_buf.size(), blk_id)
                             .thenValue([&blk_id](auto&& err) -> BlobManager::AsyncResult< blob_id_t > {
                                 // TODO: do we need to update repl_dev metrics?
                                 if (err) {
                                     LOGE("Failed to write blob info to blk_id: {}", blk_id.to_string());
                                     return folly::makeUnexpected(BlobError(BlobErrorCode::REPLICATION_ERROR));
                                 }
                                 LOGD("Blob info written to blk_id:{}", blk_id.to_string());
                                 return 0;
                             })
                             .get();
        if (ret.hasError()) {
            LOGE("Failed to write blob info of blob_id {} to blk_id:{}", blob->blob_id(), blk_id.to_string());
            free_allocated_blks();
            return WRITE_DATA_ERR;
        }
        if (homestore::data_service().commit_blk(blk_id) != homestore::BlkAllocStatus::SUCCESS) {
            LOGE("Failed to commit blk_id:{} for blob_id: {}", blk_id.to_string(), blob->blob_id());
            free_allocated_blks();
            return COMMIT_BLK_ERR;
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

        // We only update the snp info superblk on completion of each shard, since resumption is also shard-level
        update_snp_info_sb(ctx_->shard_cursor == ctx_->shard_list.front());
    }

    return 0;
}

int64_t HSHomeObject::SnapshotReceiveHandler::get_context_lsn() const { return ctx_ ? ctx_->snp_lsn : -1; }
pg_id_t HSHomeObject::SnapshotReceiveHandler::get_context_pg_id() const { return ctx_ ? ctx_->pg_id : 0; }

bool HSHomeObject::SnapshotReceiveHandler::load_prev_context() {
    HS_PG* hs_pg = nullptr;
    {
        std::shared_lock lck(home_obj_._pg_lock);
        auto iter = find_if(home_obj_._pg_map.begin(), home_obj_._pg_map.end(), [this](auto& pg) {
            auto hs_pg = dynamic_cast< HS_PG* >(pg.second.get());
            return hs_pg->repl_dev_ == repl_dev_;
        });
        hs_pg = iter == home_obj_._pg_map.end() ? nullptr : dynamic_cast< HS_PG* >(iter->second.get());
    }
    if (hs_pg == nullptr || hs_pg->snp_rcvr_info_sb_.is_empty() || hs_pg->snp_rcvr_shard_list_sb_.is_empty() ||
        hs_pg->snp_rcvr_info_sb_->snp_lsn != hs_pg->snp_rcvr_shard_list_sb_->snp_lsn) {
        return false;
    }

    RELEASE_ASSERT(hs_pg->snp_rcvr_info_sb_->pg_id == hs_pg->pg_sb_->id &&
                       hs_pg->snp_rcvr_shard_list_sb_->pg_id == hs_pg->pg_sb_->id,
                   "PG id in snp_info sb not matching with PG sb");

    ctx_ = std::make_unique< SnapshotContext >(hs_pg->snp_rcvr_info_sb_->snp_lsn, hs_pg->snp_rcvr_info_sb_->pg_id);
    ctx_->shard_cursor = hs_pg->snp_rcvr_info_sb_->shard_cursor;
    ctx_->cur_batch_num = 0; // Always resume from the beginning of the shard
    ctx_->index_table = hs_pg->index_table_;
    ctx_->shard_list = hs_pg->snp_rcvr_shard_list_sb_->get_shard_list();

    LOGINFO("Resuming snapshot receiver context from lsn:{} pg_id:{} shard_cursor:{}", ctx_->snp_lsn,
            hs_pg->snp_rcvr_info_sb_->pg_id, ctx_->shard_cursor);
    return true;
}

void HSHomeObject::SnapshotReceiveHandler::reset_context(int64_t lsn, pg_id_t pg_id) {
    if (ctx_ != nullptr) { destroy_context(); }
    ctx_ = std::make_unique< SnapshotContext >(lsn, pg_id);
}

void HSHomeObject::SnapshotReceiveHandler::destroy_context() {
    auto hs_pg = home_obj_.get_hs_pg(ctx_->pg_id);
    if (hs_pg == nullptr) { return; }
    hs_pg->snp_rcvr_info_sb_.destroy();
    hs_pg->snp_rcvr_shard_list_sb_.destroy();
    ctx_.reset();
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

void HSHomeObject::SnapshotReceiveHandler::update_snp_info_sb(bool init) {
    auto hs_pg = home_obj_.get_hs_pg(ctx_->pg_id);
    RELEASE_ASSERT(hs_pg != nullptr, "PG not found");

    auto* sb = hs_pg->snp_rcvr_info_sb_.get();
    if (init) {
        if (!hs_pg->snp_rcvr_info_sb_.is_empty()) { hs_pg->snp_rcvr_info_sb_.destroy(); }
        if (!hs_pg->snp_rcvr_shard_list_sb_.is_empty()) { hs_pg->snp_rcvr_shard_list_sb_.destroy(); }
        sb = hs_pg->snp_rcvr_info_sb_.create(sizeof(snapshot_rcvr_info_superblk));

        auto lst_sb = hs_pg->snp_rcvr_shard_list_sb_.create(sizeof(snapshot_rcvr_shard_list_superblk) +
                                                            (ctx_->shard_list.size() - 1) * sizeof(shard_id_t));
        lst_sb->pg_id = ctx_->pg_id;
        lst_sb->snp_lsn = ctx_->snp_lsn;
        lst_sb->shard_cnt = ctx_->shard_list.size();
        std::copy(ctx_->shard_list.begin(), ctx_->shard_list.end(), lst_sb->shard_list);
        hs_pg->snp_rcvr_shard_list_sb_.write();
    }
    RELEASE_ASSERT(sb != nullptr, "Snapshot info superblk not found");
    sb->snp_lsn = ctx_->snp_lsn;
    sb->pg_id = ctx_->pg_id;
    sb->shard_cursor = get_next_shard();
    hs_pg->snp_rcvr_info_sb_.write();

    // Ensure all the superblk & corresponding index/data update have been written to disk
    auto fut = homestore::hs()->cp_mgr().trigger_cp_flush(true /* force */);
    LOGINFO("Update snp_info sb, CP Flush {}", std::move(fut).get() ? "success" : "failed");
}

void HSHomeObject::on_snp_rcvr_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf) {
    LOGINFO("Found snapshot info meta blk");
    homestore::superblk< snapshot_rcvr_info_superblk > sb(_snp_rcvr_meta_name);
    sb.load(buf, mblk);

    auto hs_pg = get_hs_pg(sb->pg_id);
    RELEASE_ASSERT(hs_pg != nullptr, "PG not found");
    if (!hs_pg->snp_rcvr_info_sb_.is_empty()) { hs_pg->snp_rcvr_info_sb_.destroy(); }
    hs_pg->snp_rcvr_info_sb_ = std::move(sb);

}

void HSHomeObject::on_snp_rcvr_meta_blk_recover_completed(bool success) {
    LOGINFO("Snapshot info meta blk recovery completed");
}

void HSHomeObject::on_snp_rcvr_shard_list_meta_blk_found(homestore::meta_blk* mblk, sisl::byte_view buf) {
    LOGINFO("Found snapshot shard list meta blk");
    homestore::superblk< snapshot_rcvr_shard_list_superblk > sb(_snp_rcvr_shard_list_meta_name);
    sb.load(buf, mblk);

    auto hs_pg = get_hs_pg(sb->pg_id);
    RELEASE_ASSERT(hs_pg != nullptr, "PG not found");
    if (!hs_pg->snp_rcvr_shard_list_sb_.is_empty()) { hs_pg->snp_rcvr_shard_list_sb_.destroy(); }
    hs_pg->snp_rcvr_shard_list_sb_ = std::move(sb);

}

void HSHomeObject::on_snp_rcvr_shard_list_meta_blk_recover_completed(bool success) {
    LOGINFO("Snapshot shard list meta blk recovery completed");
}

} // namespace homeobject
