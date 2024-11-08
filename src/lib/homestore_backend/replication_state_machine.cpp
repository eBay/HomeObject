#include "replication_message.hpp"
#include "replication_state_machine.hpp"
#include "hs_backend_config.hpp"

namespace homeobject {
void ReplicationStateMachine::on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                                        const homestore::MultiBlkId& pbas, cintrusive< homestore::repl_req_ctx >& ctx) {
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    LOGD("applying raft log commit with lsn:{}, msg type: {}", lsn, msg_header->msg_type);
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_PG_MSG: {
        home_object_->on_create_pg_message_commit(lsn, header, repl_dev(), ctx);
        break;
    }
    case ReplicationMessageType::CREATE_SHARD_MSG:
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        home_object_->on_shard_message_commit(lsn, header, pbas, repl_dev(), ctx);
        break;
    }

    case ReplicationMessageType::PUT_BLOB_MSG: {
        home_object_->on_blob_put_commit(lsn, header, key, pbas, ctx);
        break;
    }
    case ReplicationMessageType::DEL_BLOB_MSG:
        home_object_->on_blob_del_commit(lsn, header, key, ctx);
        break;
    default: {
        break;
    }
    }
}

bool ReplicationStateMachine::on_pre_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                            cintrusive< homestore::repl_req_ctx >& ctx) {
    // For shard creation, since homestore repldev inside will write shard header to data service first before this
    // function is called. So there is nothing is needed to do and we can get the binding chunk_id with the newly shard
    // from the blkid in on_commit()
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    if (msg_header->corrupted()) {
        LOGE("corrupted message in pre_commit, lsn:{}", lsn);
        return false;
    }
    LOGD("on_pre_commit with lsn:{}, msg type: {}", lsn, msg_header->msg_type);
    switch (msg_header->msg_type) {
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        return home_object_->on_shard_message_pre_commit(lsn, header, key, ctx);
    }
    default: {
        break;
    }
    }
    return true;
}

void ReplicationStateMachine::on_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                          cintrusive< homestore::repl_req_ctx >& ctx) {
    LOGI("on_rollback  with lsn:{}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    if (msg_header->corrupted()) {
        LOGE("corrupted message in rollback, lsn:{}", lsn);
        return;
    }
    switch (msg_header->msg_type) {
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        home_object_->on_shard_message_rollback(lsn, header, key, ctx);
        break;
    }
    default: {
        break;
    }
    }
}

void ReplicationStateMachine::on_restart() { home_object_->on_replica_restart(); }

void ReplicationStateMachine::on_error(ReplServiceError error, const sisl::blob& header, const sisl::blob& key,
                                       cintrusive< repl_req_ctx >& ctx) {
    RELEASE_ASSERT(ctx, "ctx should not be nullptr in on_error");
    RELEASE_ASSERT(ctx->is_proposer(), "on_error should only be called from proposer");
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    LOGE("on_error, message type {} with lsn {}, error {}", msg_header->msg_type, ctx->lsn(), error);
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_PG_MSG: {
        auto result_ctx = boost::static_pointer_cast< repl_result_ctx< PGManager::NullResult > >(ctx).get();
        result_ctx->promise_.setValue(folly::makeUnexpected(homeobject::toPgError(error)));
        break;
    }
    case ReplicationMessageType::CREATE_SHARD_MSG:
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        auto result_ctx = boost::static_pointer_cast< repl_result_ctx< ShardManager::Result< ShardInfo > > >(ctx).get();
        result_ctx->promise_.setValue(folly::makeUnexpected(toShardError(error)));
        break;
    }

    case ReplicationMessageType::PUT_BLOB_MSG: {
        auto result_ctx =
            boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< HSHomeObject::BlobInfo > > >(ctx).get();
        result_ctx->promise_.setValue(folly::makeUnexpected(toBlobError(error)));
        break;
    }
    case ReplicationMessageType::DEL_BLOB_MSG: {
        auto result_ctx =
            boost::static_pointer_cast< repl_result_ctx< BlobManager::Result< HSHomeObject::BlobInfo > > >(ctx).get();
        result_ctx->promise_.setValue(folly::makeUnexpected(toBlobError(error)));
        break;
    }
    default: {
        LOGE("Unknown message type, error unhandled , error :{}, lsn {}", error, ctx->lsn());
        break;
    }
    }
}

homestore::ReplResult< homestore::blk_alloc_hints >
ReplicationStateMachine::get_blk_alloc_hints(sisl::blob const& header, uint32_t data_size) {
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        auto const [pg_found, shards_found, chunk_id] = home_object_->get_any_chunk_id(msg_header->pg_id);
        if (!pg_found) {
            LOGW("Requesting a chunk for an unknown pg={}, letting the caller retry after sometime", msg_header->pg_id);
            return folly::makeUnexpected(homestore::ReplServiceError::RESULT_NOT_EXIST_YET);
        } else if (!shards_found) {
            // pg is empty without any shards, we leave the decision the HeapChunkSelector to select a pdev
            // with most available space and then select one chunk based on that pdev
        } else {
            return home_object_->chunk_selector()->chunk_to_hints(chunk_id);
        }
        break;
    }

    case ReplicationMessageType::SEAL_SHARD_MSG: {
        auto chunk_id = home_object_->get_shard_chunk(msg_header->shard_id);
        RELEASE_ASSERT(chunk_id.has_value(), "unknown shard id to get binded chunk");
        homestore::blk_alloc_hints hints;
        hints.chunk_id_hint = chunk_id.value();
        return hints;
    }

    case ReplicationMessageType::PUT_BLOB_MSG:
        return home_object_->blob_put_get_blk_alloc_hints(header, nullptr);

    case ReplicationMessageType::DEL_BLOB_MSG:
    default:
        break;
    }

    return homestore::blk_alloc_hints();
}

void ReplicationStateMachine::on_replace_member(const homestore::replica_member_info& member_out,
                                                const homestore::replica_member_info& member_in) {
    home_object_->on_pg_replace_member(repl_dev()->group_id(), member_out, member_in);
}

void ReplicationStateMachine::on_destroy() {
    // TODO:: add the logic to handle destroy
    LOGI("replica destroyed");
}

homestore::AsyncReplResult<>
ReplicationStateMachine::create_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
    // TODO::add create snapshot logic
    auto ctx = dynamic_pointer_cast< homestore::nuraft_snapshot_context >(context);
    auto s = ctx->nuraft_snapshot();

    std::lock_guard lk(m_snapshot_lock);
    auto current = dynamic_pointer_cast< homestore::nuraft_snapshot_context >(m_snapshot_context)->nuraft_snapshot();
    if (s->get_last_log_idx() < current->get_last_log_idx()) {
        LOGI("Skipping create snapshot new idx/term: {}/{}  current idx/term: {}/{}", s->get_last_log_idx(),
             s->get_last_log_term(), current->get_last_log_idx(), current->get_last_log_term());
        return folly::makeSemiFuture< homestore::ReplResult< folly::Unit > >(folly::Unit{});
    }

    LOGI("create snapshot last_log_idx: {} last_log_term: {}", s->get_last_log_idx(), s->get_last_log_term());
    m_snapshot_context = context;
    return folly::makeSemiFuture< homestore::ReplResult< folly::Unit > >(folly::Unit{});
}

bool ReplicationStateMachine::apply_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
    // TODO persist snapshot
    std::lock_guard lk(m_snapshot_lock);
    m_snapshot_context = context;
    return true;
}

std::shared_ptr< homestore::snapshot_context > ReplicationStateMachine::last_snapshot() {
    std::lock_guard lk(m_snapshot_lock);
    auto snp = m_snapshot_context;
    return snp;
}

int ReplicationStateMachine::read_snapshot_data(std::shared_ptr< homestore::snapshot_context > context,
                                                std::shared_ptr< homestore::snapshot_data > snp_data) {
    HSHomeObject::PGBlobIterator* pg_iter = nullptr;
    auto s = dynamic_pointer_cast< homestore::nuraft_snapshot_context >(context)->nuraft_snapshot();

    if (snp_data->user_ctx == nullptr) {
        // Create the pg blob iterator for the first time.
        pg_iter = new HSHomeObject::PGBlobIterator(*home_object_, repl_dev()->group_id());
        snp_data->user_ctx = (void*)pg_iter;
    } else { pg_iter = r_cast< HSHomeObject::PGBlobIterator* >(snp_data->user_ctx); }

    // Nuraft uses obj_id as a way to track the state of the snapshot read and write.
    // Nuraft starts with obj_id == 0 as first message always, leader send all the shards and
    // PG metadata as response. Follower responds with next obj_id it expects. obj_id's are
    // encoded in the form ofobj_id (64 bits) = type_bit (1 bit) | shard_seq_num (48 bits) | batch_id (15 bits)
    // Leader starts with shard sequence number 1 and read upto maximum size of data
    // and send to follower in a batch. Once all blob's are send in a shard,
    // leader notifies the follower by setting is_last_batch in the payload. Follower
    // moves to the next shard by incrementing shard_seq_num and reset batch number to 0.
    // Batch number is used to identify which batch in the current shard sequence number.
    // We use pg blob iterator to go over all the blobs in all the shards in that PG.
    // Once all the shards are done, follower will return next obj Id = LAST_OBJ_ID(ULLONG_MAX) as a end marker,
    // leader will stop sending the snapshot data.
    auto log_str = fmt::format("group={}, term={}, lsn={},",
                               boost::uuids::to_string(repl_dev()->group_id()), s->get_last_log_term(),
                               s->get_last_log_idx());
    //TODO snp_data->offset is int64, need to change to uint64 in homestore
    if (snp_data->offset == int64_t(LAST_OBJ_ID)) {
        // No more shards to read, baseline resync is finished after this.
        snp_data->is_last_obj = true;
        LOGD("Read snapshot end, {}", log_str);
        return 0;
    }

    auto obj_id = objId(snp_data->offset);
    log_str = fmt::format("{} shard_seq_num={} batch_num={}", log_str, obj_id.shard_seq_num, obj_id.batch_id);

    //invalid Id
    if (!pg_iter->updateCursor(obj_id)) {
        LOGW("Invalid objId in snapshot read, {}, current shard_seq_num={}, current batch_num={}",
             log_str, pg_iter->cur_shard_seq_num, pg_iter->cur_batch_num);
        return -1;
    }

    //pg metadata message
    //shardId starts from 1
    if (obj_id.shard_seq_num == 0) {
        pg_iter->create_pg_snapshot_data(snp_data->blob);
        return 0;
    }
    //shard metadata message
    if (obj_id.shard_seq_num != 0 && obj_id.batch_id == 0) {
        if (!pg_iter->generate_shard_blob_list()) {
            LOGE("Failed to generate shard blob list for snapshot read, {}", log_str);
            return -1;
        };
        pg_iter->create_shard_snapshot_data(snp_data->blob);
        return 0;
    }
    //general blob message
    pg_iter->create_blobs_snapshot_data(snp_data->blob);
    return 0;
}

void ReplicationStateMachine::write_snapshot_data(std::shared_ptr< homestore::snapshot_context > context,
                                                  std::shared_ptr< homestore::snapshot_data > snp_data) {
    // RELEASE_ASSERT(context != nullptr, "Context null");
    // RELEASE_ASSERT(snp_data != nullptr, "Snapshot data null");
    // auto s = dynamic_pointer_cast< homestore::nuraft_snapshot_context >(context)->nuraft_snapshot();
    // int64_t obj_id = snp_data->offset;
    // uint64_t shard_seq_num = obj_id >> 16;
    // uint64_t batch_number = obj_id & 0xFFFF;
    //
    // auto log_str = fmt::format("group={} term={} lsn={} shard_seq={} batch_num={} size={}",
    //                            boost::uuids::to_string(repl_dev()->group_id()), s->get_last_log_term(),
    //                            s->get_last_log_idx(), shard_seq_num, batch_number, snp_data->blob.size());
    //
    // if (snp_data->is_last_obj) {
    //     LOGD("Write snapshot reached is_last_obj true {}", log_str);
    //     return;
    // }
    //
    // if (obj_id == 0) {
    //     snp_data->offset = 1 << 16;
    //     // TODO add metadata.
    //     return;
    // }
    //
    // auto snp = GetSizePrefixedResyncBlobDataBatch(snp_data->blob.bytes());
    // // TODO Add blob puts

    // if (snp->end_of_batch()) {
    //     snp_data->offset = (shard_seq_num + 1) << 16;
    // } else {
    //     snp_data->offset = (shard_seq_num << 16) | (batch_number + 1);
    // }
    //
    // LOGT("Read snapshot num_blobs={} end_of_batch={} {}", snp->data_array()->size(), snp->end_of_batch(), log_str);
}

void ReplicationStateMachine::free_user_snp_ctx(void*& user_snp_ctx) {
    if (user_snp_ctx) {
        LOGE("User snapshot context null group={}", boost::uuids::to_string(repl_dev()->group_id()));
        return;
    }

    auto pg_iter = r_cast< HSHomeObject::PGBlobIterator* >(user_snp_ctx);
    LOGD("Freeing snapshot iterator pg_id={} group={}", pg_iter->pg_id_, boost::uuids::to_string(pg_iter->group_id_));
    delete pg_iter;
}

} // namespace homeobject
