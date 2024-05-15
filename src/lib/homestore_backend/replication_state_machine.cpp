#include "replication_message.hpp"
#include "replication_state_machine.hpp"

namespace homeobject {
void ReplicationStateMachine::on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                                        const homestore::MultiBlkId& pbas, cintrusive< homestore::repl_req_ctx >& ctx) {
    LOGI("applying raft log commit with lsn:{}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
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
    LOGI("on_pre_commit with lsn:{}", lsn);
    // For shard creation, since homestore repldev inside will write shard header to data service first before this
    // function is called. So there is nothing is needed to do and we can get the binding chunk_id with the newly shard
    // from the blkid in on_commit()
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.cbytes());
    if (msg_header->corrupted()) {
        LOGE("corrupted message in pre_commit, lsn:{}", lsn);
        return false;
    }
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

void ReplicationStateMachine::on_destroy() {
    // TODO:: add the logic to handle destroy
    LOGI("replica destroyed");
}

homestore::AsyncReplResult<> ReplicationStateMachine::create_snapshot(homestore::repl_snapshot& s) {
    // TODO::add create snapshot logic
    LOGI("create snapshot, last_log_idx_: {} , last_log_term_: {}", s.last_log_idx_, s.last_log_term_);
    return folly::makeSemiFuture< homestore::ReplResult< folly::Unit > >(folly::Unit{});
}

} // namespace homeobject
