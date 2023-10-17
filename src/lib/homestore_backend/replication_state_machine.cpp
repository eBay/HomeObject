#include "replication_message.hpp"
#include "replication_state_machine.hpp"

namespace homeobject {
void ReplicationStateMachine::on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                                        const homestore::MultiBlkId& pbas, cintrusive< homestore::repl_req_ctx >& ctx) {
    LOGI("applying raft log commit with lsn:{}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.bytes);
    switch (msg_header->msg_type) {
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
        break;
    default: {
        break;
    }
    }
}


bool ReplicationStateMachine::on_pre_commit(int64_t lsn, sisl::blob const&, sisl::blob const&,
                                            cintrusive< homestore::repl_req_ctx >&) {
    LOGI("on_pre_commit with lsn:{}", lsn);
    // For shard creation, since homestore repldev inside will write shard header to data service first before this
    // function is called. So there is nothing is needed to do and we can get the binding chunk_id with the newly shard
    // from the blkid in on_commit()
    return true;
}

void ReplicationStateMachine::on_rollback(int64_t lsn, sisl::blob const&, sisl::blob const&,
                                          cintrusive< homestore::repl_req_ctx >&) {
    LOGI("on_rollback  with lsn:{}", lsn);
}

homestore::blk_alloc_hints ReplicationStateMachine::get_blk_alloc_hints(sisl::blob const& header,
                                                                        cintrusive< homestore::repl_req_ctx >& ctx) {
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.bytes);
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        auto any_allocated_chunk_id = home_object_->get_any_chunk_id(msg_header->pg_id);
        if (!any_allocated_chunk_id.has_value()) {
            // pg is empty without any shards, we leave the decision the HeapChunkSelector to select a pdev
            // with most available space and then select one chunk based on that pdev
        } else {
            return home_object_->chunk_selector()->chunk_to_hints(any_allocated_chunk_id.value());
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
        return home_object_->blob_put_get_blk_alloc_hints(header, ctx);
    case ReplicationMessageType::DEL_BLOB_MSG:
    default: {
        break;
    }
    }

    return homestore::blk_alloc_hints();
}

void ReplicationStateMachine::on_replica_stop() {}

} // namespace homeobject
