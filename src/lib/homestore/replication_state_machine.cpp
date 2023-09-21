#include "replication_message.hpp"
#include "replication_state_machine.hpp"

namespace homeobject {

std::unique_ptr< homestore::ReplDevListener >
HOReplServiceCallbacks::on_repl_dev_init(homestore::cshared< homestore::ReplDev >& repl_dev) {
    return std::make_unique< ReplicationStateMachine >(_home_object, repl_dev);
}

void ReplicationStateMachine::on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                                        homestore::MultiBlkId const& blkids, void* ctx) {
    LOGINFO("applying raft log commit with lsn:{}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.bytes);
    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG:
    case ReplicationMessageType::SEAL_SHARD_MSG: {
        _home_object->on_shard_message_commit(lsn, header, key, blkids, ctx, _repl_dev);
        break;
    }
    case ReplicationMessageType::PUT_BLOB_MSG:
    case ReplicationMessageType::DEL_BLOB_MSG:
    default: {
        break;
    }
    }
}

void ReplicationStateMachine::on_pre_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key, void* ctx) {
    LOGINFO("on_pre_commit with lsn:{}", lsn);
    // For shard creation, since homestore repldev inside will write shard header to data service first before this
    // function is called. So there is nothing is needed to do and we can get the binding chunk_id with the newly shard
    // from the blkid in on_commit()
}

void ReplicationStateMachine::on_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key, void* ctx) {
    LOGINFO("on_rollback  with lsn:{}", lsn);
}

homestore::blk_alloc_hints ReplicationStateMachine::get_blk_alloc_hints(sisl::blob const& header, void* user_ctx) {
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.bytes);
    if (msg_header->header_crc != msg_header->calculate_crc()) {
        LOGWARN("replication message header is corrupted with crc error and can not get blk alloc hints");
        return homestore::blk_alloc_hints();
    }

    switch (msg_header->msg_type) {
    case ReplicationMessageType::CREATE_SHARD_MSG: {
        auto list_shard_result = _home_object->shard_manager()->list_shards(msg_header->repl_group_id).get();
        if (!list_shard_result) {
            LOGWARN("list shards failed with unknown pg {}", msg_header->repl_group_id);
            break;
        }

        if (list_shard_result.value().empty()) {
            // pg is empty without any shards, we leave the decision the HeapChunkSelector to select a pdev
            // with most available space and select one chunk based on that pdev
        } else {
            auto chunk_id = _home_object->get_shard_chunk(list_shard_result.value().front().id);
            RELEASE_ASSERT(!!chunk_id, "unknown shard id to get binded chunk");
            // TODO:HS will add a new interface to get alloc hint based on a reference chunk;
            // and we can will call that interface for return alloc hint;
        }
        break;
    }

    case ReplicationMessageType::SEAL_SHARD_MSG:
    case ReplicationMessageType::PUT_BLOB_MSG:
    case ReplicationMessageType::DEL_BLOB_MSG:
    default: {
        break;
    }
    }

    return homestore::blk_alloc_hints();
}

void ReplicationStateMachine::on_replica_stop() {}

} // namespace homeobject
