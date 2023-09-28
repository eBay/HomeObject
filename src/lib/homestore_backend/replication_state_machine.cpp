#include "replication_message.hpp"
#include "replication_state_machine.hpp"

namespace homeobject {

void ReplicationStateMachine::on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                                        const homestore::MultiBlkId& pbas, cintrusive< homestore::repl_req_ctx >& ctx) {
    LOGINFO("applying raft log commit with lsn:{}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.bytes);

    switch (msg_header->message_type) {
    case ReplicationMessageType::SHARD_MESSAGE: {
        _home_object->on_shard_message_commit(lsn, header, key, ctx);
        break;
    }
    case ReplicationMessageType::PG_MESSAGE:
    case ReplicationMessageType::BLOB_MESSAGE:
    default: {
        break;
    }
    }
}

bool ReplicationStateMachine::on_pre_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                            cintrusive< homestore::repl_req_ctx >& ctx) {
    bool ret{false};
    LOGINFO("on_pre_commit with lsn:{}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.bytes);

    switch (msg_header->message_type) {
    case ReplicationMessageType::SHARD_MESSAGE: {
        ret = _home_object->on_pre_commit_shard_msg(lsn, header, key, ctx);
        break;
    }
    case ReplicationMessageType::PG_MESSAGE:
    case ReplicationMessageType::BLOB_MESSAGE:
    default: {
        break;
    }
    }
    return ret;
}

void ReplicationStateMachine::on_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                          cintrusive< homestore::repl_req_ctx >& ctx) {
    LOGINFO("rollback with lsn:{}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast< const ReplicationMessageHeader* >(header.bytes);

    switch (msg_header->message_type) {
    case ReplicationMessageType::SHARD_MESSAGE: {
        _home_object->on_rollback_shard_msg(lsn, header, key, ctx);
        break;
    }
    case ReplicationMessageType::PG_MESSAGE:
    case ReplicationMessageType::BLOB_MESSAGE:
    default: {
        break;
    }
    }
}

homestore::blk_alloc_hints ReplicationStateMachine::get_blk_alloc_hints(sisl::blob const& header,
                                                                        cintrusive< homestore::repl_req_ctx >& ctx) {
    // TODO: Return blk_alloc_hints specific to create shard or blob put
    return homestore::blk_alloc_hints{};
}

void ReplicationStateMachine::on_replica_stop() {}

} // namespace homeobject
