#include "replication_message.hpp"
#include "replication_state_machine.hpp"

namespace homeobject {

void ReplicationStateMachine::on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                                        const home_replication::pba_list_t& pbas, void* ctx) {
    LOGINFO("applying raft log commit with lsn:{}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast<const ReplicationMessageHeader*>(header.bytes);

    switch (msg_header->message_type) {
    case ReplicationMessageType::SHARD_MESSAGE: {
        _home_object->on_shard_message_commit(lsn, header, key,ctx);
        break;
    }
    case ReplicationMessageType::PG_MESSAGE:
    case ReplicationMessageType::BLOB_MESSAGE:
    default: {
        break;
    }
    }
}

void ReplicationStateMachine::on_pre_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key, void* ctx) {
    LOGINFO("on_pre_commit with lsn:{}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast<const ReplicationMessageHeader*>(header.bytes);

    switch (msg_header->message_type) {
    case ReplicationMessageType::SHARD_MESSAGE: {
        _home_object->on_pre_commit_shard_msg(lsn, header, key,ctx);
        break;
    }
    case ReplicationMessageType::PG_MESSAGE:
    case ReplicationMessageType::BLOB_MESSAGE:
    default: {
        break;
    }
    }
}

void ReplicationStateMachine::on_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key, void* ctx) {
    LOGINFO("rollback with lsn:{}", lsn);
    const ReplicationMessageHeader* msg_header = r_cast<const ReplicationMessageHeader*>(header.bytes);

    switch (msg_header->message_type) {
    case ReplicationMessageType::SHARD_MESSAGE: {
        _home_object->on_rollback_shard_msg(lsn, header, key,ctx);
        break;
    }
    case ReplicationMessageType::PG_MESSAGE:
    case ReplicationMessageType::BLOB_MESSAGE:
    default: {
        break;
    }
    }
}

void ReplicationStateMachine::on_replica_stop() {}

}
