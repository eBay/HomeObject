#include "replication_state_machine.hpp"
#include "homeobject_impl.hpp"

namespace homeobject {

void ReplicationStateMachine::on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                        blkid_list_t const& blkids,void* ctx) {
    const ReplicationMessageHeader* msg_header = r_cast<const ReplicationMessageHeader*>(header.bytes);
    //TODO: read data from pba or get data from replication context(prefered)
    sisl::sg_list value;

    switch (msg_header->message_type) {
    case SHARD_MESSAGE: {
        home_object->on_shard_message_commit(lsn, header, key, value, ctx);
    }
    case PG_MESSAGE:
    case BLOB_MESSAGE:
    default: {
        break;
    }
    }
}

void ReplicationStateMachine::on_pre_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key, void* ctx) {}

void ReplicationStateMachine::on_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key, void* ctx) {}

blk_alloc_hints ReplicationStateMachine::get_blk_alloc_hints(sisl::blob const& header) {
    return blk_alloc_hints();
}

void ReplicationStateMachine::on_replica_stop() {}

}
