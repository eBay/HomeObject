#pragma once

#include "homeobject.hpp"
#include "mocks/repl_service.h"

namespace homeobject {

class HomeObjectImpl;

class ReplicationStateMachine : public home_replication::ReplicaSetListener {
public:
    /// @brief Called when the log entry has been committed in the replica set.
    ///
    /// This function is called from a dedicated commit thread which is different from the original thread calling
    /// replica_set::write(). There is only one commit thread, and lsn is guaranteed to be monotonically increasing.
    ///
    /// @param lsn - The log sequence number
    /// @param header - Header originally passed with replica_set::write() api
    /// @param key - Key originally passed with replica_set::write() api
    /// @param pbas - List of pbas where data is written to the storage engine.
    /// @param ctx - User contenxt passed as part of the replica_set::write() api
    ///
    virtual void on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                           const home_replication::pba_list_t& pbas, void* ctx);

    /// @brief Called when the log entry has been received by the replica set.
    ///
    /// On recovery, this is called from a random worker thread before the raft server is started. It is
    /// guaranteed to be serialized in log index order.
    ///
    /// On the leader, this is called from the same thread that replica_set::write() was called.
    ///
    /// On the follower, this is called when the follower has received the log entry. It is guaranteed to be serialized
    /// in log sequence order.
    ///
    /// NOTE: Listener can choose to ignore this pre commit, however, typical use case of maintaining this is in-case
    /// replica set needs to support strong consistent reads and follower needs to ignore any keys which are not being
    /// currently in pre-commit, but yet to be committed.
    ///
    /// @param lsn - The log sequence number
    /// @param header - Header originally passed with replica_set::write() api
    /// @param key - Key originally passed with replica_set::write() api
    /// @param ctx - User contenxt passed as part of the replica_set::write() api
    virtual void on_pre_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key, void* ctx);

    /// @brief Called when the log entry has been rolled back by the replica set.
    ///
    /// This function is called on followers only when the log entry is going to be overwritten. This function is called
    /// from a random worker thread, but is guaranteed to be serialized.
    ///
    /// For each log index, it is guaranteed that either on_commit() or on_rollback() is called but not both.
    ///
    /// NOTE: Listener should do the free any resources created as part of pre-commit.
    ///
    /// @param lsn - The log sequence number getting rolled back
    /// @param header - Header originally passed with replica_set::write() api
    /// @param key - Key originally passed with replica_set::write() api
    /// @param ctx - User contenxt passed as part of the replica_set::write() api
    virtual void on_rollback(int64_t lsn, const sisl::blob& header, const sisl::blob& key, void* ctx);

    /// @brief Called when the replica set is being stopped
    virtual void on_replica_stop() = 0;

private:
    HSHomeObject* _home_object;
};

}
