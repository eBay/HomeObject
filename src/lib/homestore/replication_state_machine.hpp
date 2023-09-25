#pragma once

#include "homeobject.hpp"

#include <homestore/replication_service.hpp>
#include <homestore/replication/repl_dev.h>

namespace homeobject {

class HomeObjectImpl;

class HOReplServiceCallbacks : public homestore::ReplServiceCallbacks {
    HSHomeObject* _home_object{nullptr};

public:
    explicit HOReplServiceCallbacks(HSHomeObject* home_object) : _home_object(home_object) {}
    virtual ~HOReplServiceCallbacks() = default;
    virtual std::unique_ptr< homestore::ReplDevListener >
    on_repl_dev_init(homestore::cshared< homestore::ReplDev >& rs);
};

class ReplicationStateMachine : public homestore::ReplDevListener {
public:
    explicit ReplicationStateMachine(HSHomeObject* home_object, homestore::ReplDev& repl_dev) :
            _home_object(home_object), _repl_dev(repl_dev) {}

    virtual ~ReplicationStateMachine() = default;

    /// @brief Called when the log entry has been committed in the replica set.
    ///
    /// This function is called from a dedicated commit thread which is different from the original thread calling
    /// replica_set::write(). There is only one commit thread, and lsn is guaranteed to be monotonically increasing.
    ///
    /// @param lsn - The log sequence number
    /// @param header - Header originally passed with replica_set::write() api
    /// @param key - Key originally passed with replica_set::write() api
    /// @param blkids - List of blkids where data is written to the storage engine.
    /// @param ctx - User contenxt passed as part of the replica_set::write() api
    ///
    virtual void on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                           homestore::MultiBlkId const& blkids, void* ctx) override;

    /// @brief Called when the log entry has been received by the replica dev.
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
    /// @param header - Header originally passed with repl_dev::write() api
    /// @param key - Key originally passed with repl_dev::write() api
    /// @param ctx - User contenxt passed as part of the repl_dev::write() api
    virtual void on_pre_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key, void* ctx) override;

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
    /// @param header - Header originally passed with repl_dev::write() api
    /// @param key - Key originally passed with repl_dev::write() api
    /// @param ctx - User contenxt passed as part of the repl_dev::write() api
    virtual void on_rollback(int64_t lsn, const sisl::blob& header, const sisl::blob& key, void* ctx) override;

    /// @brief Called when replication module is trying to allocate a block to write the value
    ///
    /// This function can be called both on leader and follower when it is trying to allocate a block to write the
    /// value. Caller is expected to provide hints for allocation based on the header supplied as part of original
    /// write. In cases where caller don't care about the hints can return default blk_alloc_hints.
    ///
    /// @param header Header originally passed with repl_dev::write() api on the leader
    /// @return Expected to return blk_alloc_hints for this write
    virtual homestore::blk_alloc_hints get_blk_alloc_hints(sisl::blob const& header, void* user_ctx) override;

    /// @brief Called when the replica set is being stopped
    virtual void on_replica_stop() override;

private:
    HSHomeObject* _home_object;
    homestore::ReplDev&  _repl_dev;
};

} // namespace homeobject
