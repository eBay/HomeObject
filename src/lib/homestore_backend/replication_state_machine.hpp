#pragma once

#include <folly/futures/Future.h>
#include <homestore/replication/repl_dev.h>
#include "hs_homeobject.hpp"
#include "replication_message.hpp"

namespace homeobject {

class HSHomeObject;

struct ho_repl_ctx : public homestore::repl_req_ctx {
    ReplicationMessageHeader header_;
    sisl::io_blob_safe hdr_buf_;

    ho_repl_ctx(uint32_t size, uint32_t alignment) : homestore::repl_req_ctx{}, hdr_buf_{size, alignment} {}
    template < typename T >
    T* to() {
        return r_cast< T* >(this);
    }
};

template < typename T >
struct repl_result_ctx : public ho_repl_ctx {
    folly::Promise< T > promise_;

    template < typename... Args >
    static intrusive< repl_result_ctx< T > > make(Args&&... args) {
        return intrusive< repl_result_ctx< T > >{new repl_result_ctx< T >(std::forward< Args >(args)...)};
    }

    repl_result_ctx(uint32_t hdr_size, uint32_t alignment) : ho_repl_ctx{hdr_size, alignment} {}
    folly::SemiFuture< T > result() { return promise_.getSemiFuture(); }
};

class ReplicationStateMachine : public homestore::ReplDevListener {
public:
    explicit ReplicationStateMachine(HSHomeObject* home_object) : home_object_(home_object) {}

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

    void on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key, homestore::MultiBlkId const& blkids,
                   cintrusive< homestore::repl_req_ctx >& ctx) override;

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

    /// @param header - Header originally passed with replica_set::write() api
    /// @param key - Key originally passed with replica_set::write() api
    /// @param ctx - User contenxt passed as part of the replica_set::write() api
    bool on_pre_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                       cintrusive< homestore::repl_req_ctx >& ctx) override;

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
    void on_rollback(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                     cintrusive< homestore::repl_req_ctx >& ctx) override;

    /// @brief Called when replication module is trying to allocate a block to write the value
    ///
    /// This function can be called both on leader and follower when it is trying to allocate a block to write the
    /// value. Caller is expected to provide hints for allocation based on the header supplied as part of original
    /// write. In cases where caller don't care about the hints can return default blk_alloc_hints.
    ///
    /// @param header Header originally passed with repl_dev::write() api on the leader
    /// @return Expected to return blk_alloc_hints for this write
    homestore::blk_alloc_hints get_blk_alloc_hints(sisl::blob const& header,
                                                   cintrusive< homestore::repl_req_ctx >& ctx) override;

    /// @brief Called when the replica set is being stopped
    void on_replica_stop() override;

private:
    HSHomeObject* home_object_{nullptr};
};

} // namespace homeobject
