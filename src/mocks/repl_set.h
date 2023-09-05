#pragma once

#include <nuraft_mesg/messaging_if.hpp>
#include <sisl/fds/buffer.hpp>

#include "repl_decls.h"

namespace home_replication {

//
// Callbacks to be implemented by ReplicaSet users.
//
class ReplicaSetListener {
public:
    virtual ~ReplicaSetListener() = default;

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
    virtual void on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key, const pba_list_t& pbas,
                           void* ctx) = 0;

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
    virtual void on_pre_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key, void* ctx) = 0;

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
    virtual void on_rollback(int64_t lsn, const sisl::blob& header, const sisl::blob& key, void* ctx) = 0;

    /// @brief Called when the replica set is being stopped
    virtual void on_replica_stop() = 0;
};

class ReplicaSet : public nuraft_mesg::mesg_state_mgr {
public:
    virtual ~ReplicaSet() = default;

    /// @brief Replicate the data to the replica set. This method goes through the
    /// following steps:
    /// Step 1: Allocates pba from the storage engine to write the value into. Storage
    /// engine returns a pba_list in cases where single contiguous blocks are not
    /// available. For convenience, the comment will continue to refer pba_list as pba.
    /// Step 2: Uses data channel to send the <pba, value> to all replicas
    /// Step 3: Creates a log/journal entry with <header, key, pba> and calls nuraft to
    /// append the entry and replicate using nuraft channel (also called header_channel).
    ///
    /// @param header - Blob representing the header (it is opaque and will be copied
    /// as-is to the journal entry)
    /// @param key - Blob representing the key (it is opaque and will be copied as-is to
    /// the journal entry). We are tracking this seperately to support consistent read use
    /// cases
    /// @param value - vector of io buffers that contain value for the key
    /// @param user_ctx - User supplied opaque context which will be passed to listener
    /// callbacks
    virtual void write(const sisl::blob& header, const sisl::blob& key, const sisl::sg_list& value, void* user_ctx) = 0;

    /// @brief After data is replicated and on_commit to the listener is called. the pbas
    /// are implicityly transferred to listener. This call will transfer the ownership of
    /// pba back to the replication service. This listener should never free the pbas on
    /// its own and should always transfer the ownership after it is no longer useful.
    ///
    /// @param lsn - LSN of the old pba that is being transferred
    /// @param pbas - PBAs to be transferred.
    virtual void transfer_pba_ownership(int64_t lsn, const pba_list_t& pbas) = 0;

    /// @brief Send the final responce to the rpc client.
    /// @param outgoing_buf - response buf to client
    /// @param rpc_data - context provided by the rpc server
    virtual void send_data_service_response(sisl::io_blob_list_t const& outgoing_buf,
                                            boost::intrusive_ptr< sisl::GenericRpcData >& rpc_data) = 0;

    /// @brief Append an application/user specific message to the journal.
    ///
    /// @param buffer - Opaque buffer to be interpreted by the user
    virtual void append_entry(nuraft::buffer const& b) = 0;

    /// @brief Checks if this replica is the leader in this replica set
    /// @return true or false
    virtual bool is_leader() const = 0;

    virtual std::string group_id() const = 0;
};

} // namespace home_replication
