#pragma once

#include <nuraft_mesg/messaging_if.hpp>
#include <sisl/fds/buffer.hpp>

#include "repl_common.hpp"

namespace home_replication {

class ReplicaSet : public nuraft_mesg::mesg_state_mgr {
public:
    virtual ~ReplicaSet() = default;

    /// @brief Attach a nuraft::state_machine that will piggy-back on all class made to HomeReplication's own
    /// nuraft::state_machine (e.g. commit(), rollback()...)
    virtual void attach_state_machine(std::unique_ptr< nuraft::state_machine > state_machine) = 0;

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

    virtual bool is_leader() const = 0;
    virtual std::string_view group_id() const = 0;
};

} // namespace home_replication
