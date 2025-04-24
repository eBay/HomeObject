#pragma once

#include <folly/futures/Future.h>
#include <homestore/replication/repl_dev.h>
#include <homestore/replication/repl_decls.h>
#include <homestore/blk.h>
#include "hs_homeobject.hpp"
#include "replication_message.hpp"

namespace homeobject {

class HSHomeObject;
using homestore::repl_req_ctx;
using homestore::ReplServiceError;

struct ho_repl_ctx : public homestore::repl_req_ctx {
    sisl::io_blob_safe hdr_buf_;
    sisl::io_blob_safe key_buf_;

    // Data bufs corresponding to data_sgs_. Since data_sgs are raw pointers, we need to keep the data bufs alive
    folly::small_vector< sisl::io_blob_safe, 3 > data_bufs_;
    sisl::sg_list data_sgs_;

    ho_repl_ctx(uint32_t hdr_extn_size, uint32_t key_size = 0) : homestore::repl_req_ctx{} {
        hdr_buf_ = std::move(sisl::io_blob_safe{uint32_cast(sizeof(ReplicationMessageHeader) + hdr_extn_size), 0});
        new (hdr_buf_.bytes()) ReplicationMessageHeader();

        if (key_size) { key_buf_ = std::move(sisl::io_blob_safe{key_size, 0}); }
        data_sgs_.size = 0;
    }

    ~ho_repl_ctx() {
        if (hdr_buf_.bytes()) { header()->~ReplicationMessageHeader(); }
    }

    template < typename T >
    T* to() {
        return r_cast< T* >(this);
    }

    ReplicationMessageHeader* header() { return r_cast< ReplicationMessageHeader* >(hdr_buf_.bytes()); }
    uint8_t* header_extn() { return hdr_buf_.bytes() + sizeof(ReplicationMessageHeader); }

    sisl::io_blob_safe& header_buf() { return hdr_buf_; }
    sisl::io_blob_safe const& cheader_buf() const { return hdr_buf_; }

    sisl::io_blob_safe& key_buf() { return key_buf_; }
    sisl::io_blob_safe const& ckey_buf() const { return key_buf_; }

    void add_data_sg(uint8_t* buf, uint32_t size) {
        data_sgs_.iovs.emplace_back(iovec{.iov_base = buf, .iov_len = size});
        data_sgs_.size += size;
    }

    void add_data_sg(sisl::io_blob_safe&& buf) {
        add_data_sg(buf.bytes(), buf.size());
        data_bufs_.emplace_back(std::move(buf));
    }

    sisl::sg_list& data_sgs() { return data_sgs_; }
    std::string data_sgs_string() const {
        fmt::memory_buffer buf;
        fmt::format_to(fmt::appender(buf), "total_size={} iovcnt={} [", data_sgs_.size, data_sgs_.iovs.size());
        for (auto const& iov : data_sgs_.iovs) {
            fmt::format_to(fmt::appender(buf), "<base={},len={}> ", iov.iov_base, iov.iov_len);
        }
        fmt::format_to(fmt::appender(buf), "]");
        return fmt::to_string(buf);
    }
};

template < typename T >
struct repl_result_ctx : public ho_repl_ctx {
    folly::Promise< T > promise_;

    template < typename... Args >
    static intrusive< repl_result_ctx< T > > make(Args&&... args) {
        return intrusive< repl_result_ctx< T > >{new repl_result_ctx< T >(std::forward< Args >(args)...)};
    }

    repl_result_ctx(uint32_t hdr_extn_size, uint32_t key_size = 0) : ho_repl_ctx{hdr_extn_size, key_size} {}
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

    void on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                   std::vector< homestore::MultiBlkId > const& blkids,
                   cintrusive< homestore::repl_req_ctx >& ctx) override;

    /// @brief Called when the log entry has been committed in the replica set.
    ///
    /// This function is called from a dedicated commit thread which is different from the original thread calling
    /// replica_set::write(). There is only one commit thread, and lsn is guaranteed to be monotonically increasing.
    ///
    /// @param lsn - The log sequence number
    /// @param header - Header originally passed with replica_set::write() api
    /// @param key - Key originally passed with replica_set::write() api
    /// @param blkids - List of independent blkids where data is written to the storage engine.
    /// @param ctx - Context passed as part of the replica_set::write() api
    ///
    virtual void on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                           std::vector< homestore::MultiBlkId > const& blkids,
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

    /// @brief Called when the raft service is created after restart.
    ///
    /// homeobject should recover all the necessary components to serve log replay/commit requests.
    void on_restart() override;

    /// @brief Called when the async_alloc_write call failed to initiate replication
    ///
    /// Called only on the node which called async_alloc_write
    ///
    ///
    /// NOTE: Listener should do the free any resources created as part of pre-commit.
    ///
    /// @param header - Header originally passed with ReplDev::async_alloc_write() api
    /// @param key - Key originally passed with ReplDev::async_alloc_write() api
    /// @param ctx - Context passed as part of the ReplDev::async_alloc_write() api
    void on_error(ReplServiceError error, const sisl::blob& header, const sisl::blob& key,
                  cintrusive< repl_req_ctx >& ctx);

    /// @brief Called when replication module is trying to allocate a block to write the value
    ///
    /// This function can be called both on leader and follower when it is trying to allocate a block to write the
    /// value. Caller is expected to provide hints for allocation based on the header supplied as part of original
    /// write. In cases where caller don't care about the hints can return default blk_alloc_hints.
    ///
    /// @param header Header originally passed with repl_dev::write() api on the leader
    /// @return Expected to return blk_alloc_hints for this write
    homestore::ReplResult< homestore::blk_alloc_hints >
    get_blk_alloc_hints(sisl::blob const& header, uint32_t data_size,
                        cintrusive< homestore::repl_req_ctx >& hs_ctx) override;

    /// @brief Called when replication module is replacing an existing member with a new member
    void on_replace_member(const homestore::replica_member_info& member_out,
                           const homestore::replica_member_info& member_in) override;

    /// @brief Called when the replica is being destroyed by nuraft;
    void on_destroy(const homestore::group_id_t& group_id) override;

    // Snapshot related functions
    homestore::AsyncReplResult<> create_snapshot(std::shared_ptr< homestore::snapshot_context > context) override;
    bool apply_snapshot(std::shared_ptr< homestore::snapshot_context > context) override;
    std::shared_ptr< homestore::snapshot_context > last_snapshot() override;
    int read_snapshot_obj(std::shared_ptr< homestore::snapshot_context > context,
                          std::shared_ptr< homestore::snapshot_obj > snp_obj) override;
    void write_snapshot_obj(std::shared_ptr< homestore::snapshot_context > context,
                            std::shared_ptr< homestore::snapshot_obj > snp_obj) override;
    void free_user_snp_ctx(void*& user_snp_ctx) override;

    /// @brief ask upper layer to decide which data should be returned.
    // @param header - header of the log entry.
    // @param blkid - original blkid of the log entry
    // @param sgs - sgs to be filled with data
    // @param lsn - lsn of the log entry
    folly::Future< std::error_code > on_fetch_data(const int64_t lsn, const sisl::blob& header,
                                                   const homestore::MultiBlkId& local_blk_id,
                                                   sisl::sg_list& sgs) override;

    /// @brief ask upper layer to handle no_space_left event
    // @param lsn - on which repl_lsn no_space_left happened
    // @param chunk_id - on which chunk no_space_left happened
    void on_no_space_left(homestore::repl_lsn_t lsn, homestore::chunk_num_t chunk_id) override;

private:
    HSHomeObject* home_object_{nullptr};

    std::shared_ptr< homestore::snapshot_context > m_snapshot_context;
    std::mutex m_snapshot_lock;

    std::unique_ptr< HSHomeObject::SnapshotReceiveHandler > m_snp_rcv_handler;

    std::shared_ptr< homestore::snapshot_context > get_snapshot_context();
    void set_snapshot_context(std::shared_ptr< homestore::snapshot_context > context);

    bool validate_blob(shard_id_t shard_id, blob_id_t blob_id, void* data, size_t size) const;

    /* no space left error handling*/
private:
    // this is used to track the latest no_space_left error. It means after we commit to lsn, we have to start handling
    // no_space_left for the chunk(chunk_id)
    struct no_space_left_error_info {
        homestore::repl_lsn_t wait_commit_lsn{std::numeric_limits< homestore::repl_lsn_t >::max()};
        homestore::chunk_num_t chunk_id{0};
        mutable std::shared_mutex mutex;
    } m_no_space_left_error_info;

    void set_no_space_left_error_info(homestore::repl_lsn_t lsn, homestore::chunk_num_t chunk_id);

    void reset_no_space_left_error_info();

    std::pair< homestore::repl_lsn_t, homestore::chunk_num_t > get_no_space_left_error_info() const;

    void handle_no_space_left(homestore ::repl_lsn_t lsn, homestore ::chunk_num_t chunk_id);
};

} // namespace homeobject
