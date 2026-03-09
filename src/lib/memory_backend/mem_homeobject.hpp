#pragma once

#include <atomic>
#include <utility>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <folly/concurrency/ConcurrentHashMap.h>
#pragma GCC diagnostic pop
#include "lib/homeobject_impl.hpp"
#include "lib/blob_route.hpp"

namespace homeobject {

///
// Used to TombStone Blob's in the Index to defer for GC.
ENUM(BlobState, uint8_t, ALIVE = 0, DELETED);

struct BlobExt {
    BlobState state_{BlobState::DELETED};
    Blob* blob_;

    explicit operator bool() const { return state_ == BlobState::ALIVE; }
    bool operator==(const BlobExt& rhs) const { return blob_ == rhs.blob_; }
};

struct ShardIndex {
    folly::ConcurrentHashMap< BlobRoute, BlobExt > btree_;
    ~ShardIndex();
};

class MemoryHomeObject : public HomeObjectImpl {
    /// Simulates the Shard=>Chunk mapping in IndexSvc
    using index_svc = folly::ConcurrentHashMap< shard_id_t, std::unique_ptr< ShardIndex > >;
    index_svc index_;
    ///

    /// Helpers
    // ShardManager
    ShardManager::AsyncResult< ShardInfo > _create_shard(pg_id_t, uint64_t size_bytes, std::string meta, trace_id_t tid) override;
    ShardManager::AsyncResult< ShardInfo > _seal_shard(ShardInfo const&, trace_id_t tid) override;

    // BlobManager
    BlobManager::AsyncResult< blob_id_t > _put_blob(ShardInfo const&, Blob&&, trace_id_t tid) override;
    BlobManager::AsyncResult< Blob > _get_blob(ShardInfo const&, blob_id_t, uint64_t off, uint64_t len,
                                               bool allow_skip_verify, trace_id_t tid) const override;
    BlobManager::NullAsyncResult _del_blob(ShardInfo const&, blob_id_t, trace_id_t tid) override;
    ///

    // PGManager
    PGManager::NullAsyncResult _create_pg(PGInfo&& pg_info, std::set< peer_id_t > const& peers,
                                          trace_id_t tid) override;
    PGManager::NullAsyncResult _replace_member(pg_id_t id, std::string& task_id, peer_id_t const& old_member,
                                               PGMember const& new_member, uint32_t commit_quorum,
                                               trace_id_t tid) override;
    PGReplaceMemberStatus _get_replace_member_status(pg_id_t id, std::string& task_id, const PGMember& old_member,
                                                     const PGMember& new_member, const std::vector< PGMember >& others,
                                                     uint64_t trace_id) const override;

    bool _get_stats(pg_id_t id, PGStats& stats) const override;
    void _get_pg_ids(std::vector< pg_id_t >& pg_ids) const override;

    HomeObjectStats _get_stats() const override;
    void _destroy_pg(pg_id_t pg_id) override;
    PGManager::NullResult _exit_pg(uuid_t group_id, peer_id_t peer_id, trace_id_t trace_id) override;

    PGManager::NullAsyncResult _flip_learner_flag(pg_id_t pg_id, peer_id_t const& member_id, bool is_learner,
                                                  uint32_t commit_quorum, trace_id_t trace_id) override;
    PGManager::NullAsyncResult _remove_member(pg_id_t pg_id, peer_id_t const& member_id, uint32_t commit_quorum,
                                              trace_id_t trace_id) override;
    PGManager::NullAsyncResult _clean_replace_member_task(pg_id_t pg_id, std::string& task_id, uint32_t commit_quorum,
                                                          trace_id_t trace_id) override;
    PGManager::Result< std::vector< replace_member_task > >
    _list_all_replace_member_tasks(trace_id_t trace_id) override;

    ShardIndex& _find_index(shard_id_t) const;

public:
    MemoryHomeObject(std::weak_ptr< HomeObjectApplication >&& application);
    ~MemoryHomeObject() override = default;
    void shutdown() override final;
};

} // namespace homeobject
