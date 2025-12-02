#pragma once

#include "homeobject/homeobject.hpp"
#include "homeobject/blob_manager.hpp"
#include "homeobject/pg_manager.hpp"
#include "homeobject/shard_manager.hpp"
#include <boost/intrusive_ptr.hpp>
#include <sisl/logging/logging.h>

#define LOGT(...) LOGTRACEMOD(homeobject, ##__VA_ARGS__)
#define LOGD(...) LOGDEBUGMOD(homeobject, ##__VA_ARGS__)
#define LOGI(...) LOGINFOMOD(homeobject, ##__VA_ARGS__)
#define LOGW(...) LOGWARNMOD(homeobject, ##__VA_ARGS__)
#define LOGE(...) LOGERRORMOD(homeobject, ##__VA_ARGS__)
#define LOGC(...) LOGCRITICALMOD(homeobject, ##__VA_ARGS__)

namespace homeobject {

template < typename T >
using shared = std::shared_ptr< T >;

template < typename T >
using cshared = const std::shared_ptr< T >;

template < typename T >
using unique = std::unique_ptr< T >;

template < typename T >
using intrusive = boost::intrusive_ptr< T >;

template < typename T >
using cintrusive = const boost::intrusive_ptr< T >;
constexpr size_t pg_width = sizeof(pg_id_t) * 8;
constexpr size_t shard_width = (sizeof(shard_id_t) * 8) - pg_width;
constexpr size_t shard_mask = std::numeric_limits< homeobject::shard_id_t >::max() >> pg_width;

inline shard_id_t make_new_shard_id(pg_id_t pg, shard_id_t next_shard) {
    return ((uint64_t)pg << shard_width) | next_shard;
}

struct Shard {
    explicit Shard(ShardInfo info) : info(std::move(info)) {}
    virtual ~Shard() = default;
    ShardInfo info;
    bool is_open() { return ShardInfo::State::OPEN == info.state; }
};

using ShardPtr = unique< Shard >;
using ShardPtrList = std::list< ShardPtr >;
using ShardIterator = ShardPtrList::iterator;

struct PG {
    explicit PG(PGInfo info) : pg_info_(std::move(info)) {}
    PG(PG const& pg) = delete;
    PG(PG&& pg) = default;
    PG& operator=(PG const& pg) = delete;
    PG& operator=(PG&& pg) = default;
    virtual ~PG() = default;

    struct DurableEntities {
        std::atomic< blob_id_t > blob_sequence_num{0ull};
        std::atomic< uint64_t > active_blob_count{0ull};
        std::atomic< uint64_t > tombstone_blob_count{0ull};
        std::atomic< uint64_t > total_occupied_blk_count{0ull};  // this will only decrease after GC
        std::atomic< uint64_t > total_reclaimed_blk_count{0ull}; // this will start from 0 if baseline resync happens
    };

    PGInfo pg_info_;
    uint64_t shard_sequence_num_{0};
    std::atomic< bool > is_dirty_{false};
    ShardPtrList shards_;

    void durable_entities_update(auto&& cb, bool dirty = true) {
        cb(durable_entities_);
        if (dirty) { is_dirty_.store(true, std::memory_order_relaxed); }
    }

    DurableEntities const& durable_entities() const { return durable_entities_; }

protected:
    DurableEntities durable_entities_;
};

class HomeObjectImpl : public HomeObject,
                       public BlobManager,
                       public PGManager,
                       public ShardManager,
                       public std::enable_shared_from_this< HomeObjectImpl > {

    /// Implementation defines these
    virtual ShardManager::AsyncResult< ShardInfo > _create_shard(pg_id_t, uint64_t size_bytes, trace_id_t tid) = 0;
    virtual ShardManager::AsyncResult< ShardInfo > _seal_shard(ShardInfo const&, trace_id_t tid) = 0;

    virtual BlobManager::AsyncResult< blob_id_t > _put_blob(ShardInfo const&, Blob&&, trace_id_t tid) = 0;
    virtual BlobManager::AsyncResult< Blob > _get_blob(ShardInfo const&, blob_id_t, uint64_t off, uint64_t len,
                                                       bool allow_skip_verify, trace_id_t tid) const = 0;
    virtual BlobManager::NullAsyncResult _del_blob(ShardInfo const&, blob_id_t, trace_id_t tid) = 0;
    ///

    virtual PGManager::NullAsyncResult _create_pg(PGInfo&& pg_info, std::set< peer_id_t > const& peers,
                                                  trace_id_t tid) = 0;
    virtual PGManager::NullAsyncResult _replace_member(pg_id_t id, std::string& task_id, peer_id_t const& old_member,
                                                       PGMember const& new_member, uint32_t commit_quorum,
                                                       trace_id_t trace_id) = 0;
    virtual PGReplaceMemberStatus _get_replace_member_status(pg_id_t id, std::string& task_id,
                                                             const PGMember& old_member, const PGMember& new_member,
                                                             const std::vector< PGMember >& others,
                                                             uint64_t trace_id) const = 0;
    virtual bool _get_stats(pg_id_t id, PGStats& stats) const = 0;
    virtual void _get_pg_ids(std::vector< pg_id_t >& pg_ids) const = 0;

    virtual HomeObjectStats _get_stats() const = 0;

    virtual void _destroy_pg(pg_id_t pg_id) = 0;

    virtual PGManager::NullResult _exit_pg(uuid_t group_id, peer_id_t peer_id, trace_id_t trace_id) = 0;

    virtual PGManager::NullAsyncResult _flip_learner_flag(pg_id_t pg_id, peer_id_t const& member_id, bool is_learner,
                                                          uint32_t commit_quorum, trace_id_t trace_id) = 0;

    virtual PGManager::NullAsyncResult _remove_member(pg_id_t pg_id, peer_id_t const& member_id, uint32_t commit_quorum,
                                                      trace_id_t trace_id) = 0;

    virtual PGManager::NullAsyncResult _clean_replace_member_task(pg_id_t pg_id, std::string& task_id,
                                                                  uint32_t commit_quorum, trace_id_t trace_id) = 0;

    virtual PGManager::Result< std::vector< replace_member_task > >
    _list_all_replace_member_tasks(trace_id_t trace_id) = 0;

protected:
    std::mutex _repl_lock;
    peer_id_t _our_id;

    /// Our SvcId retrieval and SvcId->IP mapping
    std::weak_ptr< HomeObjectApplication > _application;

    folly::Executor::KeepAlive<> executor_;

    ///
    mutable std::shared_mutex _pg_lock;
    std::map< pg_id_t, unique< PG > > _pg_map;

    mutable std::shared_mutex _shard_lock;
    std::map< shard_id_t, ShardIterator > _shard_map;
    ///

    auto _defer() const { return folly::makeSemiFuture().via(executor_); }
    folly::Future< ShardManager::Result< ShardInfo > > _get_shard(shard_id_t id, trace_id_t tid) const;

public:
    explicit HomeObjectImpl(std::weak_ptr< HomeObjectApplication >&& application);

    ~HomeObjectImpl() override = default;
    HomeObjectImpl(const HomeObjectImpl&) = delete;
    HomeObjectImpl(HomeObjectImpl&&) noexcept = delete;
    HomeObjectImpl& operator=(const HomeObjectImpl&) = delete;
    HomeObjectImpl& operator=(HomeObjectImpl&&) noexcept = delete;

    std::shared_ptr< BlobManager > blob_manager() final;
    std::shared_ptr< PGManager > pg_manager() final;
    std::shared_ptr< ShardManager > shard_manager() final;

    /// HomeObject
    /// Returns the UUID of this HomeObject.
    peer_id_t our_uuid() const final { return _our_id; }
    HomeObjectStats get_stats() const final { return _get_stats(); }
    void shutdown() { LOGI("HomeObjectImpl: Executing shutdown procedure"); };

    /// PgManager
    PGManager::NullAsyncResult create_pg(PGInfo&& pg_info, trace_id_t tid) final;
    PGManager::NullAsyncResult replace_member(pg_id_t id, std::string& task_id, peer_id_t const& old_member,
                                              PGMember const& new_member, u_int32_t commit_quorum,
                                              trace_id_t trace_id) final;
    PGReplaceMemberStatus get_replace_member_status(pg_id_t id, std::string& task_id, const PGMember& member_out,
                                                    const PGMember& member_in, const std::vector< PGMember >& others,
                                                    uint64_t trace_id) const final;
    // see api comments in base class;
    bool get_stats(pg_id_t id, PGStats& stats) const final;
    void get_pg_ids(std::vector< pg_id_t >& pg_ids) const final;
    void destroy_pg(pg_id_t pg_id) final;
    PGManager::NullResult exit_pg(uuid_t group_id, peer_id_t peer_id, trace_id_t trace_id) final;
    PGManager::NullAsyncResult flip_learner_flag(pg_id_t pg_id, peer_id_t const& member_id, bool is_learner,
                                                 uint32_t commit_quorum, trace_id_t trace_id) final;
    PGManager::NullAsyncResult remove_member(pg_id_t pg_id, peer_id_t const& member_id, uint32_t commit_quorum,
                                             trace_id_t trace_id) final;
    PGManager::NullAsyncResult clean_replace_member_task(pg_id_t pg_id, std::string& task_id, uint32_t commit_quorum,
                                                         trace_id_t trace_id) final;
    PGManager::Result< std::vector< replace_member_task > > list_all_replace_member_tasks(trace_id_t trace_id) final;

    /// ShardManager
    ShardManager::AsyncResult< ShardInfo > get_shard(shard_id_t id, trace_id_t tid) const final;
    ShardManager::AsyncResult< ShardInfo > create_shard(pg_id_t pg_owner, uint64_t size_bytes, trace_id_t tid) final;
    ShardManager::AsyncResult< InfoList > list_shards(pg_id_t pg, trace_id_t tid) const final;
    ShardManager::AsyncResult< ShardInfo > seal_shard(shard_id_t id, trace_id_t tid) final;
    uint64_t get_current_timestamp();

    /// BlobManager
    BlobManager::AsyncResult< blob_id_t > put(shard_id_t shard, Blob&&, trace_id_t tid) final;
    BlobManager::AsyncResult< Blob > get(shard_id_t shard, blob_id_t const& blob, uint64_t off, uint64_t len,
                                         bool allow_skip_verify, trace_id_t tid) const final;
    BlobManager::NullAsyncResult del(shard_id_t shard, blob_id_t const& blob, trace_id_t tid) final;
};

} // namespace homeobject
