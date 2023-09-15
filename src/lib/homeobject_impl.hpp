#pragma once

#include "homeobject/homeobject.hpp"
#include "homeobject/blob_manager.hpp"
#include "homeobject/pg_manager.hpp"
#include "homeobject/shard_manager.hpp"

#include <sisl/logging/logging.h>

namespace home_replication {
class ReplicationService;
}

namespace homeobject {

inline bool operator<(ShardInfo const& lhs, ShardInfo const& rhs) { return lhs.id < rhs.id; }
inline bool operator==(ShardInfo const& lhs, ShardInfo const& rhs) { return lhs.id == rhs.id; }

struct Shard {
    explicit Shard(ShardInfo info) : info(std::move(info)) {}
    ShardInfo info;
    uint16_t chunk_id;
    void* metablk_cookie{nullptr};
};

using ShardList = std::list< Shard >;
using ShardIterator = ShardList::iterator;

struct PG {
    explicit PG(PGInfo info) : pg_info(std::move(info)) {}
    PGInfo pg_info;
    uint64_t shard_sequence_num{0};
    ShardList shards;
};

class HomeObjectImpl : public HomeObject,
                       public BlobManager,
                       public PGManager,
                       public ShardManager,
                       public std::enable_shared_from_this< HomeObjectImpl > {

    /// Implementation defines these
    virtual ShardManager::Result< ShardInfo > _create_shard(pg_id, uint64_t size_bytes) = 0;
    virtual ShardManager::Result< ShardInfo > _seal_shard(shard_id) = 0;

    virtual BlobManager::Result< blob_id > _put_blob(ShardInfo const&, Blob&&) = 0;
    virtual BlobManager::Result< Blob > _get_blob(ShardInfo const&, blob_id) const = 0;
    virtual BlobManager::NullResult _del_blob(ShardInfo const&, blob_id) = 0;
    ///
    folly::Future< ShardManager::Result< Shard > > _get_shard(shard_id id) const;
    auto _defer() const { return folly::makeSemiFuture().via(folly::getGlobalCPUExecutor());}

protected:
    std::mutex _repl_lock;
    std::shared_ptr< home_replication::ReplicationService > _repl_svc;
    //std::shared_ptr<homestore::ReplicationService> _repl_svc;  
    peer_id _our_id;

    /// Our SvcId retrieval and SvcId->IP mapping
    std::weak_ptr< HomeObjectApplication > _application;

    ///
    mutable std::shared_mutex _pg_lock;
    std::map< pg_id, PG > _pg_map;

    mutable std::shared_mutex _shard_lock;
    std::map< shard_id, ShardIterator > _shard_map;
    ///
    PGManager::Result< PG > _get_pg(pg_id pg);
public:
    explicit HomeObjectImpl(std::weak_ptr< HomeObjectApplication >&& application) :
            _application(std::move(application)) {}

    ~HomeObjectImpl() override = default;
    HomeObjectImpl(const HomeObjectImpl&) = delete;
    HomeObjectImpl(HomeObjectImpl&&) noexcept = delete;
    HomeObjectImpl& operator=(const HomeObjectImpl&) = delete;
    HomeObjectImpl& operator=(HomeObjectImpl&&) noexcept = delete;

    // This is public but not exposed in the API above
    void init_repl_svc();

    std::shared_ptr< home_replication::ReplicationService > get_repl_svc()  { return _repl_svc;}

    std::shared_ptr< BlobManager > blob_manager() final;
    std::shared_ptr< PGManager > pg_manager() final;
    std::shared_ptr< ShardManager > shard_manager() final;

    peer_id our_uuid() const final { return _our_id; }

    /// PgManager
    PGManager::NullAsyncResult create_pg(PGInfo&& pg_info) final;
    PGManager::NullAsyncResult replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member) final;

    /// ShardManager
    ShardManager::AsyncResult< ShardInfo > get_shard(shard_id id) const final;
    ShardManager::AsyncResult< ShardInfo > create_shard(pg_id pg_owner, uint64_t size_bytes) final;
    ShardManager::AsyncResult< InfoList > list_shards(pg_id pg) const final;
    ShardManager::AsyncResult< ShardInfo > seal_shard(shard_id id) final;
    uint64_t get_current_timestamp();
    /// BlobManager
    BlobManager::AsyncResult< blob_id > put(shard_id shard, Blob&&) final;
    BlobManager::AsyncResult< Blob > get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len) const final;
    BlobManager::NullAsyncResult del(shard_id shard, blob_id const& blob) final;
};

} // namespace homeobject
