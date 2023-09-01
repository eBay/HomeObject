#pragma once

#include "homeobject/homeobject.hpp"
#include "homeobject/blob_manager.hpp"
#include "homeobject/pg_manager.hpp"
#include "homeobject/shard_manager.hpp"

#include <sisl/logging/logging.h>
#include <home_replication/repl_service.h>

namespace homeobject {

class HomeObjectImpl : public HomeObject,
                       public BlobManager,
                       public PGManager,
                       public ShardManager,
                       public std::enable_shared_from_this< HomeObjectImpl > {

    std::mutex _repl_lock;
    std::shared_ptr< home_replication::ReplicationService > _repl_svc;

protected:
    peer_id _our_id;

    /// Our SvcId retrieval and SvcId->IP mapping
    std::weak_ptr< HomeObjectApplication > _application;

    /// This simulates the MetaBlkSvc thats used within real HomeObject
    mutable std::mutex _pg_lock;
    std::map< pg_id, std::unordered_set< shard_id > > _pg_map;
    ///

    /// Overridable Helpers
    virtual folly::Future< ShardManager::Result< ShardInfo > > _get_shard(shard_id id) const = 0;
    virtual ShardManager::Result< ShardInfo > _create_shard(pg_id pg_owner, uint64_t size_bytes) = 0;
    virtual ShardManager::Result< InfoList > _list_shards(pg_id pg) const = 0;
    virtual ShardManager::Result< ShardInfo > _seal_shard(shard_id id) = 0;

    virtual BlobManager::Result< blob_id > _put_blob(shard_id, Blob&&) = 0;
    virtual BlobManager::Result< Blob > _get_blob(shard_id id, blob_id blob) const = 0;
    virtual BlobManager::NullResult _del_blob(shard_id id, blob_id blob) = 0;
    ///

public:
    explicit HomeObjectImpl(std::weak_ptr< HomeObjectApplication >&& application) :
            _application(std::move(application)) {}

    ~HomeObjectImpl() override = default;

    // This is public but not exposed in the API above
    void init_repl_svc();

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

    /// BlobManager
    BlobManager::AsyncResult< blob_id > put(shard_id shard, Blob&&) final;
    BlobManager::AsyncResult< Blob > get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len) const final;
    BlobManager::NullAsyncResult del(shard_id shard, blob_id const& blob) final;
};

} // namespace homeobject
