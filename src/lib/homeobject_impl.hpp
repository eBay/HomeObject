#pragma once

#include "homeobject/homeobject.hpp"
#include "homeobject/blob_manager.hpp"
#include "homeobject/pg_manager.hpp"
#include "homeobject/shard_manager.hpp"

#include <sisl/logging/logging.h>

SISL_LOGGING_DECL(homeobject);

namespace homeobject {

class HomeObjectImpl : public HomeObject,
                       public BlobManager,
                       public PGManager,
                       public ShardManager,
                       public std::enable_shared_from_this< HomeObjectImpl > {
    HomeObject::lookup_cb _lookup_peer;

public:
    explicit HomeObjectImpl(HomeObject::lookup_cb const& lookup) : _lookup_peer(lookup) {}
    ~HomeObjectImpl() override = default;

    std::shared_ptr< BlobManager > blob_manager() override;
    std::shared_ptr< PGManager > pg_manager() override;
    std::shared_ptr< ShardManager > shard_manager() override;

    /// PgManager
    void create_pg(PGInfo const& pg_info, PGManager::ok_cb const& cb) override;
    void replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member,
                        PGManager::ok_cb const& cb) override;

    /// ShardManager
    void create_shard(pg_id pg_owner, uint64_t size_bytes, ShardManager::info_cb const& cb) override;
    void list_shards(pg_id pg, ShardManager::list_cb const& cb) const override;
    void get_shard(shard_id id, ShardManager::info_cb const& cb) const override;
    void seal_shard(shard_id id, ShardManager::info_cb const& cb) override;

    /// BlobManager
    void put(shard_id shard, Blob&&, BlobManager::id_cb const& cb) override;
    void get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len,
             BlobManager::get_cb const& cb) const override;
    void del(shard_id shard, blob_id const& blob, BlobManager::ok_cb const& cb) override;
};

} // namespace homeobject
