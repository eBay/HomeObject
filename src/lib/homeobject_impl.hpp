#pragma once

#include "homeobject/homeobject.hpp"
#include "homeobject/blob_manager.hpp"
#include "homeobject/pg_manager.hpp"
#include "homeobject/shard_manager.hpp"

#include <sisl/logging/logging.h>
/// TODO these should come from HomeReplication
#include "mocks/repl_service/repl_service.hpp"

SISL_LOGGING_DECL(homeobject);

namespace homeobject {

class HomeObjectImpl : public HomeObject,
                       public BlobManager,
                       public PGManager,
                       public ShardManager,
                       public home_replication::ReplicatedServer,
                       public std::enable_shared_from_this< HomeObjectImpl > {
    /// Our SvcId retrieval and SvcId->IP mapping
    init_params _svcid_routines;

    peer_id _our_id;

protected:
    /// This simulates the MetaBlkSvc thats used within real HomeObject
    mutable std::mutex _pg_lock;
    std::map< pg_id, std::unordered_set< shard_id > > _pg_map;
    ///

    std::mutex _repl_lock;
    std::shared_ptr< home_replication::ReplicationService > _repl_svc;

    void init_repl_svc();

public:
    explicit HomeObjectImpl(HomeObject::init_params&& params) : _svcid_routines(std::move(params)) {}
    ~HomeObjectImpl() override = default;

    std::shared_ptr< BlobManager > blob_manager() override;
    std::shared_ptr< PGManager > pg_manager() override;
    std::shared_ptr< ShardManager > shard_manager() override;

    folly::SemiFuture< std::string > member_address(boost::uuids::uuid const&) const override {
        return folly::makeSemiFuture(std::string());
    }

    /// PgManager
    folly::SemiFuture< PGError > create_pg(PGInfo&& pg_info) override;
    folly::SemiFuture< PGError > replace_member(pg_id id, peer_id const& old_member,
                                                PGMember const& new_member) override;

    /// ShardManager
    ShardManager::info_var get_shard(shard_id id) const override;
    folly::SemiFuture< ShardManager::info_var > create_shard(pg_id pg_owner, uint64_t size_bytes) override;
    folly::SemiFuture< ShardManager::list_var > list_shards(pg_id pg) const override;
    folly::SemiFuture< ShardManager::info_var > seal_shard(shard_id id) override;

    /// BlobManager
    folly::SemiFuture< std::variant< blob_id, BlobError > > put(shard_id shard, Blob&&) override;
    folly::SemiFuture< std::variant< Blob, BlobError > > get(shard_id shard, blob_id const& blob, uint64_t off,
                                                             uint64_t len) const override;
    folly::SemiFuture< BlobError > del(shard_id shard, blob_id const& blob) override;
};

} // namespace homeobject
