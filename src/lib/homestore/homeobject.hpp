#pragma once

#include <memory>
#include <mutex>

#include <home_replication/repl_service.h>

#include "lib/homeobject_impl.hpp"

namespace homeobject {

class HSHomeObject : public HomeObjectImpl {
    /// Overridable Helpers
    folly::Future< ShardManager::Result< ShardInfo > > _get_shard(shard_id id) const override;
    ShardManager::Result< ShardInfo > _create_shard(pg_id pg_owner, uint64_t size_bytes) override;
    ShardManager::Result< ShardInfo > _seal_shard(shard_id id) override;
    ShardManager::Result< InfoList > _list_shards(pg_id id) const override;

    BlobManager::Result< blob_id > _put_blob(shard_id, Blob&&) override;
    BlobManager::Result< Blob > _get_blob(shard_id id, blob_id blob) const override;
    BlobManager::NullResult _del_blob(shard_id id, blob_id blob) override;
    ///

    std::mutex _repl_lock;
    std::shared_ptr< home_replication::ReplicationService > _repl_svc;

public:
    using HomeObjectImpl::HomeObjectImpl;
    ~HSHomeObject() override = default;
};

} // namespace homeobject
