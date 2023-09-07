#pragma once

#include <memory>
#include <mutex>

#include <home_replication/repl_service.h>

#include "lib/homeobject_impl.hpp"

namespace homeobject {

class HSHomeObject : public HomeObjectImpl {
    /// Overridable Helpers
    folly::Future< ShardManager::Result< ShardInfo > > _get_shard(shard_id) const override;
    ShardManager::Result< ShardInfo > _create_shard(pg_id, uint64_t size_bytes) override;
    ShardManager::Result< ShardInfo > _seal_shard(shard_id) override;

    BlobManager::Result< blob_id > _put_blob(ShardInfo const&, Blob&&) override;
    BlobManager::Result< Blob > _get_blob(ShardInfo const&, blob_id) const override;
    BlobManager::NullResult _del_blob(ShardInfo const&, blob_id) override;
    ///

public:
    using HomeObjectImpl::HomeObjectImpl;
    ~HSHomeObject() override = default;
};

} // namespace homeobject
