#pragma once

#include <atomic>
#include <filesystem>
#include <utility>

#include <folly/concurrency/ConcurrentHashMap.h>
#include "mocks/repl_service.h"

#include "lib/homeobject_impl.hpp"
#include "lib/blob_route.hpp"

namespace homeobject {

struct ShardIndex {
    folly::ConcurrentHashMap< BlobRoute, bool > btree_;
    std::atomic< blob_id > shard_offset_{0ull};
};

class FileHomeObject : public HomeObjectImpl {
    std::filesystem::path const file_store_;

    /// Simulates the Shard=>Chunk mapping in IndexSvc
    using index_svc = folly::ConcurrentHashMap< shard_id, std::unique_ptr< ShardIndex > >;
    index_svc index_;
    ///

    /// Helpers
    // ShardManager
    ShardManager::Result< ShardInfo > _create_shard(pg_id, uint64_t size_bytes) override;
    ShardManager::Result< ShardInfo > _seal_shard(shard_id) override;

    // BlobManager
    BlobManager::Result< blob_id > _put_blob(ShardInfo const&, Blob&&) override;
    BlobManager::Result< Blob > _get_blob(ShardInfo const&, blob_id) const override;
    BlobManager::NullResult _del_blob(ShardInfo const&, blob_id) override;
    ///

public:
    FileHomeObject(std::weak_ptr< HomeObjectApplication >&& application, std::filesystem::path const& root) :
            HomeObjectImpl::HomeObjectImpl(std::move(application)), file_store_(root) {}
    ~FileHomeObject() override = default;
};

} // namespace homeobject
