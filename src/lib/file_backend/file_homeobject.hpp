#pragma once

#include <atomic>
#include <filesystem>
#include <utility>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "lib/homeobject_impl.hpp"
#include "lib/blob_route.hpp"

namespace homeobject {

struct ShardIndex {
    folly::ConcurrentHashMap< BlobRoute, bool > btree_;
    int fd_{-1};
    std::atomic< blob_id_t > shard_offset_{0ull};
    ~ShardIndex();
};

class FileHomeObject : public HomeObjectImpl {
    std::filesystem::path const file_store_;

    /// Simulates the Shard=>Chunk mapping in IndexSvc
    using index_svc = folly::ConcurrentHashMap< shard_id_t, std::unique_ptr< ShardIndex > >;
    index_svc index_;
    ///

    /// Helpers
    // ShardManager
    ShardManager::AsyncResult< ShardInfo > _create_shard(pg_id_t, uint64_t size_bytes) override;
    ShardManager::Result< ShardInfo > _seal_shard(shard_id_t) override;

    // BlobManager
    BlobManager::AsyncResult< blob_id_t > _put_blob(ShardInfo const&, Blob&&) override;
    BlobManager::AsyncResult< Blob > _get_blob(ShardInfo const&, blob_id_t, uint64_t off = 0,
                                               uint64_t len = 0) const override;
    BlobManager::NullAsyncResult _del_blob(ShardInfo const&, blob_id_t) override;
    ///

    // PGManager
    PGManager::NullAsyncResult _create_pg(PGInfo&& pg_info, std::set< std::string, std::less<> > peers) override;
    PGManager::NullAsyncResult _replace_member(pg_id_t id, peer_id_t const& old_member,
                                               PGMember const& new_member) override;
    ///

    static nlohmann::json _read_blob_header(int shard_fd, blob_id_t& blob_id);
    void _recover();

public:
    FileHomeObject(std::weak_ptr< HomeObjectApplication >&& application, std::filesystem::path const& root);
    ~FileHomeObject() override = default;
};

} // namespace homeobject
