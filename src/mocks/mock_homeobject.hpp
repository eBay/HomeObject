#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <utility>

#include <home_replication/repl_service.h>

#include "lib/homeobject_impl.hpp"

namespace homeobject {
struct BlobRoute {
    shard_id shard;
    blob_id blob;
};

inline std::string toString(BlobRoute const& r) { return fmt::format("{}:{}", r.shard, r.blob); }

inline bool operator<(BlobRoute const& lhs, BlobRoute const& rhs) { return toString(lhs) < toString(rhs); }

inline bool operator==(BlobRoute const& lhs, BlobRoute const& rhs) { return toString(lhs) == toString(rhs); }
} // namespace homeobject

template <>
struct std::hash< homeobject::BlobRoute > {
    std::size_t operator()(homeobject::BlobRoute const& r) const noexcept {
        return std::hash< std::string >()(homeobject::toString(r));
    }
};

namespace homeobject {

class MockHomeObject : public HomeObjectImpl {
    /// This simulates the IndexSvc thats used within real HomeObject
    mutable std::mutex _shard_lock;
    std::map< shard_id, ShardInfo > _shards;
    shard_id _cur_shard_id{0};
    ///

    /// Simulates the Shard=>Chunk mapping in IndexSvc *and* DataSvc BlkId=>Data
    mutable std::mutex _data_lock;
    std::unordered_map< BlobRoute, Blob > _in_memory_disk;
    blob_id _cur_blob_id{0};
    ///

    std::mutex _repl_lock;
    std::shared_ptr< home_replication::ReplicationService > _repl_svc;

public:
    using HomeObjectImpl::HomeObjectImpl;
    ~MockHomeObject() override = default;

    // BlobManager
    folly::SemiFuture< std::variant< blob_id, BlobError > > put(shard_id shard, Blob&&) override;
    folly::SemiFuture< std::variant< Blob, BlobError > > get(shard_id shard, blob_id const& blob, uint64_t off,
                                                             uint64_t len) const override;
    folly::SemiFuture< BlobError > del(shard_id shard, blob_id const& blob) override;

    // ShardManager
    folly::SemiFuture< ShardManager::info_var > create_shard(pg_id pg_owner, uint64_t size_bytes) override;
    ShardManager::info_var get_shard(shard_id id) const override;
    folly::SemiFuture< ShardManager::list_var > list_shards(pg_id id) const override;
    folly::SemiFuture< ShardManager::info_var > seal_shard(shard_id id) override;
};

} // namespace homeobject
