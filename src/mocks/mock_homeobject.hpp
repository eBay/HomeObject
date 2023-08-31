#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
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
    /// This simulates the MetaSvc thats used within real HomeObject
    mutable std::shared_mutex _shard_lock;
    std::map< shard_id, ShardInfo > _shards;
    shard_id _cur_shard_id{0};
    ///

    /// Simulates the Shard=>Chunk mapping in IndexSvc *and* DataSvc BlkId=>Data
    mutable std::mutex _data_lock;
    std::list< Blob > _in_memory_disk;
    using pba = decltype(_in_memory_disk)::const_iterator;

    mutable std::shared_mutex _index_lock;
    std::unordered_map< BlobRoute, pba > _in_memory_index;
    ///

    std::mutex _repl_lock;
    std::shared_ptr< home_replication::ReplicationService > _repl_svc;

    BlobManager::AsyncResult< MockHomeObject::pba > _get_route(shard_id shard, blob_id blob) const;

public:
    using HomeObjectImpl::HomeObjectImpl;
    ~MockHomeObject() override = default;

    // BlobManager
    BlobManager::AsyncResult< blob_id > put(shard_id shard, Blob&&) override;
    BlobManager::AsyncResult< Blob > get(shard_id shard, blob_id const& blob, uint64_t off,
                                         uint64_t len) const override;
    BlobManager::NullAsyncResult del(shard_id shard, blob_id const& blob) override;

    // ShardManager
    ShardManager::AsyncResult< ShardInfo > create_shard(pg_id pg_owner, uint64_t size_bytes) override;
    ShardManager::AsyncResult< ShardInfo > seal_shard(shard_id id) override;
    ShardManager::Result< ShardInfo > get_shard(shard_id id) const override;
    ShardManager::Result< InfoList > list_shards(pg_id id) const override;
};

} // namespace homeobject
