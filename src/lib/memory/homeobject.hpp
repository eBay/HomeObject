#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <set>
#include <utility>

#include "mocks/repl_service.h"

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

using btree = std::unordered_map< BlobRoute, Blob >;

struct ShardIndex {
    mutable std::shared_mutex _btree_lock;
    btree _btree;
    blob_id _shard_seq_num{0ull};
};

class MemoryHomeObject : public HomeObjectImpl {
    /// Simulates the Shard=>Chunk mapping in IndexSvc
    mutable std::shared_mutex _index_lock;
    std::unordered_map< shard_id, ShardIndex > _in_memory_index;
    std::list< Blob > _garbage;
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
    using HomeObjectImpl::HomeObjectImpl;
    ~MemoryHomeObject() override = default;
};

} // namespace homeobject
