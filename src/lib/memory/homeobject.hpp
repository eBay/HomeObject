#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <set>
#include <utility>

#include <folly/synchronization/RWSpinLock.h>
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

///
// Used to TombStone Blob's in the Index to defer for GC.
ENUM(BlobState, uint8_t, ALIVE = 0, DELETED);

struct BlobExt : public Blob {
    std::atomic< BlobState > _state{BlobState::ALIVE};

    explicit operator bool() const { return _state.load(std::memory_order_relaxed) == BlobState::ALIVE; }
};

using btree = std::unordered_map< BlobRoute, BlobExt >;

struct ShardIndex {
    mutable folly::RWSpinLock _btree_lock;
    btree _btree;
    blob_id _shard_seq_num{0ull};
};

class MemoryHomeObject : public HomeObjectImpl {
    /// Simulates the Shard=>Chunk mapping in IndexSvc
    mutable std::shared_mutex _index_lock;
    using index_svc = std::unordered_map< shard_id, ShardIndex >;
    index_svc _in_memory_index;
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

    ShardIndex& _find_index(shard_id);

public:
    using HomeObjectImpl::HomeObjectImpl;
    ~MemoryHomeObject() override = default;
};

} // namespace homeobject
