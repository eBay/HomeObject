#pragma once

#include <atomic>
#include <utility>

#include <folly/concurrency/ConcurrentHashMap.h>
#include "mocks/repl_service.h"

#include "lib/homeobject_impl.hpp"

namespace homeobject {
struct BlobRoute {
    shard_id shard;
    blob_id blob;
};
} // namespace homeobject

namespace fmt {
template <>
struct formatter< homeobject::BlobRoute > {
    template < typename ParseContext >
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template < typename FormatContext >
    auto format(const homeobject::BlobRoute& r, FormatContext& ctx) {
        return format_to(ctx.out(), "{}:{:012x}", r.shard, r.blob);
    }
};
} // namespace fmt

template <>
struct std::hash< homeobject::BlobRoute > {
    std::size_t operator()(homeobject::BlobRoute const& r) const noexcept {
        return std::hash< std::string >()(fmt::format("{}", r));
    }
};

namespace homeobject {

inline bool operator<(BlobRoute const& lhs, BlobRoute const& rhs) {
    return fmt::format("{}", lhs) < fmt::format("{}", rhs);
}
inline bool operator==(BlobRoute const& lhs, BlobRoute const& rhs) {
    return fmt::format("{}", lhs) == fmt::format("{}", rhs);
}

///
// Used to TombStone Blob's in the Index to defer for GC.
ENUM(BlobState, uint8_t, ALIVE = 0, DELETED);

struct BlobExt : public Blob {
    BlobState _state{BlobState::DELETED};
    Blob* _blob;

    explicit operator bool() const { return _state == BlobState::ALIVE; }
};
inline bool operator==(BlobExt const& lhs, BlobExt const& rhs) { return lhs._blob == rhs._blob; }

using btree = folly::ConcurrentHashMap< BlobRoute, BlobExt >;

struct ShardIndex {
    btree _btree;
    std::atomic< blob_id > _shard_seq_num{0ull};
    ~ShardIndex();
};

class MemoryHomeObject : public HomeObjectImpl {
    /// Simulates the Shard=>Chunk mapping in IndexSvc
    using index_svc = folly::ConcurrentHashMap< shard_id, std::unique_ptr< ShardIndex > >;
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

    ShardIndex& _find_index(shard_id) const;

public:
    using HomeObjectImpl::HomeObjectImpl;
    ~MemoryHomeObject() override = default;
};

} // namespace homeobject
