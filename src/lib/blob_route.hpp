#pragma once

#include <compare>
#include <functional>

#include <boost/functional/hash.hpp>

#include "homeobject_impl.hpp"

namespace homeobject {

///
// A Key used in the IndexService (BTree). The inclusion of Shard allows BlobRoutes
// to appear in a different Index should the Blob (Shard) be moved between Pgs.
#pragma pack(1)
struct BlobRoute {
    shard_id_t shard{0};
    blob_id_t blob{0};
    auto operator<=>(BlobRoute const&) const = default;
    sisl::blob to_blob() const { return sisl::blob{uintptr_cast(const_cast< BlobRoute* >(this)), sizeof(*this)}; }
};

// used for gc to quickly identify all the blob in the move_to chunk
struct BlobRouteByChunk {
    // homestore::chunk_num_t == uint16_t
    uint16_t chunk{0};
    shard_id_t shard{0};
    blob_id_t blob{0};
    auto operator<=>(BlobRouteByChunk const&) const = default;
    sisl::blob to_blob() const {
        return sisl::blob{uintptr_cast(const_cast< BlobRouteByChunk* >(this)), sizeof(*this)};
    }
};
#pragma pack()

} // namespace homeobject

namespace fmt {
template <>
struct formatter< homeobject::BlobRouteByChunk > {
    template < typename ParseContext >
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template < typename FormatContext >
    auto format(homeobject::BlobRouteByChunk const& r, FormatContext& ctx) {
        return fmt::v10::format_to(ctx.out(), "{:04x}:{:04x}:{:012x}:{:016x}", r.chunk,
                                   (r.shard >> homeobject::shard_width), (r.shard & homeobject::shard_mask), r.blob);
    }
};

template <>
struct formatter< homeobject::BlobRoute > {
    template < typename ParseContext >
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template < typename FormatContext >
    auto format(homeobject::BlobRoute const& r, FormatContext& ctx) {
        return fmt::v10::format_to(ctx.out(), "{:04x}:{:012x}:{:016x}", (r.shard >> homeobject::shard_width),
                                   (r.shard & homeobject::shard_mask), r.blob);
    }
};

} // namespace fmt

template <>
struct std::hash< homeobject::BlobRoute > {
    std::size_t operator()(homeobject::BlobRoute const& r) const noexcept {
        return boost::hash_value< homeobject::blob_id_t >(std::make_pair(r.shard, r.blob));
    }
};

template <>
struct std::hash< homeobject::BlobRouteByChunk > {
    std::size_t operator()(homeobject::BlobRouteByChunk const& r) const noexcept {
        std::size_t seed = 0;
        boost::hash_combine(seed, r.chunk);
        boost::hash_combine(seed, r.shard);
        boost::hash_combine(seed, r.blob);
        return seed;
    }
};
