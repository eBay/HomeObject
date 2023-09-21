#include <functional>

#include "homeobject/common.hpp"

namespace homeobject {

///
// A Key used in the IndexService (BTree). The inclusion of Shard allows BlobRoutes
// to appear in a different Index should the Blob (Shard) be moved between Pgs.
struct BlobRoute {
    shard_id shard;
    blob_id blob;
    auto operator<=>(BlobRoute const&) const = default;
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
    auto format(homeobject::BlobRoute const& r, FormatContext& ctx) {
        return format_to(ctx.out(), "{:04x}:{:012x}:{:016x}", (r.shard >> homeobject::shard_width),
                         (r.shard & homeobject::shard_mask), r.blob);
    }
};
} // namespace fmt

template <>
struct std::hash< homeobject::BlobRoute > {
    std::size_t operator()(homeobject::BlobRoute const& r) const noexcept {
        // This arithmetic lifted from: https://en.cppreference.com/w/cpp/utility/hash
        return std::hash< homeobject::shard_id >{}(r.shard) ^ (std::hash< homeobject::blob_id >{}(r.blob) << 1);
    }
};
