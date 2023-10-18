#include <compare>
#include <functional>

#include <boost/functional/hash.hpp>

#include "homeobject/common.hpp"

namespace homeobject {

///
// A Key used in the IndexService (BTree). The inclusion of Shard allows BlobRoutes
// to appear in a different Index should the Blob (Shard) be moved between Pgs.
#pragma pack(1)
struct BlobRoute {
    shard_id_t shard;
    blob_id_t blob;
    auto operator<=>(BlobRoute const&) const = default;
    sisl::blob to_blob() const { return sisl::blob{uintptr_cast(const_cast< BlobRoute* >(this)), sizeof(*this)}; }
};
#pragma pack()

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
        return boost::hash_value< homeobject::blob_id_t >(std::make_pair(r.shard, r.blob));
    }
};
