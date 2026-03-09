#pragma once
#include <memory>
#include <optional>
#include <string>

#include <sisl/fds/buffer.hpp>

#include "common.hpp"

namespace homeobject {

ENUM(BlobErrorCode, uint16_t, UNKNOWN = 1, TIMEOUT, INVALID_ARG, UNSUPPORTED_OP, NOT_LEADER, REPLICATION_ERROR,
     UNKNOWN_SHARD, UNKNOWN_BLOB, UNKNOWN_PG, CHECKSUM_MISMATCH, READ_FAILED, INDEX_ERROR, SEALED_SHARD, RETRY_REQUEST,
     SHUTTING_DOWN, ROLL_BACK, NO_SPACE_LEFT);
struct BlobError {
    BlobErrorCode code;
    // set when we are not the current leader of the PG.
    std::optional< peer_id_t > current_leader{std::nullopt};
    BlobError(BlobErrorCode _code) { code = _code; }

    BlobError(BlobErrorCode _code, peer_id_t _leader) {
        code = _code;
        current_leader = _leader;
    }
    BlobErrorCode getCode() const { return code; }
};

struct Blob {
    Blob() = default;
    Blob(sisl::io_blob_safe b, std::string const& u, uint64_t o) : body(std::move(b)), user_key(u), object_off(o) {}
    Blob(sisl::io_blob_safe b, std::string const& u, uint64_t o, peer_id_t l) :
            body(std::move(b)), user_key(u), object_off(o), current_leader(l) {}

    Blob clone() const;

    sisl::io_blob_safe body;
    std::string user_key{};
    uint64_t object_off{};
    std::optional< peer_id_t > current_leader{std::nullopt};
};

class BlobManager : public Manager< BlobError > {
public:
    virtual AsyncResult< blob_id_t > put(shard_id_t shard, Blob&&, trace_id_t tid = 0) = 0;
    virtual AsyncResult< Blob > get(shard_id_t shard, blob_id_t const& blob, uint64_t off = 0, uint64_t len = 0,
                                    bool allow_skip_verify = false, trace_id_t tid = 0) const = 0;
    virtual NullAsyncResult del(shard_id_t shard, blob_id_t const& blob, trace_id_t tid = 0) = 0;
};

} // namespace homeobject

namespace fmt {
template <>
struct formatter< homeobject::BlobError > {
    template < typename ParseContext >
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }
    template < typename FormatContext >
    auto format(homeobject::BlobError const& err, FormatContext& ctx) {
        if (err.current_leader.has_value()) {
            return fmt::format_to(ctx.out(), "Code={}, Leader={}", err.code, err.current_leader.value());
        } else {
            return fmt::format_to(ctx.out(), "Code={}", err.code);
        }
    }
};
template <>
struct formatter< boost::uuids::uuid > : ostream_formatter {};
} // namespace fmt