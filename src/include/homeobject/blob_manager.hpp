#pragma once
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <variant>

#include <folly/futures/Future.h>
#include <sisl/fds/buffer.hpp>

#include "common.hpp"

namespace homeobject {

ENUM(BlobError, uint16_t, OK = 0, UNKNOWN, TIMEOUT, INVALID_ARG, NOT_LEADER, UNKNOWN_SHARD, UNKNOWN_BLOB,
     CHECKSUM_MISMATCH);

using unique_buffer = std::unique_ptr< sisl::byte_array_impl >;
struct Blob {
    unique_buffer body;
    std::string user_key;
    uint64_t object_off;
    std::optional< peer_id > current_leader{std::nullopt};
};

class BlobManager {
public:
    virtual ~BlobManager() = default;

    virtual folly::Future< std::variant< blob_id, BlobError > > put(shard_id shard, Blob&&) = 0;
    virtual folly::Future< std::variant< Blob, BlobError > > get(shard_id shard, blob_id const& blob, uint64_t off,
                                                                 uint64_t len) const = 0;
    virtual folly::Future< BlobError > del(shard_id shard, blob_id const& blob) = 0;
};

} // namespace homeobject
