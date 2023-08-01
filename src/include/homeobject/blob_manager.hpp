#pragma once
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <variant>

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
};

class BlobManager {
public:
    using get_cb = std::function< void(std::variant< Blob, BlobError > const&, std::optional< peer_id >) >;
    using id_cb = std::function< void(std::variant< blob_id, BlobError > const&, std::optional< peer_id >) >;
    using ok_cb = std::function< void(BlobError, std::optional< peer_id >) >;

    virtual ~BlobManager() = default;
    virtual void put(shard_id shard, Blob&&, id_cb const& cb) = 0;
    virtual void get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len, get_cb const& cb) const = 0;
    virtual void del(shard_id shard, blob_id const& blob, ok_cb const& cb) = 0;
};

} // namespace homeobject
