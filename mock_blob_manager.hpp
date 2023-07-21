#pragma once
#include <functional>
#include <optional>
#include <string>
#include <variant>

#include <sisl/fds/buffer.hpp>

#include <boost/uuid/uuid.hpp>

namespace homeobject {

using blob_id = uint64_t;
using peer_id = boost::uuids::uuid;
using pg_id = uint16_t;
using shard_id = uint64_t;

enum class BlobError { OK = 0, TIMEOUT, INVALID_ARG, NOT_LEADER, UNKNOWN_SHARD, UNKNOWN_BLOB };

struct Blob {
    sisl::io_blob body;
    std::string user_key;
    uint64_t object_off;
};

class BlobManager {
public:
    using get_cb = std::function< void(std::variant< Blob, BlobError > const&, std::optional< peer_id >) >;
    using id_cb = std::function< void(std::variant< blob_id, BlobError > const&, std::optional< peer_id >) >;
    using ok_cb = std::function< void(BlobError const, std::optional< peer_id >) >;

    virtual ~BlobManager() = default;
    virtual void put(shard_id shard, Blob const&, id_cb cb) = 0;
    virtual void get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len, get_cb) const = 0;
    virtual void del(shard_id shard, blob_id const& blob, ok_cb cb) = 0;
};

} // namespace homeobject
