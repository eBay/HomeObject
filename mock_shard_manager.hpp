#pragma once
#include <functional>
#include <optional>
#include <variant>

#include <boost/uuid/uuid.hpp>

namespace homeobject {

using blob_id = uint64_t;
using peer_id = boost::uuids::uuid;
using pg_id = uint16_t;
using shard_id = uint64_t;

enum class ShardError {
    OK = 0,
    TIMEOUT,
    NOT_LEADER,
    INVALID_ARG,
    UNKNOWN_PG,
    UNKNOWN_SHARD,
    UNKNOWN,
};

struct ShardInfo {
    enum class State {
        OPEN = 0,
        SEALED,
        DELETED,
    };

    shard_id id;
    pg_id placement_group;
    State state;
    uint64_t created_time;
    uint64_t last_modified_time;
    uint64_t total_capacity_bytes;
    uint64_t available_capacity_bytes;
    uint64_t deleted_capacity_bytes;
};

class ShardManager {
public:
    // std::optional<peer_id> returned in case follower received request.
    using list_cb =
        std::function< void(std::variant< std::vector< ShardInfo >, ShardError > const&, std::optional< peer_id >) >;
    using id_cb = std::function< void(std::variant< shard_id, ShardError > const&, std::optional< peer_id >) >;
    using info_cb = std::function< void(std::variant< ShardInfo, ShardError > const&, std::optional< peer_id >) >;
    using ok_cb = std::function< void(ShardError, std::optional< peer_id >) >;

    static uint64_t max_shard_size_mb(); // Static function forces runtime evaluation.

    virtual ~ShardManager() = default;
    virtual void create_shard(pg_id pg_owner, uint64_t size_bytes, id_cb cb) = 0;
    virtual void get_shard(shard_id id, info_cb cb) const = 0;
    virtual void list_shards(pg_id id, list_cb cb) const = 0;
    virtual void seal_shard(shard_id id, info_cb cb) = 0;
};

} // namespace homeobject
