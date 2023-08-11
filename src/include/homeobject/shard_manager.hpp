#pragma once
#include <functional>
#include <optional>
#include <variant>

#include <sisl/utility/enum.hpp>

#include "common.hpp"

namespace homeobject {

ENUM(ShardError, uint16_t, OK = 0, UNKNOWN, TIMEOUT, INVALID_ARG, NOT_LEADER, UNKNOWN_PG, UNKNOWN_SHARD);

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
    uint64_t available_capacity_bytes;
    uint64_t total_capacity_bytes;
    uint64_t deleted_capacity_bytes;
    std::optional< peer_id > current_leader;
};

class ShardManager {
public:
    // std::optional<peer_id> returned in case follower received request.
    using info_var = std::variant< ShardInfo, ShardError >;
    using list_var = std::variant< std::vector< ShardInfo >, ShardError >;

    static uint64_t max_shard_size(); // Static function forces runtime evaluation.

    virtual ~ShardManager() = default;

    // Sync
    virtual info_var get_shard(shard_id id) const = 0;

    // Async
    virtual folly::Future< info_var > create_shard(pg_id pg_owner, uint64_t size_bytes) = 0;
    virtual folly::Future< list_var > list_shards(pg_id id) const = 0;
    virtual folly::Future< info_var > seal_shard(shard_id id) = 0;
};

} // namespace homeobject
