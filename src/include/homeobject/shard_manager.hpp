#pragma once
#include <functional>
#include <optional>
#include <variant>

#include <sisl/utility/enum.hpp>

#include "common.hpp"

namespace homeobject {

ENUM(ShardError, uint16_t, OK = 0, TIMEOUT, INVALID_ARG, NOT_LEADER, UNKNOWN_PG, UNKNOWN_SHARD);

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
    uint32_t available_replica_count_mb;
    uint64_t used_capacity_mb;
    uint64_t deleted_capacity_mb;
};

class ShardManager {
public:
    // std::optional<peer_id> returned in case follower received request.
    using info_cb =
        std::function< void(std::variant< std::vector< ShardInfo >, ShardError > const&, std::optional< peer_id >) >;

    static uint64_t max_shard_size_mb(); // Static function forces runtime evaluation.

    virtual ~ShardManager() = default;
    virtual void create_shard(pg_id pg_owner, uint64_t size_mb, info_cb cb) = 0;
    virtual void get_shard(shard_id id, info_cb cb) const = 0;
    virtual void list_shards(pg_id id, info_cb cb) const = 0;
    virtual void seal_shard(shard_id id, info_cb cb) = 0;
};

} // namespace homeobject
