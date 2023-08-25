#pragma once
#include <list>
#include <optional>

#include <sisl/utility/enum.hpp>

#include "common.hpp"

namespace homeobject {

ENUM(ShardError, uint16_t, UNKNOWN = 1, TIMEOUT, INVALID_ARG, NOT_LEADER, UNKNOWN_PG, UNKNOWN_SHARD);

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
    std::optional< peer_id > current_leader{std::nullopt};
};
using InfoList = std::list< ShardInfo >;

class ShardManager : public Manager< ShardError > {
public:
    static uint64_t max_shard_size(); // Static function forces runtime evaluation.

    // Sync
    virtual Result< ShardInfo > get_shard(shard_id id) const = 0;
    virtual Result< InfoList > list_shards(pg_id id) const = 0;

    // Async
    virtual AsyncResult< ShardInfo > create_shard(pg_id pg_owner, uint64_t size_bytes) = 0;
    virtual AsyncResult< ShardInfo > seal_shard(shard_id id) = 0;
};

} // namespace homeobject
