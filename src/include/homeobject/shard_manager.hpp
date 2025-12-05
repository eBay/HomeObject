#pragma once
#include <compare>
#include <list>
#include <optional>

#include <sisl/utility/enum.hpp>

#include "common.hpp"

namespace homeobject {

ENUM(ShardErrorCode, uint16_t, UNKNOWN = 1, TIMEOUT, INVALID_ARG, NOT_LEADER, UNSUPPORTED_OP, UNKNOWN_PG, UNKNOWN_SHARD,
     PG_NOT_READY, CRC_MISMATCH, NO_SPACE_LEFT, RETRY_REQUEST, SHUTTING_DOWN);
struct ShardError {
    ShardErrorCode code;
    // set when we are not the current leader of the PG.
    std::optional< peer_id_t > current_leader{std::nullopt};
    ShardError(ShardErrorCode _code) { code = _code; }

    ShardError(ShardErrorCode _code, peer_id_t _leader) {
        code = _code;
        current_leader = _leader;
    }
    ShardErrorCode getCode() const { return code; }
};

struct ShardInfo {
    static constexpr uint64_t meta_length = 1024 + 1;
    enum class State : uint8_t {
        OPEN = 0,
        SEALED = 1,
        DELETED = 2,
    };

    shard_id_t id;
    pg_id_t placement_group;
    State state;
    uint64_t lsn; // created_lsn
    uint64_t created_time;
    uint64_t last_modified_time;
    uint64_t available_capacity_bytes;
    uint64_t total_capacity_bytes;
    std::optional< peer_id_t > current_leader{std::nullopt};
    uint8_t meta[meta_length]{};

    auto operator<=>(ShardInfo const& rhs) const { return id <=> rhs.id; }
    auto operator==(ShardInfo const& rhs) const { return id == rhs.id; }
    bool is_open() const { return state == State::OPEN; }
};

using InfoList = std::list< ShardInfo >;

class ShardManager : public Manager< ShardError > {
public:
    static uint64_t max_shard_size(); // Static function forces runtime evaluation.
    static uint64_t max_shard_num_in_pg();

    virtual AsyncResult< ShardInfo > get_shard(shard_id_t id, trace_id_t tid = 0) const = 0;
    virtual AsyncResult< InfoList > list_shards(pg_id_t id, trace_id_t tid = 0) const = 0;
    virtual AsyncResult< ShardInfo > create_shard(pg_id_t pg_owner, uint64_t size_bytes, std::string meta, trace_id_t tid = 0) = 0;
    virtual AsyncResult< ShardInfo > seal_shard(shard_id_t id, trace_id_t tid = 0) = 0;
};

} // namespace homeobject
