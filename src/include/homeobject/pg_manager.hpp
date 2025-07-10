#pragma once
#include <compare>
#include <set>
#include <string>

#include <boost/uuid/uuid_io.hpp>
#include <sisl/utility/enum.hpp>
#include <sisl/logging/logging.h>
#include "common.hpp"

namespace homeobject {

ENUM(PGError, uint16_t, UNKNOWN = 1, INVALID_ARG, TIMEOUT, UNKNOWN_PG, NOT_LEADER, UNKNOWN_PEER, UNSUPPORTED_OP,
     CRC_MISMATCH, NO_SPACE_LEFT, DRIVE_WRITE_ERROR, RETRY_REQUEST, SHUTTING_DOWN, ROLL_BACK);
ENUM(PGReplaceMemberTaskStatus, uint16_t, COMPLETED = 0, IN_PROGRESS, NOT_LEADER, TASK_ID_MISMATCH, TASK_NOT_FOUND, UNKNOWN);
// https://github.corp.ebay.com/SDS/nuobject_proto/blob/main/src/proto/pg.proto#L52
ENUM(PGStateMask, uint32_t, HEALTHY = 0, DISK_DOWN = 0x1, SCRUBBING = 0x2, BASELINE_RESYNC = 0x4, INCONSISTENT = 0x8,
     REPAIR = 0x10, GC_IN_PROGRESS = 0x20, RESYNCING = 0x40);

struct PGMember {
    // Max length is based on homestore::replica_member_info::max_name_len - 1. Last byte is null terminated.
    static constexpr uint64_t max_name_len = 127;
    explicit PGMember(peer_id_t _id) : id(_id) {}
    PGMember(peer_id_t _id, std::string const& _name) : id(_id), name(_name) {
        RELEASE_ASSERT(name.size() <= max_name_len, "Name exceeds max length");
    }
    PGMember(peer_id_t _id, std::string const& _name, int32_t _priority) : id(_id), name(_name), priority(_priority) {
        RELEASE_ASSERT(name.size() <= max_name_len, "Name exceeds max length");
    }
    peer_id_t id;
    std::string name;
    int32_t priority{0}; // <0 (Arbiter), ==0 (Follower), >0 (F|Leader)

    auto operator<=>(PGMember const& rhs) const {
        return boost::uuids::hash_value(id) <=> boost::uuids::hash_value(rhs.id);
    }
    auto operator==(PGMember const& rhs) const { return id == rhs.id; }
};

using MemberSet = std::set< PGMember >;

struct PGInfo {
    explicit PGInfo(pg_id_t _id) : id(_id) {}
    pg_id_t id;
    mutable MemberSet members;
    peer_id_t replica_set_uuid;
    uint64_t size;
    uint64_t chunk_size;
    // The expected member count, this is a fixed value decided by pg creation.
    uint32_t expected_member_num = 0;

    auto operator<=>(PGInfo const& rhs) const { return id <=> rhs.id; }
    auto operator==(PGInfo const& rhs) const { return id == rhs.id; }

    // check if the PGInfo has same id, size and members with the rhs PGInfo.
    bool is_equivalent_to(PGInfo const& rhs) const {
        if (id != rhs.id || size != rhs.size || members.size() != rhs.members.size()) {
            return false;
        }
        for (auto const& m : members) {
            auto it = rhs.members.find(m);
            if (it == rhs.members.end() || it->priority != m.priority) {
                return false;
            }
        }
        return true;
    }

    std::string to_string() const {
        std::string members_str;
        uint32_t i = 0ul;
        for (auto const& m : members) {
            if (i++ > 0) { members_str += ", "; }
            members_str += fmt::format("member-{}: id={}, name={}, priority={}",
                                       i, boost::uuids::to_string(m.id), m.name, m.priority);
        }
        return fmt::format("PGInfo: id={}, replica_set_uuid={}, size={}, chunk_size={}, "
                           "expected_member_num={}, members={}",
                           id, boost::uuids::to_string(replica_set_uuid), size, chunk_size,
                           expected_member_num, members_str);
    }
};

struct peer_info {
    peer_id_t id;
    std::string name;
    uint64_t last_commit_lsn{0}; // last commit lsn from this peer
    uint64_t last_succ_resp_us{0};
    bool can_vote = true;
};

struct pg_state {
    std::atomic<uint64_t> state{0};

    explicit pg_state(uint64_t s) : state{s} {}

    void set_state(PGStateMask mask) {
        state.fetch_or(static_cast<uint64_t>(mask), std::memory_order_relaxed);
    }

    void clear_state(PGStateMask mask) {
        state.fetch_and(~static_cast<uint64_t>(mask), std::memory_order_relaxed);
    }

    bool is_state_set(PGStateMask mask) const {
        return (state.load(std::memory_order_relaxed) & static_cast<uint64_t>(mask)) != 0;
    }

    uint64_t get() const {
        return state.load(std::memory_order_relaxed);
    }
};

struct PGStats {
    pg_id_t id;
    peer_id_t replica_set_uuid;
    peer_id_t leader_id;            // the leader of this PG from my perspective;
    uint32_t num_members;           // number of members in this PG;
    uint32_t total_shards;          // shards allocated on this PG (including open shards)
    uint32_t open_shards;           // active shards on this PG;
    uint32_t avail_open_shards;     // total number of shards that could be opened on this PG;
    uint64_t used_bytes;            // total number of bytes used by all shards on this PG;
    uint64_t avail_bytes;           // total number of bytes available on this PG;
    uint64_t num_active_objects;    // total number of active objects on this PG;
    uint64_t num_tombstone_objects; // total number of tombstone objects on this PG;
    uint64_t pg_state;              // PG state;
    uint32_t snp_progress;          // snapshot progress, the value is set only when the peer is under baseline resync.
    std::vector< peer_info > members;

    PGStats() :
            id{0},
            replica_set_uuid{},
            leader_id{},
            num_members{0},
            total_shards{0},
            open_shards{0},
            avail_open_shards{0},
            used_bytes{0},
            avail_bytes{0},
            num_active_objects{0},
            num_tombstone_objects{0},
            pg_state{0},
            snp_progress{0},
            members{} {}

    std::string to_string() {
        std::string members_str;
        uint32_t i = 0ul;
        for (auto const& m : members) {
            if (i++ > 0) { members_str += ", "; };
            members_str +=
                fmt::format("member-{}: id={}, name={}, last_commit_lsn={}, last_succ_resp_us={}, can_vote={}", i,
                            boost::uuids::to_string(m.id), m.name, m.last_commit_lsn, m.last_succ_resp_us, m.can_vote);
        }

        return fmt::format(
            "PGStats: id={}, replica_set_uuid={}, leader={}, num_members={}, total_shards={}, open_shards={}, "
            "avail_open_shards={}, used_bytes={}, avail_bytes={}, members={}",
            id, boost::uuids::to_string(replica_set_uuid), boost::uuids::to_string(leader_id), num_members,
            total_shards, open_shards, avail_open_shards, used_bytes, avail_bytes, members_str);
    }
};

struct PGReplaceMemberStatus {
    std::string task_id;
    PGReplaceMemberTaskStatus status = PGReplaceMemberTaskStatus::UNKNOWN;
    std::vector< peer_info > members;
};

class PGManager : public Manager< PGError > {
public:
    virtual NullAsyncResult create_pg(PGInfo&& pg_info, trace_id_t tid = 0) = 0;
    virtual NullAsyncResult replace_member(pg_id_t id, std::string& task_id, peer_id_t const& old_member, PGMember const& new_member,
                                           u_int32_t commit_quorum = 0, trace_id_t tid = 0) = 0;
    virtual PGReplaceMemberStatus get_replace_member_status(pg_id_t id, std::string& task_id, const PGMember& old_member,
                                                          const PGMember& new_member,
                                                          const std::vector< PGMember >& others,
                                                          uint64_t trace_id = 0) const = 0;

    /**
     * Retrieves the statistics for a specific PG (Placement Group) identified by its ID.
     *
     * @param id The ID of the PG.
     * @param stats The reference to the PGStats object where the statistics will be stored.
     * @return True if the statistics were successfully retrieved, false otherwise (e.g. id not found).
     */
    virtual bool get_stats(pg_id_t id, PGStats& stats) const = 0;

    /**
     * @brief Retrieves the list of pg_ids.
     *
     * This function retrieves the list of pg_ids and stores them in the provided vector.
     *
     * @param pg_ids The vector to store the pg_ids.
     */
    virtual void get_pg_ids(std::vector< pg_id_t >& pg_ids) const = 0;
};

} // namespace homeobject
