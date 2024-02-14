#pragma once
#include <compare>
#include <set>
#include <string>

#include <boost/uuid/uuid_io.hpp>
#include <sisl/utility/enum.hpp>

#include "common.hpp"

namespace homeobject {

ENUM(PGError, uint16_t, UNKNOWN = 1, INVALID_ARG, TIMEOUT, UNKNOWN_PG, UNKNOWN_PEER, UNSUPPORTED_OP, CRC_MISMATCH,
     NO_SPACE_LEFT, DRIVE_WRITE_ERROR);

struct PGMember {
    explicit PGMember(peer_id_t _id) : id(_id) {}
    PGMember(peer_id_t _id, std::string const& _name) : id(_id), name(_name) {}
    PGMember(peer_id_t _id, std::string const& _name, int32_t _priority) : id(_id), name(_name), priority(_priority) {}
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

    auto operator<=>(PGInfo const& rhs) const { return id <=> rhs.id; }
    auto operator==(PGInfo const& rhs) const { return id == rhs.id; }
};

struct PGStats {
    pg_id_t id;
    peer_id_t replica_set_uuid;
    uint32_t num_members;       // number of members in this PG;
    uint32_t total_shards;      // shards allocated on this PG (including open shards)
    uint32_t open_shards;       // active shards on this PG;
    uint32_t avail_open_shards; // total number of shards that could be opened on this PG;
    uint64_t used_bytes;        // total number of bytes used by all shards on this PG;
    uint64_t avail_bytes;       // total number of bytes available on this PG;
    std::vector< std::tuple< peer_id_t, std::string, uint64_t /* last_commit_lsn */ > > members;

    std::string to_string() {
        std::string members_str;
        uint32_t i = 0ul;
        for (auto const& m : members) {
            if (i++ > 0) { members_str += ", "; };
            members_str += fmt::format("member-{}: id={}, name={}, last_commit_lsn={}", i,
                                       boost::uuids::to_string(std::get< 0 >(m)), std::get< 1 >(m), std::get< 2 >(m));
        }

        return fmt::format("PGStats: id={}, replica_set_uuid={}, num_members={}, total_shards={}, open_shards={}, "
                           "avail_open_shards={}, used_bytes={}, avail_bytes={}, members: {}",
                           id, boost::uuids::to_string(replica_set_uuid), num_members, total_shards, open_shards,
                           avail_open_shards, used_bytes, avail_bytes, members_str);
    }
};

class PGManager : public Manager< PGError > {
public:
    virtual NullAsyncResult create_pg(PGInfo&& pg_info) = 0;
    virtual NullAsyncResult replace_member(pg_id_t id, peer_id_t const& old_member, PGMember const& new_member) = 0;

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
