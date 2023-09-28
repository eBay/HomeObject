#pragma once
#include <compare>
#include <set>
#include <string>

#include <sisl/utility/enum.hpp>

#include "common.hpp"

namespace homeobject {

ENUM(PGError, uint16_t, UNKNOWN = 1, INVALID_ARG, TIMEOUT, UNKNOWN_PG, UNKNOWN_PEER, UNSUPPORTED_OP);

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

class PGManager : public Manager< PGError > {
public:
    virtual NullAsyncResult create_pg(PGInfo&& pg_info) = 0;
    virtual NullAsyncResult replace_member(pg_id_t id, peer_id_t const& old_member, PGMember const& new_member) = 0;
};

} // namespace homeobject
