#pragma once
#include <compare>
#include <set>
#include <string>

#include <sisl/utility/enum.hpp>

#include "common.hpp"

namespace homeobject {

ENUM(PGError, uint16_t, UNKNOWN = 1, INVALID_ARG, TIMEOUT, UNKNOWN_PG, UNKNOWN_PEER);

struct PGMember {
    explicit PGMember(peer_id _id) : id(_id) {}
    PGMember(peer_id _id, std::string const& _name) : id(_id), name(_name) {}
    PGMember(peer_id _id, std::string const& _name, int32_t _priority) : id(_id), name(_name), priority(_priority) {}
    peer_id id;
    std::string name;
    int32_t priority{0}; // <0 (Arbiter), ==0 (Follower), >0 (F|Leader)

    auto operator<=>(PGMember const& rhs) const {
        return boost::uuids::hash_value(id) <=> boost::uuids::hash_value(rhs.id);
    }
};

using MemberSet = std::set< PGMember >;

struct PGInfo {
    explicit PGInfo(pg_id _id) : id(_id) {}
    pg_id id;
    mutable MemberSet members;

    auto operator<=>(PGInfo const& rhs) const { return id <=> rhs.id; }
};

class PGManager : public Manager< PGError > {
public:
    virtual NullAsyncResult create_pg(PGInfo&& pg_info) = 0;
    virtual NullAsyncResult replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member) = 0;
};

} // namespace homeobject
