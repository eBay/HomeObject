#pragma once
#include <functional>
#include <set>

#include <sisl/utility/enum.hpp>

#include "common.hpp"

namespace homeobject {

ENUM(PGError, uint16_t, OK = 0, UNKNOWN, INVALID_ARG, TIMEOUT, UNKNOWN_PG, UNKNOWN_PEER);

struct PGMember {
    explicit PGMember(peer_id _id) : id(_id) {}
    PGMember(peer_id _id, std::string const& _name) : id(_id), name(_name) {}
    PGMember(peer_id _id, std::string const& _name, int32_t _priority) : id(_id), name(_name), priority(_priority) {}
    peer_id id;
    std::string name;
    int32_t priority{0}; // <0 (Arbiter), ==0 (Follower), >0 (F|Leader)
};

struct PGInfo {
    explicit PGInfo(pg_id _id) : id(_id) {}
    pg_id id;
    mutable std::set< PGMember > members;
};

class PGManager {
public:
    using ok_cb = std::function< void(PGError) >;

    virtual ~PGManager() = default;
    virtual void create_pg(PGInfo const& pg_info, ok_cb const& cb) = 0;
    virtual void replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member, ok_cb const& cb) = 0;
};

inline bool operator<(homeobject::PGMember const& lhs, homeobject::PGMember const& rhs) { return lhs.id < rhs.id; }
inline bool operator<(homeobject::PGInfo const& lhs, homeobject::PGInfo const& rhs) { return lhs.id < rhs.id; }

} // namespace homeobject
