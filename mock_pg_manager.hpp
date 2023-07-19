#pragma once
#include <functional>
#include <set>

#include <boost/uuid/uuid.hpp>

namespace homeobject {

using blob_id = uint64_t;
using peer_id = boost::uuids::uuid;
using pg_id = uint16_t;
using shard_id = uint64_t;

enum class PGError {
    OK = 0,
    TIMEOUT,
    UNKNOWN_PG,
    UNKNOWN_PEER,
};

struct PGMember {
    PGMember(peer_id _id) : id(_id) {}
    peer_id id;
    std::string name;
    int32_t priority; // <0 (Arbiter), ==0 (Follower), >0 (F|Leader)
};

struct PGInfo {
    PGInfo(pg_id _id) : id(_id) {}
    pg_id id;
    mutable std::set< PGMember > members;
};

class PGManager {
public:
    using ok_cb = std::function< void(PGError) >;

    virtual ~PGManager() = default;
    virtual void create_pg(PGInfo const& pg_info, ok_cb cb) = 0;
    virtual void replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member, ok_cb cb) = 0;
};

inline bool operator<(homeobject::PGMember const lhs, homeobject::PGMember const rhs) { return lhs.id < rhs.id; }
inline bool operator<(homeobject::PGInfo const lhs, homeobject::PGInfo const rhs) { return lhs.id < rhs.id; }

} // namespace homeobject
