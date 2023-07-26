#include "mock_homeobject.hpp"

#include <boost/uuid/uuid_io.hpp>

namespace homeobject {
void MockHomeObject::create_pg(PGInfo const& pg_info, PGManager::ok_cb cb) {
    LOGINFO("Creating PG: [{}] of [{}] members", pg_info.id, pg_info.members.size());
    auto lg = std::scoped_lock(_pg_lock);
    _pg_map.insert(std::make_pair(pg_info.id, std::make_pair(pg_info, std::unordered_set< shard_id >())));
    if (cb) { cb(PGError::OK); }
}
void MockHomeObject::replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member,
                                    PGManager::ok_cb cb) {
    LOGINFO("Replacing PG: [{}] member [{}] with [{}]", id, to_string(old_member), to_string(new_member.id));
    auto err = PGError::OK;
    {
        auto lg = std::scoped_lock(_pg_lock);
        auto pg_it = _pg_map.end();
        if (pg_it = _pg_map.find(id); _pg_map.end() == pg_it) {
            err = PGError::UNKNOWN_PG;
        } else if (auto& members = pg_it->second.first.members; 0 == members.erase(old_member)) {
            err = PGError::UNKNOWN_PEER;
        } else {
            auto [it, happened] = members.insert(new_member);
            if (!happened) {
                LOGERROR("Already have this member [{}] in [pg={}]", to_string(new_member.id), id);
                err = PGError::BAD_ARGUMENT;
            }
        }
    }
    if (cb) { cb(err); }
}
} // namespace homeobject
