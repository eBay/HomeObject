#include "mock_homeobject.hpp"

#include <boost/uuid/uuid_io.hpp>

namespace homeobject {
void MockHomeObject::create_pg(PGInfo const& pg_info, PGManager::ok_cb cb) {
    LOGINFO("Creating PG: [{}] of [{}] members", pg_info.id, pg_info.members.size());
    auto lg = std::scoped_lock(_pg_lock);
    _pgs.insert(pg_info);
    cb(PGError::OK);
}
void MockHomeObject::replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member,
                                    PGManager::ok_cb cb) {
    LOGINFO("Replacing PG: [{}] member [{}] with [{}]", id, to_string(old_member), to_string(new_member.id));
    auto err = PGError::OK;
    {
        auto lg = std::scoped_lock(_pg_lock);
        auto pg_it = _pgs.end();
        if (pg_it = _pgs.find(id); _pgs.end() == pg_it) {
            err = PGError::UNKNOWN_PG;
        } else if (0 == pg_it->members.erase(old_member)) {
            err = PGError::UNKNOWN_PEER;
        } else {
            auto [it, happened] = pg_it->members.insert(new_member);
            if (!happened) {
                LOGERROR("Already have this member [{}] in [pg={}]", to_string(new_member.id), id);
                err = PGError::BAD_ARGUMENT;
            }
        }
    }
    cb(err);
}
} // namespace homeobject
