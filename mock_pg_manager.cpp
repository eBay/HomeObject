#include "mock_homeobject.hpp"

namespace homeobject {
void HomeObject::create_pg(PGInfo const& pg_info, PGManager::ok_cb cb) {
    auto lg = std::scoped_lock(_pg_lock);
    _pgs.insert(pg_info);
    cb(PGError::OK);
}
void HomeObject::replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member, PGManager::ok_cb cb) {
    auto err = PGError::OK;
    {
        auto lg = std::scoped_lock(_pg_lock);
        auto pg_it = _pgs.end();
        if (pg_it = _pgs.find(id); _pgs.end() == pg_it) {
            err = PGError::UNKNOWN_PG;
        } else if (0 == pg_it->members.erase(old_member)) {
            err = PGError::UNKNOWN_PEER;
        } else
            pg_it->members.insert(new_member);
    }
    cb(err);
}
} // namespace homeobject
