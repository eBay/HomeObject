#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< PGManager > HomeObjectImpl::pg_manager() { return shared_from_this(); }

void HomeObjectImpl::create_pg(PGInfo const& pg_info, PGManager::ok_cb const& cb) { cb(PGError::TIMEOUT); }

void HomeObjectImpl::replace_member(pg_id id, peer_id const& old_member, PGMember const& new_member,
                                    PGManager::ok_cb const& cb) {
    cb(PGError::UNKNOWN_PG);
}

} // namespace homeobject
