#include <boost/uuid/uuid_io.hpp>

#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< PGManager > HomeObjectImpl::pg_manager() { return shared_from_this(); }

PGManager::NullAsyncResult HomeObjectImpl::create_pg(PGInfo&& pg_info) {
    LOGINFO("Creating PG: [{}] of [{}] members", pg_info.id, pg_info.members.size());
    auto saw_ourself = false;
    auto saw_leader = false;
    auto peers = std::set< std::string, std::less<> >();
    for (auto const& member : pg_info.members) {
        if (member.id == our_uuid()) saw_ourself = true;
        if (member.priority > 0) saw_leader = true;
        peers.insert(to_string(member.id));
    }
    if (!saw_ourself || !saw_leader) return folly::makeUnexpected(PGError::INVALID_ARG);

    return _create_pg(std::move(pg_info), std::move(peers));
}

PGManager::NullAsyncResult HomeObjectImpl::replace_member(pg_id_t id, peer_id_t const& old_member,
                                                          PGMember const& new_member) {
    LOGINFO("Replacing PG: [{}] member [{}] with [{}]", id, to_string(old_member), to_string(new_member.id));
    if (old_member == new_member.id) {
        LOGWARN("Rejecting replace_member with identical replacement SvcId [{}]!", to_string(old_member));
        return folly::makeUnexpected(PGError::INVALID_ARG);
    }

    if (old_member == our_uuid()) {
        LOGWARN("Rejecting replace_member removing ourself {}!", to_string(old_member));
        return folly::makeUnexpected(PGError::INVALID_ARG);
    }

    return _replace_member(id, old_member, new_member);
}

PGManager::Result< PG const* > HomeObjectImpl::get_pg(pg_id_t pg) const {
    std::shared_lock lock_guard(_pg_lock);
    auto iter = _pg_map.find(pg);
    if (iter == _pg_map.cend()) { return folly::makeUnexpected(PGError::UNKNOWN_PG); }
    return iter->second.get();
}
} // namespace homeobject
