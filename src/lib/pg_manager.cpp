#include <boost/uuid/uuid_io.hpp>

#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< PGManager > HomeObjectImpl::pg_manager() { return shared_from_this(); }

PGManager::NullAsyncResult HomeObjectImpl::create_pg(PGInfo&& pg_info) {
    LOGI("[pg={}] has [{}] members", pg_info.id, pg_info.members.size());
    auto saw_ourself = false;
    auto saw_leader = false;
    auto peers = std::set< peer_id_t >();
    for (auto const& member : pg_info.members) {
        if (member.id == our_uuid()) saw_ourself = true;
        if (member.priority > 0) saw_leader = true;
        peers.insert(member.id);
    }
    if (!saw_ourself || !saw_leader) { return folly::makeUnexpected(PGError::INVALID_ARG); }
    return _create_pg(std::move(pg_info), peers);
}

PGManager::NullAsyncResult HomeObjectImpl::replace_member(pg_id_t id, peer_id_t const& old_member,
                                                          PGMember const& new_member) {
    LOGI("[pg={}] replace member [{}] with [{}]", id, to_string(old_member), to_string(new_member.id));
    if (old_member == new_member.id) {
        LOGW("rejecting identical replacement SvcId [{}]!", to_string(old_member));
        return folly::makeUnexpected(PGError::INVALID_ARG);
    }

    if (old_member == our_uuid()) {
        LOGW("refusing to remove ourself {}!", to_string(old_member));
        return folly::makeUnexpected(PGError::INVALID_ARG);
    }

    return _replace_member(id, old_member, new_member);
}

bool HomeObjectImpl::get_stats(pg_id_t id, PGStats& stats) const { return _get_stats(id, stats); }
void HomeObjectImpl::get_pg_ids(std::vector< pg_id_t >& pg_ids) const { return _get_pg_ids(pg_ids); }
} // namespace homeobject
