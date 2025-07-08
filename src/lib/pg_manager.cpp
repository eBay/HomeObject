#include <boost/uuid/uuid_io.hpp>

#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< PGManager > HomeObjectImpl::pg_manager() { return shared_from_this(); }

PGManager::NullAsyncResult HomeObjectImpl::create_pg(PGInfo&& pg_info, trace_id_t tid) {
    LOGI("[pg={}] has [{}] members, trace_id={}", pg_info.id, pg_info.members.size(), tid);
    auto saw_ourself = false;
    auto saw_leader = false;
    auto peers = std::set< peer_id_t >();
    for (auto const& member : pg_info.members) {
        if (member.id == our_uuid()) saw_ourself = true;
        if (member.priority > 0) saw_leader = true;
        peers.insert(member.id);
    }
    if (!saw_ourself || !saw_leader) { return folly::makeUnexpected(PGError::INVALID_ARG); }
    return _create_pg(std::move(pg_info), peers, tid);
}

PGManager::NullAsyncResult HomeObjectImpl::replace_member(pg_id_t id, std::string& task_id, peer_id_t const& old_member,
                                                          PGMember const& new_member, uint32_t commit_quorum,
                                                          trace_id_t tid) {
    LOGI("[pg={}] replace member [{}] with [{}]  task_id [{}] quorum [{}] trace_id [{}]",
         id, to_string(old_member), to_string(new_member.id), task_id, commit_quorum, tid);
    if (old_member == new_member.id) {
        LOGW("rejecting identical replacement SvcId [{}]! task_id [{}] trace_id [{}]", to_string(old_member), task_id, tid);
        return folly::makeUnexpected(PGError::INVALID_ARG);
    }

    return _replace_member(id, task_id, old_member, new_member, commit_quorum, tid);
}

PGReplaceMemberStatus HomeObjectImpl::get_replace_member_status(pg_id_t id, std::string& task_id, const PGMember& old_member,
                                                              const PGMember& new_member,
                                                              const std::vector< PGMember >& others,
                                                              uint64_t trace_id) const {
    return _get_replace_member_status(id, task_id, old_member, new_member, others, trace_id);
}

bool HomeObjectImpl::get_stats(pg_id_t id, PGStats& stats) const { return _get_stats(id, stats); }
void HomeObjectImpl::get_pg_ids(std::vector< pg_id_t >& pg_ids) const { return _get_pg_ids(pg_ids); }
void HomeObjectImpl::destroy_pg(pg_id_t pg_id) { return _destroy_pg(pg_id); }
} // namespace homeobject
