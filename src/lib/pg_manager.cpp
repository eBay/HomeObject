#include "homeobject_impl.hpp"

#include <home_replication/repl_service.h>
#include <boost/uuid/uuid_io.hpp>

using home_replication::ReplServiceError;

namespace homeobject {

PGError toPgError(ReplServiceError const& e) {
    switch (e) {
    case ReplServiceError::BAD_REQUEST:
        [[fallthrough]];
    case ReplServiceError::CANCELLED:
        [[fallthrough]];
    case ReplServiceError::CONFIG_CHANGING:
        [[fallthrough]];
    case ReplServiceError::SERVER_ALREADY_EXISTS:
        [[fallthrough]];
    case ReplServiceError::SERVER_IS_JOINING:
        [[fallthrough]];
    case ReplServiceError::SERVER_IS_LEAVING:
        [[fallthrough]];
    case ReplServiceError::RESULT_NOT_EXIST_YET:
        [[fallthrough]];
    case ReplServiceError::NOT_LEADER:
        [[fallthrough]];
    case ReplServiceError::TERM_MISMATCH:
        return PGError::INVALID_ARG;
    case ReplServiceError::CANNOT_REMOVE_LEADER:
        return PGError::UNKNOWN_PEER;
    case ReplServiceError::TIMEOUT:
        return PGError::TIMEOUT;
    case ReplServiceError::SERVER_NOT_FOUND:
        return PGError::UNKNOWN_PG;
    case ReplServiceError::OK:
        DEBUG_ASSERT(false, "Should not process OK!");
        [[fallthrough]];
    case ReplServiceError::FAILED:
        return PGError::UNKNOWN;
    }
    return PGError::UNKNOWN;
}

std::shared_ptr< PGManager > HomeObjectImpl::pg_manager() {
    init_repl_svc();
    return shared_from_this();
}

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

    return _repl_svc->create_replica_set(fmt::format("{}", pg_info.id), std::move(peers))
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, pg_info = std::move(pg_info)](
                       home_replication::ReplicationService::set_var const& v) -> PGManager::NullResult {
            if (std::holds_alternative< home_replication::ReplServiceError >(v))
                return folly::makeUnexpected(PGError::INVALID_ARG);

            auto lg = std::scoped_lock(_pg_lock);
            auto [it, _] = _pg_map.try_emplace(pg_info.id, std::unordered_set< shard_id >());
            RELEASE_ASSERT(_pg_map.end() != it, "Unknown map insert error!");
            return folly::Unit();
        });
}

PGManager::NullAsyncResult HomeObjectImpl::replace_member(pg_id id, peer_id const& old_member,
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

    return _repl_svc->replace_member(fmt::format("{}", id), to_string(old_member), to_string(new_member.id))
        .deferValue([](ReplServiceError const& e) -> PGManager::NullResult {
            if (ReplServiceError::OK != e) return folly::makeUnexpected(toPgError(e));
            return folly::Unit();
        });
}

} // namespace homeobject
