#include "mock_homeobject.hpp"
#include "repl_service/repl_service.hpp"

#include <boost/uuid/uuid_io.hpp>

namespace homeobject {

std::shared_ptr< PGManager > MockHomeObject::pg_manager() {
    init_repl_svc();
    return shared_from_this();
}

void MockHomeObject::init_repl_svc() {
    auto lg = std::scoped_lock(_repl_lock);
    if (!_repl_svc) _repl_svc = home_replication::create_repl_service(*this);
}

folly::SemiFuture< PGError > MockHomeObject::create_pg(PGInfo&& pg_info) {
    LOGINFO("Creating PG: [{}] of [{}] members", pg_info.id, pg_info.members.size());
    if (std::none_of(pg_info.members.begin(), pg_info.members.end(),
                     [](PGMember const& m) { return 0 < m.priority; })) {
        LOGERROR("No possible leader for PG: [{}]", pg_info.id);
        return folly::makeSemiFuture(PGError::INVALID_ARG);
    }
    return _repl_svc->create_replica_set(fmt::format("{}", pg_info.id))
        .deferValue([this, pg_info = std::move(pg_info)](home_replication::ReplicationService::set_var const& v) {
            if (std::holds_alternative< home_replication::ReplServiceError >(v)) return PGError::INVALID_ARG;

            auto lg = std::scoped_lock(_pg_lock);
            auto [it, _] = _pg_map.try_emplace(pg_info.id, pg_info, std::unordered_set< shard_id >());
            RELEASE_ASSERT(_pg_map.end() != it, "Unknown map insert error!");
            return PGError::OK;
        });
}

folly::SemiFuture< PGError > MockHomeObject::replace_member(pg_id id, peer_id const& old_member,
                                                            PGMember const& new_member) {
    LOGINFO("Replacing PG: [{}] member [{}] with [{}]", id, to_string(old_member), to_string(new_member.id));
    if (old_member == new_member.id) {
        LOGWARN("Rejecting replace_member with identical replacement SvcId [{}]!", to_string(old_member));
        return folly::makeSemiFuture(PGError::INVALID_ARG);
    }

    auto lg = std::scoped_lock(_pg_lock);
    if (auto pg_it = _pg_map.find(id); _pg_map.end() != pg_it) {
        if (auto& members = pg_it->second.first.members; 0 < members.erase(PGMember{old_member}))
            return folly::makeSemiFuture(members.insert(new_member).second ? PGError::OK : PGError::INVALID_ARG);
        return folly::makeSemiFuture(PGError::UNKNOWN_PEER);
    } else
        return folly::makeSemiFuture(PGError::UNKNOWN_PG);
}

extern std::shared_ptr< HomeObject > init_homeobject(init_params const& params) {
    return std::make_shared< MockHomeObject >(params.lookup);
}

} // namespace homeobject
