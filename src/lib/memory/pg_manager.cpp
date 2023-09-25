#include <chrono>

#include "homeobject.hpp"

namespace homeobject {

PGManager::NullAsyncResult MemoryHomeObject::_create_pg(PGInfo&& pg_info,
                                                        std::set< std::string, std::less<> >&& peers) {
    return _repl_svc->create_replica_set(fmt::format("{}", pg_info.id), std::move(peers))
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, pg_info = std::move(pg_info)](
                       home_replication::ReplicationService::set_var const& v) -> PGManager::NullResult {
            if (std::holds_alternative< home_replication::ReplServiceError >(v))
                return folly::makeUnexpected(PGError::INVALID_ARG);
            auto pg = PG(std::move(pg_info));
            auto lg = std::scoped_lock(_pg_lock);
            auto [it, _] = _pg_map.try_emplace(pg_info.id, std::move(pg));
            RELEASE_ASSERT(_pg_map.end() != it, "Unknown map insert error!");
            return folly::Unit();
        });
}

} // namespace homeobject
