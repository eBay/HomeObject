#include "homeobject.hpp"
#include "replication_message.hpp"

#include <boost/uuid/uuid_generators.hpp>

#include <homestore/homestore.hpp>
#include <homestore/blkdata_service.hpp>
#include <homestore/meta_service.hpp>
#include <homestore/replication_service.hpp>

namespace homeobject {

PGManager::NullAsyncResult HSHomeObject::_create_pg(PGInfo&& pg_info, std::set< std::string, std::less<> >&& peers) {
    auto repl_dev_uuid = boost::uuids::random_generator()();
    using namespace homestore;
    ReplicationService* replication_service = (ReplicationService*)(&HomeStore::instance()->repl_service());
    return replication_service->create_replica_dev(repl_dev_uuid, std::move(peers))
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, pg_info = std::move(pg_info), uuid = std::move(repl_dev_uuid)](
                       homestore::Result< shared< ReplDev >, ReplServiceError > repl_dev) -> PGManager::NullResult {
            if (!(repl_dev)) { return folly::makeUnexpected(PGError::INVALID_ARG); }

            auto pg = PG(std::move(pg_info));
            pg.repl_dev_uuid = uuid;
            auto lg = std::scoped_lock(_pg_lock);
            auto [it, _] = _pg_map.try_emplace(pg_info.id, std::move(pg));
            RELEASE_ASSERT(_pg_map.end() != it, "Unknown map insert error!");
            return folly::Unit();
        });
}

} // namespace homeobject
