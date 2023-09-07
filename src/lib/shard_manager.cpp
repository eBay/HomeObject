#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< ShardManager > HomeObjectImpl::shard_manager() {
    init_repl_svc();
    return shared_from_this();
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::create_shard(pg_id pg_owner, uint64_t size_bytes) {
    if (0 == size_bytes || max_shard_size() < size_bytes) return folly::makeUnexpected(ShardError::INVALID_ARG);

    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, pg_owner, size_bytes](auto) mutable -> ShardManager::Result< ShardInfo > {
            return _create_shard(pg_owner, size_bytes);
        });
}

ShardManager::AsyncResult< InfoList > HomeObjectImpl::list_shards(pg_id pg) const {
    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, pg](auto) mutable -> ShardManager::Result< InfoList > {
            auto lg = std::shared_lock(_pg_lock);
            auto pg_it = _pg_map.find(pg);
            if (_pg_map.end() == pg_it) return folly::makeUnexpected(ShardError::UNKNOWN_PG);

            auto info_l = std::list< ShardInfo >();
            for (auto const& info : pg_it->second.second) {
                LOGDEBUG("Listing Shard {}", info.id);
                info_l.push_back(info);
            }
            return info_l;
        });
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::seal_shard(shard_id id) {
    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, id](auto) mutable -> ShardManager::Result< ShardInfo > { return _seal_shard(id); });
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::get_shard(shard_id id) const {
    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, id](auto) -> ShardManager::Result< ShardInfo > {
            auto lg = std::shared_lock(_shard_lock);
            if (auto it = _shard_map.find(id); _shard_map.end() != it) return *it->second;
            return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
        });
}

} // namespace homeobject
