#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< ShardManager > HomeObjectImpl::shard_manager() {
    init_repl_svc();
    return shared_from_this();
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::create_shard(pg_id pg_owner, uint64_t size_bytes) {
    if (0 == size_bytes || max_shard_size() < size_bytes) return folly::makeUnexpected(ShardError::INVALID_ARG);

    return _defer().thenValue([this, pg_owner, size_bytes](auto) mutable -> ShardManager::Result< ShardInfo > {
        return _create_shard(pg_owner, size_bytes);
    });
}

ShardManager::AsyncResult< InfoList > HomeObjectImpl::list_shards(pg_id pg) const {
    return _defer().thenValue([this, pg](auto) mutable -> ShardManager::Result< InfoList > {
        auto lg = std::shared_lock(_pg_lock);
        auto pg_it = _pg_map.find(pg);
        if (_pg_map.end() == pg_it) return folly::makeUnexpected(ShardError::UNKNOWN_PG);

        auto info_l = std::list< ShardInfo >();
        for (auto const& shard : pg_it->second.shards) {
            LOGDEBUG("Listing Shard {}", shard.info.id);
            info_l.push_back(shard.info);
        }
        return info_l;
    });
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::seal_shard(shard_id id) {
    return _defer().thenValue(
        [this, id](auto) mutable -> ShardManager::Result< ShardInfo > { return _seal_shard(id); });
}


ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::get_shard(shard_id id) const {
    return _get_shard(id).thenValue([this](auto const e) mutable -> ShardManager::Result< ShardInfo > {
            if (!e) { return folly::makeUnexpected(e.error());}
            return e.value().info;
      });
}

///
// This is used as a first call for many operations and initializes the Future.
//
folly::Future< ShardManager::Result< Shard > > HomeObjectImpl::_get_shard(shard_id id) const {
    return _defer().thenValue([this, id](auto) -> ShardManager::Result< Shard > {
        auto lg = std::shared_lock(_shard_lock);
        if (auto it = _shard_map.find(id); _shard_map.end() != it) return *(it->second);
        return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
    });
}

uint64_t HomeObjectImpl::get_current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast< std::chrono::milliseconds >(now.time_since_epoch());
    auto timestamp = static_cast< uint64_t >(duration.count());
    return timestamp;
}

} // namespace homeobject
