#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< ShardManager > HomeObjectImpl::shard_manager() { return shared_from_this(); }

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::create_shard(pg_id_t pg_owner, uint64_t size_bytes, std::string meta,
                                                                    trace_id_t tid) {
    if (0 == size_bytes || max_shard_size() < size_bytes) return folly::makeUnexpected(ShardError(ShardErrorCode::INVALID_ARG));
    return _defer().thenValue([this, pg_owner, size_bytes, meta, tid](auto) mutable -> ShardManager::AsyncResult< ShardInfo > {
        return _create_shard(pg_owner, size_bytes, meta, tid);
    });
}

ShardManager::AsyncResult< InfoList > HomeObjectImpl::list_shards(pg_id_t pgid, trace_id_t tid) const {
    return _defer().thenValue([this, pgid, tid](auto) mutable -> ShardManager::Result< InfoList > {
        std::shared_lock lock_guard(_pg_lock);
        auto iter = _pg_map.find(pgid);
        if (iter == _pg_map.cend()) { return folly::makeUnexpected(ShardError(ShardErrorCode::UNKNOWN_PG)); }
        auto& pg = iter->second;

        auto info_l = std::list< ShardInfo >();
        for (auto const& shard : pg->shards_) {
            LOGD("found [shard={}], trace_id=[{}]", shard->info.id, tid);
            info_l.push_back(shard->info);
        }
        return info_l;
    });
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::seal_shard(shard_id_t id, trace_id_t tid) {
    return _get_shard(id, tid).thenValue([this, tid](auto const e) mutable -> ShardManager::AsyncResult< ShardInfo > {
        if (!e) return folly::makeUnexpected(ShardError(ShardErrorCode::UNKNOWN_SHARD));
        if (ShardInfo::State::SEALED == e.value().state) return e.value();
        return _seal_shard(e.value(), tid);
    });
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::get_shard(shard_id_t id, trace_id_t tid) const {
    return _get_shard(id, tid).thenValue([this](auto const e) mutable -> ShardManager::Result< ShardInfo > {
        if (!e) { return folly::makeUnexpected(e.error()); }
        return e.value();
    });
}

///
// This is used as a first call for many operations and initializes the Future.
//
folly::Future< ShardManager::Result< ShardInfo > > HomeObjectImpl::_get_shard(shard_id_t id, trace_id_t tid) const {
    return _defer().thenValue([this, id, tid](auto) -> ShardManager::Result< ShardInfo > {
        auto lg = std::shared_lock(_shard_lock);
        if (auto it = _shard_map.find(id); _shard_map.end() != it) return (*it->second)->info;
        LOGE("Couldn't find shard id in shard map {}, trace_id=[{}]", id, tid);
        return folly::makeUnexpected(ShardError(ShardErrorCode::UNKNOWN_SHARD));
    });
}

uint64_t HomeObjectImpl::get_current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast< std::chrono::milliseconds >(now.time_since_epoch());
    auto timestamp = static_cast< uint64_t >(duration.count());
    return timestamp;
}

} // namespace homeobject
