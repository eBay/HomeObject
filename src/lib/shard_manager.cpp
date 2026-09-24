#include <chrono>

#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< ShardManager > HomeObjectImpl::shard_manager() { return shared_from_this(); }

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::create_shard(pg_id_t pg_owner, uint64_t size_bytes,
                                                                    std::string meta, trace_id_t tid) {
    if (0 == size_bytes || max_shard_size() < size_bytes)
        co_return std::unexpected(ShardError(ShardErrorCode::INVALID_ARG));
    co_return co_await _create_shard(pg_owner, size_bytes, meta, tid);
}

ShardManager::AsyncResult< InfoList > HomeObjectImpl::list_shards(pg_id_t pgid, trace_id_t tid) const {
    std::shared_lock lock_guard(_pg_lock);
    auto iter = _pg_map.find(pgid);
    if (iter == _pg_map.cend()) { co_return std::unexpected(ShardError(ShardErrorCode::UNKNOWN_PG)); }
    auto& pg = iter->second;

    auto info_l = std::list< ShardInfo >();
    for (auto const& shard : pg->shards_) {
        LOGD("found [shard={}], trace_id=[{}]", shard->info.id, tid);
        info_l.push_back(shard->info);
    }
    co_return info_l;
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::seal_shard(shard_id_t id, trace_id_t tid) {
    auto e = _get_shard(id, tid);
    if (!e) co_return std::unexpected(ShardError(ShardErrorCode::UNKNOWN_SHARD));
    if (ShardInfo::State::SEALED == e.value().state) co_return e.value();
    co_return co_await _seal_shard(e.value(), tid);
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::get_shard(shard_id_t id, trace_id_t tid) const {
    co_return _get_shard(id, tid);
}

ShardManager::Result< ShardInfo > HomeObjectImpl::_get_shard(shard_id_t id, trace_id_t tid) const {
    auto lg = std::shared_lock(_shard_lock);
    if (auto it = _shard_map.find(id); _shard_map.end() != it) return (*it->second)->info;
    LOGE("Couldn't find shard id in shard map {}, trace_id=[{}]", id, tid);
    return std::unexpected(ShardError(ShardErrorCode::UNKNOWN_SHARD));
}

uint64_t HomeObjectImpl::get_current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast< std::chrono::milliseconds >(now.time_since_epoch());
    auto timestamp = static_cast< uint64_t >(duration.count());
    return timestamp;
}

} // namespace homeobject
