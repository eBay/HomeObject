#include <chrono>

#include "homeobject.hpp"

namespace homeobject {

uint64_t ShardManager::max_shard_size() { return Gi; }

static uint64_t get_current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast< std::chrono::milliseconds >(now.time_since_epoch());
    auto timestamp = static_cast< uint64_t >(duration.count());
    return timestamp;
}

ShardManager::Result< ShardInfo > MemoryHomeObject::_create_shard(pg_id pg_owner, uint64_t size_bytes) {
    auto lg = std::scoped_lock(_pg_lock, _shard_lock);
    auto pg_it = _pg_map.find(pg_owner);
    if (_pg_map.end() == pg_it) return folly::makeUnexpected(ShardError::UNKNOWN_PG);

    auto const now = get_current_timestamp();
    auto info = ShardInfo{pg_it->second.size(), pg_owner, ShardInfo::State::OPEN, now, now, size_bytes, size_bytes, 0};

    LOGDEBUG("Creating Shard [{}]: in Pg [{}] of Size [{}b]", info.id, pg_owner, size_bytes);

    auto [s_it, happened] = pg_it->second.emplace(info);
    RELEASE_ASSERT(happened, "Duplicate Shard insertion!");
    auto [_, s_happened] = _shard_map.emplace(info.id, s_it);
    RELEASE_ASSERT(s_happened, "Duplicate Shard insertion!");
    return info;
}

folly::Future< ShardManager::Result< ShardInfo > > MemoryHomeObject::_get_shard(shard_id id) const {
    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, id](auto) -> ShardManager::Result< ShardInfo > {
            auto lg = std::shared_lock(_shard_lock);
            if (auto it = _shard_map.find(id); _shard_map.end() != it) return *it->second;
            return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
        });
}

ShardManager::Result< InfoList > MemoryHomeObject::_list_shards(pg_id id) const {
    auto lg = std::shared_lock(_pg_lock);
    auto pg_it = _pg_map.find(id);
    if (_pg_map.end() == pg_it) return folly::makeUnexpected(ShardError::UNKNOWN_PG);

    auto info_l = std::list< ShardInfo >();
    for (auto const& info : pg_it->second) {
        LOGDEBUG("Listing Shard {}", info.id);
        info_l.push_back(info);
    }
    return info_l;
}

ShardManager::Result< ShardInfo > MemoryHomeObject::_seal_shard(shard_id id) {
    auto lg = std::scoped_lock(_pg_lock, _shard_lock);
    auto shard_it = _shard_map.find(id);
    if (_shard_map.end() == shard_it) return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);

    auto const_it = shard_it->second;
    auto pg_it = _pg_map.find(const_it->placement_group);
    RELEASE_ASSERT(_pg_map.end() != pg_it, "Missing ShardInfo!");

    auto new_info = *const_it;
    new_info.state = ShardInfo::State::SEALED;
    pg_it->second.erase(const_it);

    auto [s_it, happened] = pg_it->second.emplace(new_info);
    RELEASE_ASSERT(happened, "Duplicate Shard insertion!");
    shard_it->second = s_it;

    return new_info;
}

} // namespace homeobject
