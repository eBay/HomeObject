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
    auto const now = get_current_timestamp();
    auto info = ShardInfo{0ull, pg_owner, ShardInfo::State::OPEN, now, now, size_bytes, size_bytes, 0};
    {
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);
        auto pg_it = _pg_map.find(pg_owner);
        if (_pg_map.end() == pg_it) return folly::makeUnexpected(ShardError::UNKNOWN_PG);

        auto& s_set = pg_it->second.second;
        info.id = (((uint64_t)pg_owner) << 48) + s_set.size();
        LOGDEBUG("Creating Shard [{}]: in Pg [{}] of Size [{}b]", info.id, pg_owner, size_bytes);

        auto [s_it, happened] = s_set.emplace(info);
        RELEASE_ASSERT(happened, "Duplicate Shard insertion!");
        auto [_, s_happened] = _shard_map.emplace(info.id, s_it);
        RELEASE_ASSERT(s_happened, "Duplicate Shard insertion!");
    }
    auto [it, happened] = _in_memory_index.try_emplace(info.id, std::make_unique< ShardIndex >());
    RELEASE_ASSERT(happened, "Could not create BTree!");
    return info;
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
    pg_it->second.second.erase(const_it);

    auto [s_it, happened] = pg_it->second.second.emplace(new_info);
    RELEASE_ASSERT(happened, "Duplicate Shard insertion!");
    shard_it->second = s_it;

    return new_info;
}

} // namespace homeobject
