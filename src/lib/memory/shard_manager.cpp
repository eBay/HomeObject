#include <chrono>

#include "homeobject.hpp"

namespace homeobject {

uint64_t ShardManager::max_shard_size() { return Gi; }

ShardManager::Result< ShardInfo > MemoryHomeObject::_create_shard(pg_id pg_owner, uint64_t size_bytes) {
    auto const now = get_current_timestamp();
    auto info = ShardInfo(0ull, pg_owner, ShardInfo::State::OPEN, now, now, size_bytes, size_bytes, 0);
    {
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);
        auto pg_it = _pg_map.find(pg_owner);
        if (_pg_map.end() == pg_it) return folly::makeUnexpected(ShardError::UNKNOWN_PG);

        auto& s_list = pg_it->second.shards;
        info.id = (((uint64_t)pg_owner) << 48) + s_list.size();
        auto shard = std::make_shared<Shard>(info);
        LOGDEBUG("Creating Shard [{}]: in Pg [{}] of Size [{}b]", info.id, pg_owner, size_bytes);
        s_list.push_back(shard);
        auto [_, s_happened] = _shard_map.emplace(info.id, shard);
        RELEASE_ASSERT(s_happened, "Duplicate Shard insertion!");
    }
    auto lg = std::scoped_lock(_index_lock);
    auto [_, happened] = _in_memory_index.try_emplace(info.id);
    RELEASE_ASSERT(happened, "Could not create BTree!");
    return info;
}

ShardManager::Result< ShardInfo > MemoryHomeObject::_seal_shard(shard_id id) {
    auto lg = std::scoped_lock(_shard_lock);
    auto shard_it = _shard_map.find(id);
    if (_shard_map.end() == shard_it) return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
    shard_it->second->info.state = ShardInfo::State::SEALED;
    return shard_it->second->info;
}

} // namespace homeobject
