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
    if (auto pg_it = _pg_map.find(pg_owner); _pg_map.end() != pg_it) {
        auto const now = get_current_timestamp();
        auto info = ShardInfo{_cur_shard_id++, pg_owner, ShardInfo::State::OPEN, now, now, size_bytes, size_bytes, 0};
        LOGDEBUG("Creating Shard [{}]: in Pg [{}] of Size [{}b] shard_cnt:[{}]", info.id, pg_owner, size_bytes,
                 _shards.size());

        pg_it->second.emplace(info.id);
        auto [_, happened] = _shards.try_emplace(info.id, info);
        RELEASE_ASSERT(happened, "Duplicate Shard insertion!");
        return info;
    }
    return folly::makeUnexpected(ShardError::UNKNOWN_PG);
}

folly::Future< ShardManager::Result< ShardInfo > > MemoryHomeObject::_get_shard(shard_id id) const {
    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, id](auto) -> ShardManager::Result< ShardInfo > {
            auto lg = std::scoped_lock(_shard_lock);
            if (auto shard_it = _shards.find(id); _shards.end() != shard_it) return shard_it->second;
            return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
        });
}

ShardManager::Result< InfoList > MemoryHomeObject::_list_shards(pg_id id) const {
    auto lg = std::scoped_lock(_pg_lock, _shard_lock);
    if (auto pg_it = _pg_map.find(id); _pg_map.end() != pg_it) {
        auto info = std::list< ShardInfo >();
        for (auto const& shard_id : pg_it->second) {
            LOGDEBUG("Listing Shard {}", shard_id);
            auto shard_it = _shards.find(shard_id);
            RELEASE_ASSERT(_shards.end() != shard_it, "Missing Shard [{}]!", shard_id);
            info.push_back(shard_it->second);
        }
        return info;
    }
    return folly::makeUnexpected(ShardError::UNKNOWN_PG);
}

ShardManager::Result< ShardInfo > MemoryHomeObject::_seal_shard(shard_id id) {
    auto lg = std::scoped_lock(_pg_lock, _shard_lock);
    if (auto shard_it = _shards.find(id); _shards.end() != shard_it) {
        shard_it->second.state = ShardInfo::State::SEALED;
        return shard_it->second;
    }
    return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
}

} // namespace homeobject
