#include "mock_homeobject.hpp"

namespace homeobject {
static uint64_t get_current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast< std::chrono::milliseconds >(now.time_since_epoch());
    auto timestamp = static_cast< uint64_t >(duration.count());
    return timestamp;
}

uint64_t ShardManager::max_shard_size() { return Gi; }

folly::SemiFuture< ShardManager::info_var > MockHomeObject::create_shard(pg_id pg_owner, uint64_t size_bytes) {
    if (0 == size_bytes || max_shard_size() < size_bytes) return folly::makeFuture(ShardError::INVALID_ARG);

    auto [p, f] = folly::makePromiseContract< ShardManager::info_var >();
    std::thread([this, pg_owner, size_bytes, p = std::move(p)]() mutable {
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);
        if (auto pg_it = _pg_map.find(pg_owner); _pg_map.end() != pg_it) {
            auto const now = get_current_timestamp();
            auto info =
                ShardInfo{_cur_shard_id++, pg_owner, ShardInfo::State::OPEN, now, now, size_bytes, size_bytes, 0};
            pg_it->second.second.emplace(info.id);
            LOGDEBUG("Creating Shard [{}]: in Pg [{}] of Size [{}b] shard_cnt:[{}]", info.id, pg_owner, size_bytes,
                     _shards.size());
            if (!_shards.try_emplace(info.id, info).second)
                p.setValue(ShardError::UNKNOWN);
            else
                p.setValue(std::move(info));
        } else
            p.setValue(ShardError::UNKNOWN_PG);
    }).detach();
    return f;
}

ShardManager::info_var MockHomeObject::get_shard(shard_id id) const {
    auto lg = std::scoped_lock(_pg_lock, _shard_lock);
    if (auto shard_it = _shards.find(id); _shards.end() != shard_it) return shard_it->second;
    return ShardError::UNKNOWN_SHARD;
}

folly::SemiFuture< ShardManager::list_var > MockHomeObject::list_shards(pg_id id) const {
    auto [p, f] = folly::makePromiseContract< ShardManager::list_var >();
    std::thread([this, id, p = std::move(p)]() mutable {
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);
        if (auto pg_it = _pg_map.find(id); _pg_map.end() != pg_it) {
            auto info = std::vector< ShardInfo >();
            for (auto const& shard_id : pg_it->second.second) {
                LOGDEBUG("Listing Shard {}", shard_id);
                auto shard_it = _shards.find(shard_id);
                RELEASE_ASSERT(_shards.end() != shard_it, "Missing Shard [{}]!", shard_id);
                info.push_back(shard_it->second);
            }
            p.setValue(std::move(info));
        } else
            p.setValue(ShardError::UNKNOWN_PG);
    }).detach();
    return f;
}

folly::SemiFuture< ShardManager::info_var > MockHomeObject::seal_shard(shard_id id) {
    auto [p, f] = folly::makePromiseContract< ShardManager::info_var >();
    std::thread([this, id, p = std::move(p)]() mutable {
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);
        if (auto shard_it = _shards.find(id); _shards.end() != shard_it) {
            shard_it->second.state = ShardInfo::State::SEALED;
            auto info = shard_it->second;
            p.setValue(std::move(info));
        } else
            p.setValue(ShardError::UNKNOWN_SHARD);
    }).detach();
    return f;
}

} // namespace homeobject
