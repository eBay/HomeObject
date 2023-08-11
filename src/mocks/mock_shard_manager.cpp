#include "mock_homeobject.hpp"

namespace homeobject {
static uint64_t get_current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast< std::chrono::milliseconds >(now.time_since_epoch());
    auto timestamp = static_cast< uint64_t >(duration.count());
    return timestamp;
}

uint64_t ShardManager::max_shard_size() { return Gi; }

folly::Future< ShardManager::info_var > MockHomeObject::create_shard(pg_id pg_owner, uint64_t size_bytes) {
    if (0 == size_bytes || max_shard_size() < size_bytes) return folly::makeFuture(ShardError::INVALID_ARG);
    auto const now = get_current_timestamp();
    auto info = ShardInfo{0u, pg_owner, ShardInfo::State::OPEN, now, now, size_bytes, size_bytes, 0, std::nullopt};
    auto err = ShardError::UNKNOWN_PG;
    {
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);
        if (auto pg_it = _pg_map.find(pg_owner); _pg_map.end() != pg_it) {
            info.id = _cur_shard_id++;
            LOGINFO("Creating Shard [{}]: in Pg [{}] of Size [{}b]", info.id, pg_owner, size_bytes);
            pg_it->second.second.emplace(info.id);
            err = _shards.try_emplace(info.id, info).second ? ShardError::OK : ShardError::UNKNOWN;
        }
    }
    if (err == ShardError::OK) return folly::makeFuture(std::move(info));
    return folly::makeFuture(err);
}

ShardManager::info_var MockHomeObject::get_shard(shard_id id) const {
    auto info = ShardInfo();
    auto err = ShardError::UNKNOWN_SHARD;
    {
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);
        if (auto shard_it = _shards.find(id); _shards.end() != shard_it) {
            info = shard_it->second;
            err = ShardError::OK;
        }
    }
    if (err == ShardError::OK) return info;
    return err;
}

folly::Future< ShardManager::list_var > MockHomeObject::list_shards(pg_id id) const {
    auto info = std::vector< ShardInfo >();
    auto err = ShardError::UNKNOWN_PG;
    {
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);
        if (auto pg_it = _pg_map.find(id); _pg_map.end() != pg_it) {
            err = ShardError::OK;
            for (auto const& shard_id : pg_it->second.second) {
                auto shard_it = _shards.find(shard_id);
                RELEASE_ASSERT(_shards.end() != shard_it, "Missing Shard [{}]!", shard_id);
                info.push_back(shard_it->second);
            }
        }
    }
    if (err == ShardError::OK) return folly::makeFuture(std::move(info));
    return folly::makeFuture(err);
}

folly::Future< ShardManager::info_var > MockHomeObject::seal_shard(shard_id id) {
    auto info = ShardInfo();
    auto err = ShardError::UNKNOWN_SHARD;
    {
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);
        if (auto shard_it = _shards.find(id); _shards.end() != shard_it) {
            shard_it->second.state = ShardInfo::State::SEALED;
            info = shard_it->second;
            err = ShardError::OK;
        }
    }
    if (err == ShardError::OK) return folly::makeFuture(std::move(info));
    return folly::makeFuture(err);
}

} // namespace homeobject
