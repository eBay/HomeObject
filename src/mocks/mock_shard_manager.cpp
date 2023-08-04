#include "mock_homeobject.hpp"

namespace homeobject {
static uint64_t get_current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast< std::chrono::milliseconds >(now.time_since_epoch());
    uint64_t timestamp = static_cast< uint64_t >(duration.count());
    return timestamp;
}

#define WITH_SHARD_LOCKS(e)                                                                                            \
    auto err = (e);                                                                                                    \
    {                                                                                                                  \
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);

#define CB_FROM_SHARD_LOCKS(ret)                                                                                       \
    }                                                                                                                  \
    CB_IF_DEFINED(ret, err, ShardError::OK)

uint64_t ShardManager::max_shard_size() { return Gi; }

void MockHomeObject::create_shard(pg_id pg_owner, uint64_t size_bytes, ShardManager::info_cb const& cb) {
    if (0 == size_bytes || max_shard_size() < size_bytes) {
        if (cb) cb(ShardError::INVALID_ARG, std::nullopt);
        return;
    }
    auto ret = ShardInfo{0u, pg_owner, ShardInfo::State::OPEN, 1024 * 1024 * 1024, get_current_timestamp(), 0, 0, 0};
    WITH_SHARD_LOCKS(ShardError::UNKNOWN_PG);
    if (auto pg_it = _pg_map.find(pg_owner); _pg_map.end() != pg_it) {
        ret.id = _cur_shard_id++;
        LOGINFO("Creating Shard [{}]: in Pg [{}] of Size [{}b]", ret.id, pg_owner, size_bytes);
        pg_it->second.second.emplace(ret.id);
        err = _shards.try_emplace(ret.id, ret).second ? ShardError::OK : ShardError::UNKNOWN;
    }
    CB_FROM_SHARD_LOCKS(ret)
}

void MockHomeObject::get_shard(shard_id id, ShardManager::info_cb const& cb) const {
    auto info = ShardInfo();
    WITH_SHARD_LOCKS(ShardError::UNKNOWN_SHARD)
    if (auto shard_it = _shards.find(id); _shards.end() != shard_it) {
        info = shard_it->second;
        err = ShardError::OK;
    }
    CB_FROM_SHARD_LOCKS(info)
}

void MockHomeObject::list_shards(pg_id id, ShardManager::list_cb const& cb) const {
    auto info = std::vector< ShardInfo >();
    WITH_SHARD_LOCKS(ShardError::UNKNOWN_PG)
    if (auto pg_it = _pg_map.find(id); _pg_map.end() != pg_it) {
        err = ShardError::OK;
        for (auto const& shard_id : pg_it->second.second) {
            auto shard_it = _shards.find(shard_id);
            RELEASE_ASSERT(_shards.end() != shard_it, "Missing Shard [{}]!", shard_id);
            info.push_back(shard_it->second);
        }
    }
    CB_FROM_SHARD_LOCKS(info)
}

void MockHomeObject::seal_shard(shard_id id, ShardManager::info_cb const& cb) {
    auto info = ShardInfo();
    WITH_SHARD_LOCKS(ShardError::UNKNOWN_SHARD)
    if (auto shard_it = _shards.find(id); _shards.end() != shard_it) {
        shard_it->second.state = ShardInfo::State::SEALED;
        info = shard_it->second;
        err = ShardError::OK;
    }
    CB_FROM_SHARD_LOCKS(info)
}

} // namespace homeobject
