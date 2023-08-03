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

#define CB_FROM_SHARD_LOCKS(ret, e, ok)                                                                                \
    }                                                                                                                  \
    CB_IF_DEFINED(ret, e, ok)

void MockHomeObject::create_shard(pg_id pg_owner, uint64_t size_bytes, ShardManager::info_cb const& cb) {
    LOGINFO("Creating Shard: in Pg [{}] of Size [{}b]", pg_owner, size_bytes);
    auto ret = ShardInfo();
    WITH_SHARD_LOCKS(ShardError::UNKNOWN_PG);
    if (auto pg_it = _pg_map.find(pg_owner); _pg_map.end() != pg_it) {
        auto const id = _cur_shard_id++;
        pg_it->second.second.emplace(id);
        if (auto [it, happened] = _shards.emplace(std::make_pair(
                id,
                ShardInfo{id, pg_owner, ShardInfo::State::OPEN, 1024 * 1024 * 1024, get_current_timestamp(), 0, 0, 0}));
            _shards.end() != it) {
            err = happened ? ShardError::OK : ShardError::UNKNOWN;
            ret = it->second;
        }
    }
    CB_FROM_SHARD_LOCKS(ret, err, ShardError::OK)
}

void MockHomeObject::get_shard(shard_id id, ShardManager::info_cb const& cb) const {
    auto info = ShardInfo();
    WITH_SHARD_LOCKS(ShardError::UNKNOWN_SHARD)
    if (auto shard_it = _shards.find(id); _shards.end() != shard_it) {
        info = shard_it->second;
        err = ShardError::OK;
    }
    CB_FROM_SHARD_LOCKS(info, err, ShardError::OK)
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
    CB_FROM_SHARD_LOCKS(info, err, ShardError::OK)
}

void MockHomeObject::seal_shard(shard_id id, ShardManager::info_cb const& cb) {
    auto info = ShardInfo();
    WITH_SHARD_LOCKS(ShardError::UNKNOWN_SHARD)
    if (auto shard_it = _shards.find(id); _shards.end() != shard_it) {
        shard_it->second.state = ShardInfo::State::SEALED;
        info = shard_it->second;
        err = ShardError::OK;
    }
    CB_FROM_SHARD_LOCKS(info, err, ShardError::OK)
}

} // namespace homeobject
