#include <chrono>

#include "mem_homeobject.hpp"

namespace homeobject {

uint64_t ShardManager::max_shard_size() { return Gi; }

ShardManager::AsyncResult< ShardInfo > MemoryHomeObject::_create_shard(pg_id_t pg_owner, uint64_t size_bytes, std::string meta,
                                                                       trace_id_t tid) {
    (void)tid;
    auto const now = get_current_timestamp();
    auto info = ShardInfo(0ull, pg_owner, ShardInfo::State::OPEN, 0, now, now, size_bytes, size_bytes);
    {
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);
        auto pg_it = _pg_map.find(pg_owner);
        if (_pg_map.end() == pg_it) return folly::makeUnexpected(ShardError(ShardErrorCode::UNKNOWN_PG));

        auto& s_list = pg_it->second->shards_;
        info.id = make_new_shard_id(pg_owner, s_list.size());
        auto iter = s_list.emplace(s_list.end(), std::make_unique< Shard >(info));
        LOGDEBUG("Creating Shard [{}]: in pg={} of Size [{}b]", info.id & shard_mask, pg_owner, size_bytes);
        auto [_, s_happened] = _shard_map.emplace(info.id, iter);
        RELEASE_ASSERT(s_happened, "Duplicate Shard insertion!");
    }
    auto [it, happened] = index_.try_emplace(info.id, std::make_unique< ShardIndex >());
    RELEASE_ASSERT(happened, "Could not create BTree!");
    return info;
}

ShardManager::AsyncResult< ShardInfo > MemoryHomeObject::_seal_shard(ShardInfo const& info, trace_id_t tid) {
    (void)tid;
    auto lg = std::scoped_lock(_shard_lock);
    auto shard_it = _shard_map.find(info.id);
    RELEASE_ASSERT(_shard_map.end() != shard_it, "Missing ShardIterator!");
    auto& shard_info = (*shard_it->second)->info;
    shard_info.state = ShardInfo::State::SEALED;
    return shard_info;
}

} // namespace homeobject
