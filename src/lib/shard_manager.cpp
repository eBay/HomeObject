#include "homeobject_impl.hpp"

namespace homeobject {

uint64_t ShardManager::max_shard_size() { return Gi; }

std::shared_ptr< ShardManager > HomeObjectImpl::shard_manager() { return shared_from_this(); }

std::shared_ptr< HomeObject > init_homeobject(init_params const& params) {
    LOGINFOMOD(homeobject, "Initializing HomeObject");
    return std::make_shared< HomeObjectImpl >(params.lookup);
}

void HomeObjectImpl::create_shard(pg_id pg_owner, uint64_t size_bytes, ShardManager::info_cb const& cb) {
    if (0 == size_bytes || max_shard_size() < size_bytes)
        cb(ShardError::INVALID_ARG, std::nullopt);
    else
        cb(ShardError::UNKNOWN_PG, std::nullopt);
}

void HomeObjectImpl::list_shards(pg_id pg, ShardManager::list_cb const& cb) const {
    cb(ShardError::UNKNOWN_PG, std::nullopt);
}

void HomeObjectImpl::get_shard(shard_id id, ShardManager::info_cb const& cb) const {
    cb(ShardError::UNKNOWN_SHARD, std::nullopt);
}

void HomeObjectImpl::seal_shard(shard_id id, ShardManager::info_cb const& cb) {
    cb(ShardError::UNKNOWN_SHARD, std::nullopt);
}

} // namespace homeobject
