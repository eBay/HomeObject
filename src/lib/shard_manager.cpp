#include "homeobject_impl.hpp"

namespace homeobject {

uint64_t ShardManager::max_shard_size() { return Gi; }

std::shared_ptr< ShardManager > HomeObjectImpl::shard_manager() { return shared_from_this(); }

std::shared_ptr< HomeObject > init_homeobject(init_params const& params) {
    LOGINFOMOD(homeobject, "Initializing HomeObject");
    return std::make_shared< HomeObjectImpl >(params.lookup);
}

folly::Future< ShardManager::info_var > HomeObjectImpl::create_shard(pg_id pg_owner, uint64_t size_bytes) {
    if (0 == size_bytes || max_shard_size() < size_bytes) return folly::makeFuture(ShardError::INVALID_ARG);
    return folly::makeFuture(ShardError::UNKNOWN_PG);
}

folly::Future< ShardManager::list_var > HomeObjectImpl::list_shards(pg_id pg) const {
    return folly::makeFuture(ShardError::UNKNOWN_PG);
}

ShardManager::info_var HomeObjectImpl::get_shard(shard_id id) const { return ShardError::UNKNOWN_SHARD; }

folly::Future< ShardManager::info_var > HomeObjectImpl::seal_shard(shard_id id) {
    return folly::makeFuture(ShardError::UNKNOWN_SHARD);
}

} // namespace homeobject
