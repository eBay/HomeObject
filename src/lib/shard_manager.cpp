#include "homeobject_impl.hpp"

namespace homeobject {

uint64_t ShardManager::max_shard_size() { return Gi; }

std::shared_ptr< ShardManager > HomeObjectImpl::shard_manager() {
    init_repl_svc();
    return shared_from_this();
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::create_shard(pg_id pg_owner, uint64_t size_bytes) {
    if (0 == size_bytes || max_shard_size() < size_bytes) return folly::makeUnexpected(ShardError::INVALID_ARG);
    return folly::makeUnexpected(ShardError::UNKNOWN_PG);
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::seal_shard(shard_id id) {
    return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
}

ShardManager::Result< InfoList > HomeObjectImpl::list_shards(pg_id pg) const {
    return folly::makeUnexpected(ShardError::UNKNOWN_PG);
}

ShardManager::Result< ShardInfo > HomeObjectImpl::get_shard(shard_id id) const {
    return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
}

} // namespace homeobject
