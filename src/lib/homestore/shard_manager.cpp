#include "homeobject.hpp"

namespace homeobject {

uint64_t ShardManager::max_shard_size() { return Gi; }

ShardManager::Result< ShardInfo > HSHomeObject::_create_shard(pg_id pg_owner, uint64_t size_bytes) {
    return folly::makeUnexpected(ShardError::UNKNOWN_PG);
}

ShardManager::Result< ShardInfo > HSHomeObject::_seal_shard(shard_id id) {
    return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
}

folly::Future< ShardManager::Result< ShardInfo > > HSHomeObject::_get_shard(shard_id id) const {
    return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
}

ShardManager::Result< InfoList > HSHomeObject::_list_shards(pg_id pg) const {
    return folly::makeUnexpected(ShardError::UNKNOWN_PG);
}

} // namespace homeobject
