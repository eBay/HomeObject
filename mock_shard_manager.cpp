#include "mock_homeobject.hpp"

namespace homeobject {
void MockHomeObject::create_shard(pg_id pg_owner, uint64_t size_mb, ShardManager::id_cb cb) {}
void MockHomeObject::get_shard(shard_id id, ShardManager::info_cb cb) const {}
void MockHomeObject::list_shards(pg_id id, ShardManager::list_cb cb) const {}
void MockHomeObject::seal_shard(shard_id id, ShardManager::info_cb cb) {}
} // namespace homeobject
