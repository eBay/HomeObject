#include "mock_homeobject.hpp"

namespace homeobject {
void MockHomeObject::create_shard(pg_id pg_owner, uint64_t size_mb, info_cb cb) {}
void MockHomeObject::get_shard(shard_id id, info_cb cb) const {}
void MockHomeObject::list_shards(pg_id id, info_cb cb) const {}
void MockHomeObject::seal_shard(shard_id id, ShardManager::ok_cb cb) {}
} // namespace homeobject
