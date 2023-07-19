#include "mock_homeobject.hpp"

namespace homeobject {
void HomeObject::create_shard(pg_id pg_owner, uint64_t size_mb, info_cb cb) {}
void HomeObject::get_shard(shard_id id, info_cb cb) const {}
void HomeObject::list_shards(pg_id id, info_cb cb) const {}
void HomeObject::seal_shard(shard_id id, ShardManager::ok_cb cb) {}
} // namespace homeobject
