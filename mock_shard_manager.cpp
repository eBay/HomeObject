#include "mock_homeobject.hpp"
#include "mock_shard_manager.hpp"
#include <functional>
#include <optional>
#include <variant>

namespace homeobject {
void MockHomeObject::create_shard(pg_id pg_owner, uint64_t size_bytes, ShardManager::id_cb cb) {
    uint64_t id = 123;
    std::variant< shard_id, ShardError > ret{id};
    cb(ret, std::nullopt);
}
void MockHomeObject::get_shard(shard_id id, ShardManager::info_cb cb) const {
    ShardInfo shard_info = homeobject::create_mock_shard_info(id, 0, ShardInfo::State::OPEN, 1024 * 1024 * 1024);
    std::variant< ShardInfo, ShardError > ret{shard_info};
    cb(ret, std::nullopt);
}
void MockHomeObject::list_shards(pg_id id, ShardManager::list_cb cb) const {
    std::vector< ShardInfo > shard_list;
    for (uint8_t i = 0; i < 2; i++) {
        shard_list.emplace_back(
            homeobject::create_mock_shard_info(i, id, ShardInfo::State::OPEN, 1024 * 1024 * 1024));
    }
    std::variant< std::vector< ShardInfo >, ShardError > ret = shard_list;
    cb(ret, std::nullopt);
}
void MockHomeObject::seal_shard(shard_id id, ShardManager::info_cb cb) {
    ShardInfo shard_info = homeobject::create_mock_shard_info(id, 0, ShardInfo::State::SEALED, 1024 * 1024 * 1024);
    std::variant< ShardInfo, ShardError > ret = {shard_info};
    cb(ret, std::nullopt);
}

ShardInfo create_mock_shard_info(shard_id sid, pg_id pid, ShardInfo::State state, uint64_t size) {
    uint64_t cur_time = homeobject::get_current_timestamp();
    ShardInfo shard_info{sid, pid, state, cur_time, cur_time, size, size, 0};
    return shard_info;
}

uint64_t get_current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast< std::chrono::milliseconds >(now.time_since_epoch());
    uint64_t timestamp = static_cast< uint64_t >(duration.count());
    return timestamp;
}

uint16_t get_pgId_from_shardId(shard_id id) { return uint16_t(id >> 48); }
} // namespace homeobject
