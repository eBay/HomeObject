#include "mock_homeobject.hpp"

namespace homeobject {
static constexpr uint32_t disk_latency = 15; // ms

void MockHomeObject::put(shard_id shard, Blob const& blob, id_cb cb) {
    std::thread([this, shard, blob, cb]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(disk_latency));
        auto lg = std::scoped_lock(_data_lock);
        _shards.insert(shard);
        _in_memory_disk.emplace(_cur_blob_id, blob);
        cb(_cur_blob_id++, std::nullopt);
    }).detach();
}

void MockHomeObject::get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len, get_cb cb) const {
    std::thread([this, shard, blob, cb]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(disk_latency));
        auto lg = std::scoped_lock(_data_lock);
        if (auto const it = _shards.find(shard); it == _shards.end()) {
            cb(BlobError::UNKNOWN_SHARD, std::nullopt);
            return;
        }
        auto it = _in_memory_disk.end();
        if (it = _in_memory_disk.find(blob); it == _in_memory_disk.end()) {
            cb(BlobError::UNKNOWN_BLOB, std::nullopt);
            return;
        }

        auto const& read_blob = it->second;
        auto ret = Blob{sisl::io_blob(read_blob.body.size), read_blob.user_key, read_blob.object_off};
        std::memcpy(reinterpret_cast< void* >(ret.body.bytes), reinterpret_cast< void* >(read_blob.body.bytes),
                    ret.body.size);
        cb(ret, std::nullopt);
    }).detach();
}
void MockHomeObject::del(shard_id shard, blob_id const& blob, BlobManager::ok_cb cb) {
    std::thread([this, shard, blob, cb]() {
        auto lg = std::scoped_lock(_data_lock);
        if (auto const it = _shards.find(shard); it == _shards.end()) {
            cb(BlobError::UNKNOWN_SHARD, std::nullopt);
            return;
        }
        auto it = _in_memory_disk.end();
        if (it = _in_memory_disk.find(blob); it == _in_memory_disk.end()) {
            cb(BlobError::UNKNOWN_BLOB, std::nullopt);
            return;
        }

        _shards.erase(shard);
        _in_memory_disk.erase(blob);
        cb(BlobError::OK, std::nullopt);
    }).detach();
}

} // namespace homeobject
