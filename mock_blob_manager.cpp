#include "mock_homeobject.hpp"

#define IS_M1_DEMO (0 < SISL_OPTIONS.count("m1_demo"))

namespace homeobject {
using namespace std::chrono_literals;
constexpr auto disk_latency = 15ms;

void MockHomeObject::put(shard_id shard, Blob const& blob, BlobManager::id_cb cb) {
    std::thread([this, shard, blob, cb]() {
        std::this_thread::sleep_for(disk_latency);
        blob_id id;
        auto err = BlobError::OK;
        {
            auto lg = std::scoped_lock(_shard_lock, _data_lock);
            if (0 == _shards.count(shard) && !IS_M1_DEMO) {
                err = BlobError::UNKNOWN_SHARD;
            } else {
                id = _cur_blob_id;
                auto new_blob = Blob{sisl::io_blob(blob.body.size), blob.user_key, blob.object_off};
                std::memcpy(new_blob.body.bytes, blob.body.bytes, new_blob.body.size);
                auto [it, happened] = _in_memory_disk.emplace(BlobRoute{shard, id}, new_blob);
                if (happened) { _cur_blob_id++; }
            }
        }

        if (cb) {
            if (err == BlobError::OK) {
                cb(id, std::nullopt);
            } else {
                cb(err, std::nullopt);
            }
        }
    }).detach();
}

void MockHomeObject::get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len,
                         BlobManager::get_cb cb) const {
    std::thread([this, shard, blob, cb]() {
        BlobError err = BlobError::OK;
        Blob ret;
        [&] {
            std::this_thread::sleep_for(disk_latency);
            auto lg = std::scoped_lock(_data_lock);
            if (auto const it = _shards.find(shard); it == _shards.end() && !IS_M1_DEMO) {
                err = BlobError::UNKNOWN_SHARD;
                return;
            }
            auto it = _in_memory_disk.end();
            if (it = _in_memory_disk.find(BlobRoute{shard, blob}); it == _in_memory_disk.end()) {
                err = BlobError::UNKNOWN_BLOB;
                return;
            }

            auto const& read_blob = it->second;
            ret.body = sisl::io_blob(read_blob.body.size);
            ret.object_off = read_blob.object_off;
            ret.user_key = read_blob.user_key;
            std::memcpy(ret.body.bytes, read_blob.body.bytes, ret.body.size);
        }();

        if (cb) {
            if (err == BlobError::OK) {
                cb(ret, std::nullopt);
            } else {
                cb(err, std::nullopt);
            }
        }
        
    }).detach();
}
void MockHomeObject::del(shard_id shard, blob_id const& blob, BlobManager::ok_cb cb) {
    std::thread([this, shard, blob, cb]() {
        BlobError err = BlobError::OK;
        [&] {
            auto lg = std::scoped_lock(_data_lock);
            if (auto const it = _shards.find(shard); it == _shards.end() && !IS_M1_DEMO) {
                err = BlobError::UNKNOWN_SHARD;
                return;
            }
            if (0 == _in_memory_disk.erase(BlobRoute{shard, blob}) && !IS_M1_DEMO) {
                err = BlobError::UNKNOWN_BLOB;
            }
        }();

        if (cb) { cb(err, std::nullopt); }
    }).detach();
}

} // namespace homeobject
