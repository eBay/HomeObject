#include "mock_homeobject.hpp"

namespace homeobject {
using namespace std::chrono_literals;
constexpr auto disk_latency = 15ms;

#define CB_IF_DEFINED(ret, e)                                                                                          \
    if (!cb) return;                                                                                                   \
    auto const temp_err = (e);                                                                                         \
    if (temp_err == BlobError::OK) {                                                                                   \
        cb((ret), std::nullopt);                                                                                       \
    } else {                                                                                                           \
        cb(temp_err, std::nullopt);                                                                                    \
    }

void MockHomeObject::put(shard_id shard, Blob&& blob, BlobManager::id_cb cb) {
    std::thread([this, shard, mblob = std::move(blob), cb]() mutable {
        std::this_thread::sleep_for(disk_latency);
        blob_id id;
        auto err = BlobError::UNKNOWN_SHARD;
        {
            // Need both locks to prevent sealing while writting
            auto lg = std::scoped_lock(_shard_lock, _data_lock);
            if (0 != _shards.count(shard)) {
                err = BlobError::OK;
                id = _cur_blob_id;
                if (auto [it, happened] = _in_memory_disk.emplace(BlobRoute{shard, id}, std::move(mblob)); happened) {
                    _cur_blob_id++;
                }
            }
        }
        CB_IF_DEFINED(id, err);
    }).detach();
}

void MockHomeObject::get(shard_id shard, blob_id const& id, uint64_t off, uint64_t len, BlobManager::get_cb cb) const {
    std::thread([this, shard, id, cb]() {
        BlobError err = BlobError::UNKNOWN_SHARD;
        Blob blob;

        // Only need to lookup shard with _shard_lock for READs, okay to seal while reading
        {
            auto lg = std::scoped_lock(_shard_lock);
            if (auto const it = _shards.find(shard); it != _shards.end()) { err = BlobError::OK; }
        }

        if (BlobError::OK == err) {
            [&] {
                std::this_thread::sleep_for(disk_latency);
                auto lg = std::scoped_lock(_data_lock);
                auto it = _in_memory_disk.end();
                if (it = _in_memory_disk.find(BlobRoute{shard, id}); it == _in_memory_disk.end()) {
                    err = BlobError::UNKNOWN_BLOB;
                    return;
                }

                auto const& read_blob = it->second;
                blob.body = std::make_unique< sisl::byte_array_impl >(read_blob.body->size);
                blob.object_off = read_blob.object_off;
                blob.user_key = read_blob.user_key;
                std::memcpy(blob.body->bytes, read_blob.body->bytes, blob.body->size);
            }();
        }
        CB_IF_DEFINED(std::move(blob), err);
    }).detach();
}
void MockHomeObject::del(shard_id shard, blob_id const& blob, BlobManager::ok_cb cb) {
    std::thread([this, shard, blob, cb]() {
        BlobError err = BlobError::UNKNOWN_SHARD;
        [&] {
            /// Take both locks so we don't seal while deleting the BLOB.
            auto lg = std::scoped_lock(_shard_lock, _data_lock);
            if (auto const it = _shards.find(shard); it != _shards.end()) {
                err = (0 < _in_memory_disk.erase(BlobRoute{shard, blob})) ? BlobError::OK : BlobError::UNKNOWN_BLOB;
            }
        }();
        CB_IF_DEFINED(err, BlobError::OK);
    }).detach();
}

} // namespace homeobject
