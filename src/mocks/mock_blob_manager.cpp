#include "mock_homeobject.hpp"

namespace homeobject {
using namespace std::chrono_literals;
constexpr auto disk_latency = 15ms;

#define WITH_DATA_LOCKS(e)                                                                                             \
    auto err = (e);                                                                                                    \
    {                                                                                                                  \
        auto lg = std::scoped_lock(_shard_lock, _data_lock);

#define CB_FROM_DATA_LOCKS(ret, e, ok)                                                                                 \
    }                                                                                                                  \
    CB_IF_DEFINED(ret, e, ok)

void MockHomeObject::put(shard_id shard, Blob&& blob, BlobManager::id_cb const& cb) {
    std::thread([this, shard, mblob = std::move(blob), cb]() mutable {
        std::this_thread::sleep_for(disk_latency);
        blob_id id;
        WITH_DATA_LOCKS(BlobError::UNKNOWN_SHARD)
        if (0 != _shards.count(shard)) {
            err = BlobError::OK;
            id = _cur_blob_id;
            if (auto [it, happened] = _in_memory_disk.emplace(BlobRoute{shard, id}, std::move(mblob)); happened) {
                _cur_blob_id++;
            }
        }
        CB_FROM_DATA_LOCKS(id, err, BlobError::OK)
    }).detach();
}

void MockHomeObject::get(shard_id shard, blob_id const& id, uint64_t off, uint64_t len,
                         BlobManager::get_cb const& cb) const {
    std::thread([this, shard, id, cb]() {
        BlobError serr = BlobError::UNKNOWN_SHARD;
        // Only need to lookup shard with _shard_lock for READs, okay to seal while reading
        {
            auto lg = std::scoped_lock(_shard_lock);
            if (auto const it = _shards.find(shard); it != _shards.end()) { serr = BlobError::OK; }
        }
        if (BlobError::OK != serr) {
            CB_IF_DEFINED(serr, serr, BlobError::OK);
            return;
        }

        std::this_thread::sleep_for(disk_latency);
        Blob blob;
        WITH_DATA_LOCKS(BlobError::UNKNOWN_BLOB)
        if (auto it = _in_memory_disk.find(BlobRoute{shard, id}); it != _in_memory_disk.end()) {
            auto const& read_blob = it->second;
            blob.body = std::make_unique< sisl::byte_array_impl >(read_blob.body->size);
            blob.object_off = read_blob.object_off;
            blob.user_key = read_blob.user_key;
            std::memcpy(blob.body->bytes, read_blob.body->bytes, blob.body->size);
            err = BlobError::OK;
        }
        CB_FROM_DATA_LOCKS(std::move(blob), err, BlobError::OK)
    }).detach();
}
void MockHomeObject::del(shard_id shard, blob_id const& blob, BlobManager::ok_cb const& cb) {
    std::thread([this, shard, blob, cb]() {
        WITH_DATA_LOCKS(BlobError::UNKNOWN_SHARD)
        if (auto const it = _shards.find(shard); it != _shards.end()) {
            err = (0 < _in_memory_disk.erase(BlobRoute{shard, blob})) ? BlobError::OK : BlobError::UNKNOWN_BLOB;
        }
        CB_FROM_DATA_LOCKS(err, BlobError::OK, BlobError::OK)
    }).detach();
}

} // namespace homeobject
