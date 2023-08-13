#include "mock_homeobject.hpp"

namespace homeobject {
using namespace std::chrono_literals;
constexpr auto disk_latency = 15ms;

folly::SemiFuture< std::variant< blob_id, BlobError > > MockHomeObject::put(shard_id shard, Blob&& blob) {
    auto [p, f] = folly::makePromiseContract< std::variant< blob_id, BlobError > >();
    std::thread([this, shard, mblob = std::move(blob), p = std::move(p)]() mutable {
        std::this_thread::sleep_for(disk_latency);
        blob_id id;
        auto err = BlobError::UNKNOWN_SHARD;
        {
            auto lg = std::scoped_lock(_shard_lock, _data_lock);
            if (0 != _shards.count(shard)) {
                err = BlobError::OK;
                id = _cur_blob_id;
                if (auto [_, happened] = _in_memory_disk.try_emplace(BlobRoute{shard, id}, std::move(mblob));
                    happened) {
                    _cur_blob_id++;
                }
            }
        }
        (BlobError::OK == err) ? p.setValue(id) : p.setValue(err);
    }).detach();
    return f;
}

folly::SemiFuture< std::variant< Blob, BlobError > > MockHomeObject::get(shard_id shard, blob_id const& id, uint64_t,
                                                                         uint64_t) const {
    auto [p, f] = folly::makePromiseContract< std::variant< Blob, BlobError > >();
    std::thread([this, shard, id, p = std::move(p)]() mutable {
        // Only need to lookup shard with _shard_lock for READs, okay to seal while reading
        {
            auto lg = std::scoped_lock(_shard_lock);
            LOGDEBUG("Looking up shard {} in set of {}", shard, _shards.size());
            if (0 == _shards.count(shard)) p.setValue(BlobError::UNKNOWN_SHARD);
        }
        if (p.isFulfilled()) return;

        std::this_thread::sleep_for(disk_latency);
        Blob blob;
        {
            auto lg = std::scoped_lock(_shard_lock, _data_lock);
            LOGDEBUG("Looking up Blob {} in set of {}", id, _in_memory_disk.size());
            if (auto it = _in_memory_disk.find(BlobRoute{shard, id}); it != _in_memory_disk.end()) {
                auto const& read_blob = it->second;
                blob.body = std::make_unique< sisl::byte_array_impl >(read_blob.body->size);
                blob.object_off = read_blob.object_off;
                blob.user_key = read_blob.user_key;
                std::memcpy(blob.body->bytes, read_blob.body->bytes, blob.body->size);
                p.setValue(std::move(blob));
            }
        }
        if (!p.isFulfilled()) p.setValue(BlobError::UNKNOWN_BLOB);
    }).detach();
    return f;
}

folly::SemiFuture< BlobError > MockHomeObject::del(shard_id shard, blob_id const& blob) {
    auto [p, f] = folly::makePromiseContract< BlobError >();
    std::thread([this, shard, blob, p = std::move(p)]() mutable {
        auto err = BlobError::UNKNOWN_SHARD;
        {
            auto lg = std::scoped_lock(_shard_lock, _data_lock);
            if (auto const it = _shards.find(shard); it != _shards.end()) {
                err = (0 < _in_memory_disk.erase(BlobRoute{shard, blob})) ? BlobError::OK : BlobError::UNKNOWN_BLOB;
            }
        }
        p.setValue(err);
    }).detach();
    return f;
}

} // namespace homeobject
