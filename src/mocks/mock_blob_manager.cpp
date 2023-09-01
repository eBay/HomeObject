#include "mock_homeobject.hpp"

namespace homeobject {

BlobManager::AsyncResult< blob_id > MockHomeObject::put(shard_id shard, Blob&& blob) {
    return _get_shard(shard).thenValue(
        [this, blob = std::move(blob)](auto const e) mutable -> BlobManager::Result< blob_id > {
            if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
            auto const& shard_info = e.value();

            auto new_blkid = _in_memory_disk.end();
            blob_id new_id;
            {
                // Should be lock-free append only stucture
                auto lg = std::scoped_lock(_data_lock);
                LOGDEBUGMOD(homeobject, "Writing Blob {}", _in_memory_disk.size());
                new_id = _in_memory_disk.size();
                new_blkid = _in_memory_disk.insert(new_blkid, std::move(blob));
            }
            {
                auto lg = std::scoped_lock(_index_lock);
                auto [_, happened] = _in_memory_index.try_emplace(BlobRoute{shard_info.id, new_id}, new_blkid);
                RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
            }
            return new_id;
        });
}

BlobManager::AsyncResult< Blob > MockHomeObject::get(shard_id shard, blob_id const& id, uint64_t, uint64_t) const {
    return _get_shard(shard).thenValue([this, id](auto const e) -> BlobManager::Result< Blob > {
        if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
        auto const& shard_info = e.value();

        auto route = BlobRoute(shard_info.id, id);
        auto d_it = _in_memory_disk.cend();
        {
            auto lg = std::shared_lock(_index_lock);
            LOGDEBUGMOD(homeobject, "Looking up Blob {} in set of {}", route.blob, _in_memory_disk.size());
            if (auto it = _in_memory_index.find(route); _in_memory_index.end() != it) { d_it = it->second; }
        }
        if (_in_memory_disk.cend() == d_it) {
            LOGWARNMOD(homeobject, "Blob missing {} during get", route.blob);
            return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
        }

        auto& read_blob = *d_it;
        RELEASE_ASSERT(read_blob.body, "Blob returned with no body!");
        Blob blob;
        blob.body = std::make_unique< sisl::byte_array_impl >(read_blob.body->size);
        blob.object_off = read_blob.object_off;
        blob.user_key = read_blob.user_key;
        std::memcpy(blob.body->bytes, read_blob.body->bytes, blob.body->size);
        return blob;
    });
}

BlobManager::NullAsyncResult MockHomeObject::del(shard_id shard, blob_id const& id) {
    return _get_shard(shard).thenValue([this, id](auto const e) -> BlobManager::NullResult {
        if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
        auto const& shard_info = e.value();
        auto route = BlobRoute(shard_info.id, id);
        auto d_it = _in_memory_disk.cend();
        {
            auto lg = std::scoped_lock(_index_lock);
            LOGDEBUGMOD(homeobject, "Looking up Blob {} in set of {}", route.blob, _in_memory_index.size());
            if (auto it = _in_memory_index.find(route); _in_memory_index.end() != it) {
                d_it = it->second;
                _in_memory_index.erase(it);
            }
        }
        if (_in_memory_disk.cend() == d_it) {
            LOGWARNMOD(homeobject, "Blob missing {} during delete", route.blob);
            return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
        }
        // TODO We defer GC of the BLOB leaking BLOB into disk
        return folly::Unit();
    });
}

} // namespace homeobject
