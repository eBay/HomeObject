#include "mock_homeobject.hpp"

namespace homeobject {

bool MockHomeObject::_has_shard(shard_id shard) const {
    auto lg = std::shared_lock(_shard_lock);
    LOGTRACEMOD(homeobject, "Looking up shard {} in set of {}", shard, _shards.size());
    if (0 < _shards.count(shard)) return true;
    LOGWARNMOD(homeobject, "Missing shard: {}", shard);
    return false;
}

BlobManager::AsyncResult< blob_id > MockHomeObject::put(shard_id shard, Blob&& blob) {
    if (!_has_shard(shard)) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);

    return folly::makeSemiFuture().deferValue(
        [this, shard, blob = std::move(blob)](auto) mutable -> BlobManager::Result< blob_id > {
            auto lg = std::scoped_lock(_data_lock);
            LOGDEBUGMOD(homeobject, "Writing Blob {} in set of {}", _cur_blob_id, _in_memory_disk.size());
            auto [_, happened] = _in_memory_disk.try_emplace(BlobRoute{shard, _cur_blob_id}, std::move(blob));
            RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
            return _cur_blob_id++;
        });
}

BlobManager::AsyncResult< Blob > MockHomeObject::get(shard_id shard, blob_id const& id, uint64_t, uint64_t) const {
    if (!_has_shard(shard)) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);

    return folly::makeSemiFuture().deferValue([this, shard, id](auto) mutable -> BlobManager::Result< Blob > {
        Blob blob;
        auto lg = std::shared_lock(_data_lock);
        LOGDEBUGMOD(homeobject, "Looking up Blob {} in set of {}", id, _in_memory_disk.size());
        auto it = _in_memory_disk.find(BlobRoute{shard, id});
        if (it == _in_memory_disk.end()) {
            LOGWARNMOD(homeobject, "Blob missing {} during get", id);
            return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
        }

        LOGTRACEMOD(homeobject, "Reading BLOB data for {}", id);
        auto const& read_blob = it->second;
        blob.body = std::make_unique< sisl::byte_array_impl >(read_blob.body->size);
        blob.object_off = read_blob.object_off;
        blob.user_key = read_blob.user_key;
        std::memcpy(blob.body->bytes, read_blob.body->bytes, blob.body->size);
        return blob;
    });
}

BlobManager::NullAsyncResult MockHomeObject::del(shard_id shard, blob_id const& blob) {
    if (!_has_shard(shard)) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);

    return folly::makeSemiFuture().deferValue([this, shard, blob](auto) mutable -> BlobManager::NullResult {
        auto lg = std::scoped_lock(_data_lock);
        LOGDEBUGMOD(homeobject, "Deleting blob {} in set of {}", blob, _in_memory_disk.size());
        if (0 < _in_memory_disk.erase(BlobRoute{shard, blob})) return folly::Unit();
        LOGWARNMOD(homeobject, "Blob missing {} during delete", blob);
        return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
    });
}

} // namespace homeobject
