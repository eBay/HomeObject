#include "homeobject.hpp"

namespace homeobject {

BlobManager::Result< blob_id > MemoryHomeObject::_put_blob(ShardInfo const& shard, Blob&& blob) {
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
        auto [_, happened] = _in_memory_index.try_emplace(BlobRoute{shard.id, new_id}, new_blkid);
        RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
    }
    return new_id;
}

BlobManager::Result< Blob > MemoryHomeObject::_get_blob(ShardInfo const& shard, blob_id blob) const {
    auto route = BlobRoute(shard.id, blob);
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
    Blob user_blob;
    user_blob.body = std::make_unique< sisl::byte_array_impl >(read_blob.body->size);
    user_blob.object_off = read_blob.object_off;
    user_blob.user_key = read_blob.user_key;
    std::memcpy(user_blob.body->bytes, read_blob.body->bytes, user_blob.body->size);
    return user_blob;
}

BlobManager::NullResult MemoryHomeObject::_del_blob(ShardInfo const& shard, blob_id id) {
    auto route = BlobRoute(shard.id, id);
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
}

} // namespace homeobject
