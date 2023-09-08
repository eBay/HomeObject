#include "homeobject.hpp"

namespace homeobject {

MemoryHomeObject::index_svc::iterator MemoryHomeObject::_find_index(shard_id shard) {
    auto lg = std::shared_lock(_index_lock);
    auto index_it = _in_memory_index.find(shard);
    RELEASE_ASSERT(_in_memory_index.end() != index_it, "Missing BTree!!");
    return index_it;
}

BlobManager::Result< blob_id > MemoryHomeObject::_put_blob(ShardInfo const& shard, Blob&& blob) {
    // Lookup Shard Index (BTree and SSN)
    auto& shard_index = _find_index(shard.id)->second;

    // Lock the BTree
    shard_index._btree_lock.lock();
    // TODO: Generate BlobID (with RAFT this will happen implicitly) and Route
    auto route = BlobRoute{shard.id, shard_index._shard_seq_num++};
    auto [new_it, happened] = shard_index._btree.try_emplace(route, std::move(blob));
    shard_index._btree_lock.unlock();

    RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
    LOGDEBUGMOD(homeobject, "Wrote BLOB {} to: BlkId:[{}]", route.blob, fmt::ptr(&new_it->second));
    return route.blob;
}

BlobManager::Result< Blob > MemoryHomeObject::_get_blob(ShardInfo const& shard, blob_id blob) const {
    // Lookup Shard Index (BTree and SSN)
    auto index_it = _in_memory_index.cend();
    {
        auto lg = std::shared_lock(_index_lock);
        if (index_it = _in_memory_index.find(shard.id); _in_memory_index.cend() == index_it)
            return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
    }
    auto& shard_index = index_it->second;

    // Calculate BlobRoute from ShardInfo (use ordinal?)
    auto route = BlobRoute(shard.id, blob);
    LOGDEBUGMOD(homeobject, "Looking up Blob {}", route.blob);

    // Lock the BTree *SHARED* and find the BLOB location in memory, read the BLOB into our
    // return value except body which we'll do outside the lock (may be HUGE and guaranteed to not move)
    // This is only *safe* because we defer GC to shutdown currently.
    Blob user_blob;
    auto unsafe_ptr = decltype(Blob::body)::pointer{nullptr};
    shard_index._btree_lock.lock_shared();
    if (auto it = shard_index._btree.find(route); shard_index._btree.end() != it) {
        user_blob.object_off = it->second.object_off;
        user_blob.user_key = it->second.user_key;
        unsafe_ptr = it->second.body.get();
    }
    shard_index._btree_lock.unlock_shared();

    if (!unsafe_ptr) {
        LOGWARNMOD(homeobject, "Blob missing {} during get", route.blob);
        return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
    }
    /// Make copy outside lock
    user_blob.body = std::make_unique< sisl::byte_array_impl >(unsafe_ptr->size);
    std::memcpy(user_blob.body->bytes, unsafe_ptr->bytes, user_blob.body->size);
    return user_blob;
}

BlobManager::NullResult MemoryHomeObject::_del_blob(ShardInfo const& shard, blob_id id) {
    // Lookup Shard Index (BTree and SSN)
    auto& shard_index = _find_index(shard.id)->second;

    // Calculate BlobRoute from ShardInfo (use ordinal?)
    auto route = BlobRoute(shard.id, id);

    // Lock the BTree *OWNED* and find the BLOB location. Move the index value to *garbage*, to be dealt
    // with later and remove route from Index.
    auto result = BlobManager::NullResult(folly::makeUnexpected(BlobError::UNKNOWN_BLOB));
    shard_index._btree_lock.lock();
    auto& our_btree = shard_index._btree;
    if (auto r_it = our_btree.find(route); our_btree.end() != r_it) {
        result = folly::Unit();
        _garbage.push_back(std::move(r_it->second));
        our_btree.erase(r_it);
    }
    shard_index._btree_lock.unlock();

    if (!result) LOGWARNMOD(homeobject, "Blob missing {} during delete", route.blob);
    return result;
}

} // namespace homeobject
