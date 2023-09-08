#include "homeobject.hpp"

namespace homeobject {

BlobManager::Result< MemoryHomeObject::index_svc::iterator > MemoryHomeObject::_find_index(shard_id shard) {
    auto lg = std::shared_lock(_index_lock);
    if (auto index_it = _in_memory_index.find(shard); _in_memory_index.end() == index_it)
        return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
    else
        return index_it;
}

BlobManager::Result< blob_id > MemoryHomeObject::_put_blob(ShardInfo const& shard, Blob&& blob) {
    auto e = _find_index(shard.id);
    if (!e) return folly::makeUnexpected(e.error());
    auto index_it = e.value();

    auto route = BlobRoute{shard.id, 0ull};
    auto bt_lg = std::scoped_lock(index_it->second._btree_lock);
    route.blob = index_it->second._shard_seq_num++;
    auto [new_it, happened] = index_it->second._btree.try_emplace(route, std::move(blob));
    RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");

    LOGDEBUGMOD(homeobject, "Wrote BLOB {} to: BlkId:[{}]", route.blob, fmt::ptr(&new_it->second));
    return route.blob;
}

BlobManager::Result< Blob > MemoryHomeObject::_get_blob(ShardInfo const& shard, blob_id blob) const {
    auto index_it = _in_memory_index.cend();
    {
        auto lg = std::shared_lock(_index_lock);
        if (index_it = _in_memory_index.find(shard.id); _in_memory_index.cend() == index_it)
            return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
    }

    auto route = BlobRoute(shard.id, blob);
    Blob user_blob;
    // This is only *safe* because we defer GC to shutdown currently.
    auto unsafe_ptr = decltype(Blob::body)::pointer{nullptr};
    {
        auto bt_lg = std::shared_lock(index_it->second._btree_lock);
        LOGDEBUGMOD(homeobject, "Looking up Blob {} in set of {}", route.blob, index_it->second._shard_seq_num);
        if (auto it = index_it->second._btree.find(route); index_it->second._btree.end() != it) {
            user_blob.object_off = it->second.object_off;
            user_blob.user_key = it->second.user_key;
            unsafe_ptr = it->second.body.get();
        }
    }
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
    auto e = _find_index(shard.id);
    if (!e) return folly::makeUnexpected(e.error());
    auto index_it = e.value();

    auto route = BlobRoute(shard.id, id);
    // TODO We defer GC of the BLOB leaking BLOB into memory for now
    auto bt_lg = std::scoped_lock(index_it->second._btree_lock);
    auto& our_btree = index_it->second._btree;
    if (auto r_it = our_btree.find(route); our_btree.end() != r_it) {
        _garbage.push_back(std::move(r_it->second));
        our_btree.erase(r_it);
        return folly::Unit();
    }
    LOGWARNMOD(homeobject, "Blob missing {} during delete", route.blob);
    return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
}

} // namespace homeobject
