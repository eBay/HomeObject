#include "homeobject.hpp"

namespace homeobject {

BlobManager::Result< blob_id > MemoryHomeObject::_put_blob(ShardInfo const& shard, Blob&& blob) {
    auto lg = std::scoped_lock(_index_lock);
    auto [btree_it, h1] = _in_memory_index.try_emplace(shard.id, btree());
    RELEASE_ASSERT(_in_memory_index.end() != btree_it, "Could not create BTree!");

    auto route = BlobRoute{shard.id, btree_it->second.size()};
    LOGDEBUGMOD(homeobject, "Writing BLOB {} to: BlkId:[{}]", route.blob, fmt::ptr(&blob));

    auto [_, happened] = btree_it->second.try_emplace(route, std::move(blob));
    RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");

    return route.blob;
}

BlobManager::Result< Blob > MemoryHomeObject::_get_blob(ShardInfo const& shard, blob_id blob) const {
    auto route = BlobRoute(shard.id, blob);
    // This is only *safe* because we defer GC to shutdown currently.
    auto unsafe_ptr = decltype(Blob::body)::pointer{nullptr};
    Blob user_blob;
    {
        auto lg = std::shared_lock(_index_lock);
        if (auto b_it = _in_memory_index.find(shard.id); _in_memory_index.end() != b_it) {
            LOGDEBUGMOD(homeobject, "Looking up Blob {} in set of {}", route.blob, b_it->second.size());
            if (auto it = b_it->second.find(route); b_it->second.end() != it) {
                user_blob.object_off = it->second.object_off;
                user_blob.user_key = it->second.user_key;
                unsafe_ptr = it->second.body.get();
            }
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
    auto route = BlobRoute(shard.id, id);
    auto lg = std::scoped_lock(_index_lock);
    LOGDEBUGMOD(homeobject, "Looking up Blob {} in set of {}", route.blob, _in_memory_index.size());
    // TODO We defer GC of the BLOB leaking BLOB into memory for now
    if (auto b_it = _in_memory_index.find(shard.id); _in_memory_index.end() != b_it) {
        auto& our_btree = b_it->second;
        if (auto r_it = our_btree.find(route); our_btree.end() != r_it) {
            _garbage.push_back(std::move(r_it->second));
            our_btree.erase(r_it);
            return folly::Unit();
        }
    }
    LOGWARNMOD(homeobject, "Blob missing {} during delete", route.blob);
    return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
}

} // namespace homeobject
