#include "homeobject.hpp"

namespace homeobject {

Shard& MemoryHomeObject::_find_index(shard_id shard) {
    auto lg = std::shared_lock(_index_lock);
    auto index_it = _in_memory_index.find(shard);
    RELEASE_ASSERT(_in_memory_index.end() != index_it, "Missing BTree!!");
    return index_it->second;
}

BlobManager::Result< blob_id > MemoryHomeObject::_put_blob(ShardInfo const& shard, Blob&& blob) {
    // Lookup Shard Index (BTree and SSN)
    auto& our_shard = _find_index(shard.id);

    // Lock the BTree
    // TODO: Generate BlobID (with RAFT this will happen implicitly) and Route
    auto route = BlobRoute{shard.id, our_shard._shard_seq_num++};
    auto new_blob = BlobExt();
    new_blob._state = BlobState::ALIVE;
    new_blob._blob = std::make_shared< Blob >(std::move(blob));
    auto [new_it, happened] = our_shard._btree.try_emplace(route, std::move(new_blob));

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
    auto& our_shard = index_it->second;

    // Calculate BlobRoute from ShardInfo (use ordinal?)
    auto route = BlobRoute(shard.id, blob);
    LOGDEBUGMOD(homeobject, "Looking up Blob {}", route.blob);

    // This is only *safe* because we defer GC.
    if (auto v_blob = our_shard._btree.find(route); our_shard._btree.end() != v_blob && v_blob->second) {
        auto& ext_blob = v_blob->second;
        Blob user_blob;
        user_blob.object_off = ext_blob._blob->object_off;
        user_blob.user_key = ext_blob._blob->user_key;

        auto unsafe_ptr = ext_blob._blob->body.get();
        user_blob.body = std::make_unique< sisl::byte_array_impl >(unsafe_ptr->size);
        std::memcpy(user_blob.body->bytes, unsafe_ptr->bytes, user_blob.body->size);
        return user_blob;
    }
    LOGWARNMOD(homeobject, "Blob missing {} during get", route.blob);
    return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
}

BlobManager::NullResult MemoryHomeObject::_del_blob(ShardInfo const& shard, blob_id id) {
    // Lookup Shard Index (BTree and SSN)
    auto& our_shard = _find_index(shard.id);

    // Calculate BlobRoute from ShardInfo (use ordinal?)
    auto route = BlobRoute(shard.id, id);

    auto& our_btree = our_shard._btree;
    if (auto v_blob = our_shard._btree.find(route); our_shard._btree.end() != v_blob && v_blob->second) {
        auto del_blob = BlobExt();
        del_blob._blob = v_blob->second._blob;
        our_btree.assign_if_equal(route, v_blob->second, std::move(del_blob));
        return folly::Unit();
    }
    return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
}

} // namespace homeobject
