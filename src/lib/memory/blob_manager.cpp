#include "homeobject.hpp"

namespace homeobject {

ShardIndex& MemoryHomeObject::_find_index(shard_id shard) const {
    auto index_it = _in_memory_index.find(shard);
    RELEASE_ASSERT(_in_memory_index.end() != index_it, "Missing BTree!!");
    return *index_it->second;
}

BlobManager::Result< blob_id > MemoryHomeObject::_put_blob(ShardInfo const& shard, Blob&& blob) {
    // Lookup Shard Index (BTree and SSN)
    auto& our_shard = _find_index(shard.id);

    // Generate BlobID (with RAFT this will happen implicitly) and Route
    auto const route = BlobRoute{shard.id, our_shard._shard_seq_num++};
    if ((UINT64_MAX >> 16) < route.blob) { return folly::makeUnexpected(BlobError::UNKNOWN); }
    LOGTRACEMOD(homeobject, "Writing Blob {}", route);

    // Write (move) Blob to Heap
    auto new_blob = BlobExt();
    new_blob._state = BlobState::ALIVE;
    new_blob._blob = new Blob(std::move(blob));
    LOGDEBUGMOD(homeobject, "Wrote BLOB {} to: BlkId:[{}]", route, fmt::ptr(new_blob._blob));

    // Insert BlobExt to Index
    auto [_, happened] = our_shard._btree.try_emplace(route, std::move(new_blob));
    RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
    return route.blob;
}

BlobManager::Result< Blob > MemoryHomeObject::_get_blob(ShardInfo const& shard, blob_id blob) const {
    // Lookup Shard Index (BTree and SSN)
    auto& our_shard = _find_index(shard.id);

    // Find BlobExt by BlobRoute
    auto const route = BlobRoute(shard.id, blob);

    // Calculate BlobRoute from ShardInfo (use ordinal?)
    LOGTRACEMOD(homeobject, "Looking up Blob {}", route);
    auto blob_it = our_shard._btree.find(route);
    if (our_shard._btree.end() == blob_it || !blob_it->second) {
        LOGWARNMOD(homeobject, "Blob [{}] missing during get", route);
        return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
    }

    // Duplicate underyling Blob for user
    // This is only *safe* because we defer GC.
    auto& ext_blob = blob_it->second;
    RELEASE_ASSERT(ext_blob._blob != nullptr, "Blob Deleted!");
    RELEASE_ASSERT(!!ext_blob._blob->body, "BlobBody Deleted!");
    auto unsafe_ptr = ext_blob._blob->body.get();

    Blob user_blob;
    user_blob.object_off = ext_blob._blob->object_off;
    user_blob.user_key = ext_blob._blob->user_key;
    user_blob.body = std::make_unique< sisl::byte_array_impl >(unsafe_ptr->size);
    std::memcpy(user_blob.body->bytes, unsafe_ptr->bytes, user_blob.body->size);
    return user_blob;
}

BlobManager::NullResult MemoryHomeObject::_del_blob(ShardInfo const& shard, blob_id id) {
    // Lookup Shard Index (BTree and SSN)
    auto& our_shard = _find_index(shard.id);

    // Calculate BlobRoute from ShardInfo (use ordinal?)
    auto const route = BlobRoute(shard.id, id);

    // Tombstone BlobExt entry
    LOGTRACEMOD(homeobject, "Tombstoning Blob {}", route);
    if (auto blob_it = our_shard._btree.find(route); our_shard._btree.end() != blob_it && blob_it->second) {
        auto del_blob = BlobExt();
        del_blob._blob = blob_it->second._blob;
        our_shard._btree.assign_if_equal(route, blob_it->second, std::move(del_blob));
        return folly::Unit();
    }
    LOGWARNMOD(homeobject, "Blob [{}] missing during del", route);
    return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
}

} // namespace homeobject
