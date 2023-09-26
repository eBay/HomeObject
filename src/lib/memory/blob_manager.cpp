#include "homeobject.hpp"

namespace homeobject {

#define WITH_SHARD                                                                                                     \
    auto index_it = index_.find(_shard.id);                                                                            \
    RELEASE_ASSERT(index_.end() != index_it, "Missing BTree!!");                                                       \
    auto& shard = *index_it->second;

#define WITH_ROUTE(blob)                                                                                               \
    auto const route = BlobRoute{_shard.id, (blob)};                                                                   \
    LOGTRACEMOD(homeobject, "[route={}]", route);

#define IF_BLOB_ALIVE                                                                                                  \
    if (auto blob_it = shard.btree_.find(route); shard.btree_.end() == blob_it || !blob_it->second) {                  \
        LOGWARNMOD(homeobject, "[route={}] missing", route);                                                           \
        return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);                                                         \
    } else

// Write (move) Blob to new BlobExt on heap and Insert BlobExt to Index
BlobManager::Result< blob_id > MemoryHomeObject::_put_blob(ShardInfo const& _shard, Blob&& _blob) {
    WITH_SHARD
    WITH_ROUTE(shard.shard_seq_num_++)

    auto [_, happened] =
        shard.btree_.try_emplace(route, BlobExt{.state_ = BlobState::ALIVE, .blob_ = new Blob(std::move(_blob))});
    RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
    return route.blob;
}

// Lookup BlobExt and duplicate underyling Blob for user; only *safe* because we defer GC.
BlobManager::Result< Blob > MemoryHomeObject::_get_blob(ShardInfo const& _shard, blob_id _blob) const {
    WITH_SHARD
    WITH_ROUTE(_blob)
    IF_BLOB_ALIVE { return blob_it->second.blob_->clone(); }
}

// Tombstone BlobExt entry
BlobManager::NullResult MemoryHomeObject::_del_blob(ShardInfo const& _shard, blob_id _blob) {
    WITH_SHARD
    WITH_ROUTE(_blob)
    IF_BLOB_ALIVE {
        auto del_blob = BlobExt();
        del_blob.blob_ = blob_it->second.blob_;
        shard.btree_.assign_if_equal(route, blob_it->second, std::move(del_blob));
        return folly::Unit();
    }
}

} // namespace homeobject
