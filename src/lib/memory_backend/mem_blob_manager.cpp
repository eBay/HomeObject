#include "mem_homeobject.hpp"

namespace homeobject {

#define WITH_SHARD                                                                                                     \
    auto index_it = index_.find(_shard.id);                                                                            \
    RELEASE_ASSERT(index_.end() != index_it, "Missing BTree!!");                                                       \
    auto& shard = *index_it->second;

#define WITH_ROUTE(blob)                                                                                               \
    auto const route = BlobRoute{_shard.id, (blob)};                                                                   \
    LOGT("[route={}]", route);

#define IF_BLOB_ALIVE                                                                                                  \
    if (auto blob_it = shard.btree_.find(route); shard.btree_.end() == blob_it || !blob_it->second) {                  \
        LOGD("[route={}] missing", route);                                                                             \
    } else

// Write (move) Blob to new BlobExt on heap and Insert BlobExt to Index
BlobManager::AsyncResult< blob_id_t > MemoryHomeObject::_put_blob(ShardInfo const& _shard, Blob&& _blob,
                                                                  trace_id_t tid) {
    (void)tid;
    WITH_SHARD
    blob_id_t new_blob_id;
    {
        auto lg = std::shared_lock(_pg_lock);
        auto iter = _pg_map.find(_shard.placement_group);
        RELEASE_ASSERT(iter != _pg_map.end(), "PG not found");
        iter->second->durable_entities_update(
            [&new_blob_id](auto& de) { new_blob_id = de.blob_sequence_num.fetch_add(1, std::memory_order_relaxed); });
    }
    WITH_ROUTE(new_blob_id);

    auto [_, happened] =
        shard.btree_.try_emplace(route, BlobExt{.state_ = BlobState::ALIVE, .blob_ = new Blob(std::move(_blob))});
    RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
    return route.blob;
}

// Lookup BlobExt and duplicate underyling Blob for user; only *safe* because we defer GC.
BlobManager::AsyncResult< Blob > MemoryHomeObject::_get_blob(ShardInfo const& _shard, blob_id_t _blob, uint64_t off,
                                                             uint64_t len, bool allow_skip_verify, trace_id_t tid) const {
    (void)off;
    (void)len;
    (void)allow_skip_verify;
    (void)tid;
    WITH_SHARD
    WITH_ROUTE(_blob)
    IF_BLOB_ALIVE { return blob_it->second.blob_->clone(); }
    return folly::makeUnexpected(BlobError(BlobErrorCode::UNKNOWN_BLOB));
}

// Tombstone BlobExt entry
BlobManager::NullAsyncResult MemoryHomeObject::_del_blob(ShardInfo const& _shard, blob_id_t _blob, trace_id_t tid) {
    (void)tid;
    WITH_SHARD
    WITH_ROUTE(_blob)
    IF_BLOB_ALIVE {
        shard.btree_.assign_if_equal(route, blob_it->second,
                                     BlobExt{.state_ = BlobState::DELETED, .blob_ = blob_it->second.blob_});
    }
    return folly::Unit();
}

} // namespace homeobject
