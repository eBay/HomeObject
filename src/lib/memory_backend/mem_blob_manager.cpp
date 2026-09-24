#include "mem_homeobject.hpp"

namespace homeobject {

// Write (move) Blob to new BlobExt on heap and Insert BlobExt to Index
BlobManager::AsyncResult< blob_id_t > MemoryHomeObject::_put_blob(ShardInfo const& _shard, Blob&& _blob,
                                                                  trace_id_t tid) {
    (void)tid;
    blob_id_t new_blob_id;
    {
        auto lg = std::shared_lock(_pg_lock);
        auto iter = _pg_map.find(_shard.placement_group);
        RELEASE_ASSERT(iter != _pg_map.end(), "PG not found");
        iter->second->durable_entities_update(
            [&new_blob_id](auto& de) { new_blob_id = de.blob_sequence_num.fetch_add(1, std::memory_order_relaxed); });
    }
    auto const route = BlobRoute{_shard.id, new_blob_id};
    LOGT("[route={}]", route);

    bool happened = false;
    bool found = index_.visit(_shard.id, [&](auto& p) {
        happened = p.second->btree_.try_emplace(
            route, BlobExt{.state_ = BlobState::ALIVE, .blob_ = new Blob(std::move(_blob))});
    });
    RELEASE_ASSERT(found, "Missing BTree!!");
    RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
    co_return route.blob;
}

// Lookup BlobExt and duplicate underyling Blob for user; only *safe* because we defer GC.
BlobManager::AsyncResult< Blob > MemoryHomeObject::_get_blob(ShardInfo const& _shard, blob_id_t _blob, uint64_t off,
                                                             uint64_t len, bool allow_skip_verify,
                                                             trace_id_t tid) const {
    (void)off;
    (void)len;
    (void)allow_skip_verify;
    (void)tid;
    Blob result;
    bool alive = false;
    auto const route = BlobRoute{_shard.id, _blob};
    LOGT("[route={}]", route);

    index_.cvisit(_shard.id, [&](auto const& p) {
        p.second->btree_.cvisit(route, [&](auto const& bp) {
            if (bp.second) {
                result = bp.second.blob_->clone();
                alive = true;
            } else {
                LOGD("[route={}] missing", route);
            }
        });
    });
    if (alive) co_return result;
    LOGD("[route={}] missing", route);
    co_return std::unexpected(BlobError(BlobErrorCode::UNKNOWN_BLOB));
}

// Tombstone BlobExt entry
BlobManager::NullAsyncResult MemoryHomeObject::_del_blob(ShardInfo const& _shard, blob_id_t _blob, trace_id_t tid) {
    (void)tid;
    auto const route = BlobRoute{_shard.id, _blob};
    LOGT("[route={}]", route);

    index_.cvisit(_shard.id, [&](auto const& p) {
        p.second->btree_.visit(route, [&](auto& bp) {
            if (bp.second) { bp.second.state_ = BlobState::DELETED; }
        });
    });
    co_return std::monostate{};
}

} // namespace homeobject
