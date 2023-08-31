#include "mock_homeobject.hpp"

namespace homeobject {

BlobManager::AsyncResult< blob_id > MockHomeObject::put(shard_id shard, Blob&& blob) {
    auto e = get_shard(shard);
    if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);

    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue(
            [this, shard_info = e.value(), blob = std::move(blob)](auto) mutable -> BlobManager::Result< blob_id > {
                // Simulate ::commit()
                auto new_pba = _in_memory_disk.end();
                blob_id new_id;
                {
                    // Should be lock-free append only stucture
                    auto lg = std::scoped_lock(_data_lock);
                    LOGDEBUGMOD(homeobject, "Writing Blob {}", _in_memory_disk.size());
                    new_id = _in_memory_disk.size();
                    new_pba = _in_memory_disk.insert(new_pba, std::move(blob));
                }
                {
                    auto lg = std::scoped_lock(_index_lock);
                    auto [_, happened] = _in_memory_index.try_emplace(BlobRoute{shard_info.id, new_id}, new_pba);
                    RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
                }
                return new_id;
            });
}

BlobManager::AsyncResult< MockHomeObject::pba > MockHomeObject::_get_pba(ShardInfo const& shard, blob_id id) const {
    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, route = BlobRoute(shard.id, id)](auto) mutable -> BlobManager::Result< pba > {
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
            if (!d_it->body) return folly::makeUnexpected(BlobError::DELETED_BLOB);
            return d_it;
        });
}

BlobManager::AsyncResult< Blob > MockHomeObject::get(shard_id shard, blob_id const& id, uint64_t, uint64_t) const {
    auto e = get_shard(shard);
    if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);

    return _get_pba(e.value(), id).deferValue([](auto const& e) -> BlobManager::Result< Blob > {
        if (!e) return folly::makeUnexpected(e.error());
        auto& read_blob = *(e.value());
        Blob blob;
        blob.body = std::make_unique< sisl::byte_array_impl >(read_blob.body->size);
        blob.object_off = read_blob.object_off;
        blob.user_key = read_blob.user_key;
        std::memcpy(blob.body->bytes, read_blob.body->bytes, blob.body->size);
        return blob;
    });
}

BlobManager::NullAsyncResult MockHomeObject::_del_pba(ShardInfo const& shard, blob_id id) {
    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, route = BlobRoute(shard.id, id)](auto) mutable -> BlobManager::NullResult {
            {
                auto lg = std::shared_lock(_index_lock);
                LOGDEBUGMOD(homeobject, "Looking up Blob {} in set of {}", route.blob, _in_memory_disk.size());
                if (auto it = _in_memory_index.find(route); _in_memory_index.end() != it) {
                    _in_memory_index.erase(it);
                    return folly::Unit();
                }
                return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
            }
        });
}

BlobManager::NullAsyncResult MockHomeObject::del(shard_id shard, blob_id const& id) {
    auto e = get_shard(shard);
    if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);

    return _del_pba(e.value(), id);
}

} // namespace homeobject
