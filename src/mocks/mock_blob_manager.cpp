#include "mock_homeobject.hpp"

namespace homeobject {

BlobManager::AsyncResult< ShardInfo > MockHomeObject::_lookup_shard(shard_id shard) const {
    return folly::makeSemiFuture().defer([this, shard](auto) -> BlobManager::AsyncResult< ShardInfo > {
        if (auto e = get_shard(shard); e) return e.value();
        return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
    });
}

BlobManager::AsyncResult< blob_id > MockHomeObject::put(shard_id shard, Blob&& blob) {
    return _lookup_shard(shard).deferValue([this, inner_blob = std::move(blob)](
                                               auto&& e) mutable -> BlobManager::AsyncResult< blob_id > {
        if (!e) return folly::makeUnexpected(e.error());

        auto [p, sf] = folly::makePromiseContract< BlobManager::Result< blob_id > >();
        std::thread([this, shard_info = std::move(e.value()), tblob = std::move(inner_blob),
                     p = std::move(p)]() mutable {
            blob_id new_id;
            { // Simulate ::commit()
                auto lg = std::scoped_lock(_data_lock);
                new_id = _cur_blob_id++;
                LOGDEBUGMOD(homeobject, "Writing Blob {} in set of {}", new_id, _in_memory_disk.size());
                auto [_, happened] = _in_memory_disk.try_emplace(BlobRoute{shard_info.id, new_id}, std::move(tblob));
                RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
            }
            p.setValue(BlobManager::Result< blob_id >(new_id));
        }).detach();
        return sf;
    });
}

BlobManager::AsyncResult< Blob > MockHomeObject::get(shard_id shard, blob_id const& id, uint64_t, uint64_t) const {
    return _lookup_shard(shard).deferValue([this, id](auto const& e) mutable -> BlobManager::Result< Blob > {
        if (!e) return folly::makeUnexpected(e.error());
        auto& shard_info = e.value();
        Blob blob;
        auto lg = std::shared_lock(_data_lock);
        LOGDEBUGMOD(homeobject, "Looking up Blob {} in set of {}", id, _in_memory_disk.size());
        auto it = _in_memory_disk.find(BlobRoute{shard_info.id, id});
        if (it == _in_memory_disk.end()) {
            LOGWARNMOD(homeobject, "Blob missing {} during get", id);
            return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
        }

        LOGTRACEMOD(homeobject, "Reading BLOB data for {}", id);
        auto const& read_blob = it->second;
        blob.body = std::make_unique< sisl::byte_array_impl >(read_blob.body->size);
        blob.object_off = read_blob.object_off;
        blob.user_key = read_blob.user_key;
        std::memcpy(blob.body->bytes, read_blob.body->bytes, blob.body->size);
        return blob;
    });
}

BlobManager::NullAsyncResult MockHomeObject::del(shard_id shard, blob_id const& blob) {
    return _lookup_shard(shard).deferValue([this, blob](auto const& e) mutable -> BlobManager::NullResult {
        if (!e) return folly::makeUnexpected(e.error());
        auto& shard_info = e.value();
        auto lg = std::scoped_lock(_data_lock);
        LOGDEBUGMOD(homeobject, "Deleting blob {} in set of {}", blob, _in_memory_disk.size());
        if (0 < _in_memory_disk.erase(BlobRoute{shard_info.id, blob})) return folly::Unit();
        LOGWARNMOD(homeobject, "Blob missing {} during delete", blob);
        return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
    });
}

} // namespace homeobject
