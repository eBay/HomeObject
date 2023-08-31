#include "mock_homeobject.hpp"

namespace homeobject {

BlobManager::AsyncResult< blob_id > MockHomeObject::put(shard_id shard, Blob&& blob) {
    auto e = get_shard(shard);
    if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);

    auto [p, sf] = folly::makePromiseContract< BlobManager::Result< blob_id > >();
    std::thread([this, shard_info = e.value(), blob = std::move(blob), p = std::move(p)]() mutable {
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
        p.setValue(BlobManager::Result< blob_id >(new_id));
    }).detach();
    return sf;
}

BlobManager::AsyncResult< MockHomeObject::pba > MockHomeObject::_get_route(shard_id shard, blob_id blob) const {
    auto e = get_shard(shard);
    if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);

    // Simulate Fiber
    auto [p, sf] = folly::makePromiseContract< BlobManager::Result< pba > >();
    std::thread([this, route = BlobRoute(e.value().id, blob), p = std::move(p)]() mutable {
        auto d_it = _in_memory_disk.end();
        {
            auto lg = std::shared_lock(_index_lock);
            LOGDEBUGMOD(homeobject, "Looking up Blob {} in set of {}", route.blob, _in_memory_disk.size());
            if (auto it = _in_memory_index.find(route); _in_memory_index.end() != it) d_it = it->second;
        }
        if (_in_memory_disk.end() == d_it) {
            LOGWARNMOD(homeobject, "Blob missing {} during get", route.blob);
            p.setValue(folly::makeUnexpected(BlobError::UNKNOWN_BLOB));
        } else
            p.setValue(d_it);
    }).detach();
    return sf;
}

BlobManager::AsyncResult< Blob > MockHomeObject::get(shard_id shard, blob_id const& id, uint64_t, uint64_t) const {
    return _get_route(shard, id).deferValue([](auto const& e) -> BlobManager::Result< Blob > {
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

BlobManager::NullAsyncResult MockHomeObject::del(shard_id shard, blob_id const& id) {
    return _get_route(shard, id).deferValue([this, id](auto const& e) mutable -> BlobManager::NullResult {
        if (!e) return folly::makeUnexpected(e.error());
        auto del_blob = e.value();

        // Just free space, don't erase or we need a proper counter for put, requires safe trick with erase:
        LOGDEBUGMOD(homeobject, "Free'ing blob {}", id);
        _in_memory_disk.erase(del_blob, del_blob)->body.reset();
        return folly::Unit();
    });
}

} // namespace homeobject
