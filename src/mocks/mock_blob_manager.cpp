#include "mock_homeobject.hpp"

namespace homeobject {

BlobManager::AsyncResult< blob_id > MockHomeObject::put(shard_id shard, Blob&& blob) {
    auto e = get_shard(shard);
    if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);

    auto [p, sf] = folly::makePromiseContract< BlobManager::Result< blob_id > >();
    std::thread([this, shard_info = std::move(e.value()), blob = std::move(blob), p = std::move(p)]() mutable {
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

BlobManager::Result< MockHomeObject::pba > MockHomeObject::_index_get(BlobRoute const& r) const {
    auto d_it = _in_memory_disk.end();
    {
        auto lg = std::shared_lock(_index_lock);
        LOGDEBUGMOD(homeobject, "Looking up Blob {} in set of {}", r.blob, _in_memory_disk.size());
        if (auto it = _in_memory_index.find(r); _in_memory_index.end() != it) d_it = it->second;
    }
    if (_in_memory_disk.end() == d_it) {
        LOGWARNMOD(homeobject, "Blob missing {} during get", r.blob);
        return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);
    }
    return d_it;
}

BlobManager::AsyncResult< Blob > MockHomeObject::get(shard_id shard, blob_id const& id, uint64_t, uint64_t) const {
    auto e = get_shard(shard);
    if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
    auto& shard_info = e.value();

    auto de = _index_get(BlobRoute{shard_info.id, id});
    if (!de) return folly::makeUnexpected(de.error());

    auto const& read_blob = *(de.value());

    Blob blob;
    blob.body = std::make_unique< sisl::byte_array_impl >(read_blob.body->size);
    blob.object_off = read_blob.object_off;
    blob.user_key = read_blob.user_key;
    std::memcpy(blob.body->bytes, read_blob.body->bytes, blob.body->size);
    return blob;
}

BlobManager::NullAsyncResult MockHomeObject::del(shard_id shard, blob_id const& id) {
    auto e = get_shard(shard);
    if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
    auto& shard_info = e.value();

    auto de = _index_get(BlobRoute{shard_info.id, id});
    if (!de) return folly::makeUnexpected(de.error());

    auto d_it = de.value();

    // Just free space, don't erase or we need a proper counter for put, requires safe trick with erase:
    LOGDEBUGMOD(homeobject, "Free'ing blob {}", id);
    _in_memory_disk.erase(d_it, d_it)->body.reset();
    return folly::Unit();
}

} // namespace homeobject
