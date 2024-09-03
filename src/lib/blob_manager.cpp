#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< BlobManager > HomeObjectImpl::blob_manager() { return shared_from_this(); }

BlobManager::AsyncResult< Blob > HomeObjectImpl::get(shard_id_t shard, blob_id_t const& blob_id, uint64_t off,
                                                     uint64_t len) const {
    return _get_shard(shard).thenValue([this, blob_id, off, len](auto const e) -> BlobManager::AsyncResult< Blob > {
        if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
        return _get_blob(e.value(), blob_id, off, len);
    });
}

BlobManager::AsyncResult< PutBlobRes > HomeObjectImpl::put(shard_id_t shard, Blob&& blob) {
    return _get_shard(shard).thenValue(
        [this, blob = std::move(blob)](auto const e) mutable -> BlobManager::AsyncResult< PutBlobRes > {
            if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
            if (ShardInfo::State::SEALED == e.value().state) return folly::makeUnexpected(BlobError::SEALED_SHARD);
            if (blob.body.size() == 0) return folly::makeUnexpected(BlobError::INVALID_ARG);
            return _put_blob(e.value(), std::move(blob));
        });
}

BlobManager::AsyncResult< DelBlobRes > HomeObjectImpl::del(shard_id_t shard, blob_id_t const& blob) {
    return _get_shard(shard).thenValue([this, blob](auto const e) mutable -> BlobManager::AsyncResult< DelBlobRes > {
        if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
        return _del_blob(e.value(), blob);
    });
}

Blob Blob::clone() const {
    auto new_body = sisl::io_blob_safe(body.size());
    std::memcpy(new_body.bytes(), body.cbytes(), body.size());
    return Blob(std::move(new_body), user_key, object_off);
}

} // namespace homeobject
