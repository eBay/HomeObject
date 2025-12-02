#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< BlobManager > HomeObjectImpl::blob_manager() { return shared_from_this(); }

BlobManager::AsyncResult< Blob > HomeObjectImpl::get(shard_id_t shard, blob_id_t const& blob_id, uint64_t off,
                                                     uint64_t len, bool allow_skip_verify, trace_id_t tid) const {
    return _get_shard(shard, tid)
        .thenValue([this, blob_id, off, len, allow_skip_verify, tid](auto const e) -> BlobManager::AsyncResult< Blob > {
            if (!e) return folly::makeUnexpected(BlobError(BlobErrorCode::UNKNOWN_SHARD));
            return _get_blob(e.value(), blob_id, off, len, allow_skip_verify, tid);
        });
}

BlobManager::AsyncResult< blob_id_t > HomeObjectImpl::put(shard_id_t shard, Blob&& blob, trace_id_t tid) {
    return _get_shard(shard, tid)
        .thenValue([this, blob = std::move(blob), tid](auto const e) mutable -> BlobManager::AsyncResult< blob_id_t > {
            if (!e) return folly::makeUnexpected(BlobError(BlobErrorCode::UNKNOWN_SHARD));
            if (ShardInfo::State::SEALED == e.value().state)
                return folly::makeUnexpected(BlobError(BlobErrorCode::SEALED_SHARD));
            if (blob.body.size() == 0) return folly::makeUnexpected(BlobError(BlobErrorCode::INVALID_ARG));
            return _put_blob(e.value(), std::move(blob), tid);
        });
}

BlobManager::NullAsyncResult HomeObjectImpl::del(shard_id_t shard, blob_id_t const& blob, trace_id_t tid) {
    return _get_shard(shard, tid).thenValue([this, blob, tid](auto const e) mutable -> BlobManager::NullAsyncResult {
        if (!e) return folly::makeUnexpected(BlobError(BlobErrorCode::UNKNOWN_SHARD));
        return _del_blob(e.value(), blob, tid);
    });
}

Blob Blob::clone() const {
    auto new_body = sisl::io_blob_safe(body.size());
    std::memcpy(new_body.bytes(), body.cbytes(), body.size());
    return Blob(std::move(new_body), user_key, object_off);
}

} // namespace homeobject
