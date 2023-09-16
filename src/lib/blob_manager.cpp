#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< BlobManager > HomeObjectImpl::blob_manager() {
    init_repl_svc();
    return shared_from_this();
}

BlobManager::AsyncResult< Blob > HomeObjectImpl::get(shard_id shard, blob_id const& blob, uint64_t off,
                                                     uint64_t len) const {
    return _get_shard(shard).thenValue([this, blob](auto const e) -> BlobManager::Result< Blob > {
        if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
        return _get_blob(e.value().info, blob);
    });
}

BlobManager::AsyncResult< blob_id > HomeObjectImpl::put(shard_id shard, Blob&& blob) {
    return _get_shard(shard).thenValue(
        [this, blob = std::move(blob)](auto const e) mutable -> BlobManager::Result< blob_id > {
            if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
            if (ShardInfo::State::SEALED == e.value().info.state) return folly::makeUnexpected(BlobError::INVALID_ARG);
            return _put_blob(e.value().info, std::move(blob));
        });
}

BlobManager::NullAsyncResult HomeObjectImpl::del(shard_id shard, blob_id const& blob) {
    return _get_shard(shard).thenValue([this, blob](auto const e) mutable -> BlobManager::NullResult {
        if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
        return _del_blob(e.value().info, blob);
    });
}

Blob Blob::clone() const {
    auto new_body = sisl::io_blob_safe(body.size);
    std::memcpy(new_body.bytes, body.bytes, body.size);
    return Blob(std::move(new_body), user_key, object_off);
}

} // namespace homeobject
