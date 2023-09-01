#include "homeobject_impl.hpp"

#include <boost/uuid/uuid_io.hpp>

namespace homeobject {

std::shared_ptr< BlobManager > HomeObjectImpl::blob_manager() {
    init_repl_svc();
    return shared_from_this();
}

std::shared_ptr< ShardManager > HomeObjectImpl::shard_manager() {
    init_repl_svc();
    return shared_from_this();
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::create_shard(pg_id pg_owner, uint64_t size_bytes) {
    if (0 == size_bytes || max_shard_size() < size_bytes) return folly::makeUnexpected(ShardError::INVALID_ARG);

    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, pg_owner, size_bytes](auto) mutable -> ShardManager::Result< ShardInfo > {
            return _create_shard(pg_owner, size_bytes);
        });
}

ShardManager::AsyncResult< InfoList > HomeObjectImpl::list_shards(pg_id pg) const {
    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, pg](auto) mutable -> ShardManager::Result< InfoList > { return _list_shards(pg); });
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::seal_shard(shard_id id) {
    return folly::makeSemiFuture()
        .via(folly::getGlobalCPUExecutor())
        .thenValue([this, id](auto) mutable -> ShardManager::Result< ShardInfo > { return _seal_shard(id); });
}

ShardManager::AsyncResult< ShardInfo > HomeObjectImpl::get_shard(shard_id id) const { return _get_shard(id); }

BlobManager::AsyncResult< Blob > HomeObjectImpl::get(shard_id shard, blob_id const& blob, uint64_t off,
                                                     uint64_t len) const {
    return _get_shard(shard).thenValue([this, blob](auto const e) -> BlobManager::Result< Blob > {
        if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
        return _get_blob(e.value().id, blob);
    });
}

BlobManager::AsyncResult< blob_id > HomeObjectImpl::put(shard_id shard, Blob&& blob) {
    return _get_shard(shard).thenValue(
        [this, blob = std::move(blob)](auto const e) mutable -> BlobManager::Result< blob_id > {
            if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
            return _put_blob(e.value().id, std::move(blob));
        });
}

BlobManager::NullAsyncResult HomeObjectImpl::del(shard_id shard, blob_id const& blob) {
    return _get_shard(shard).thenValue([this, blob](auto const e) mutable -> BlobManager::NullResult {
        if (!e) return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
        return _del_blob(e.value().id, blob);
    });
}

} // namespace homeobject
