#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< BlobManager > HomeObjectImpl::blob_manager() {
    init_repl_svc();
    return shared_from_this();
}

BlobManager::AsyncResult< blob_id > HomeObjectImpl::put(shard_id shard, Blob&&) {
    return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
}

BlobManager::AsyncResult< Blob > HomeObjectImpl::get(shard_id shard, blob_id const& blob, uint64_t off,
                                                     uint64_t len) const {
    return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
}

BlobManager::NullAsyncResult HomeObjectImpl::del(shard_id shard, blob_id const& blob) {
    return folly::makeUnexpected(BlobError::UNKNOWN_SHARD);
}

} // namespace homeobject
