#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< BlobManager > HomeObjectImpl::blob_manager() { return shared_from_this(); }

folly::Future< std::variant< blob_id, BlobError > > HomeObjectImpl::put(shard_id shard, Blob&&) {
    return folly::makeFuture(BlobError::UNKNOWN_SHARD);
}

folly::Future< std::variant< Blob, BlobError > > HomeObjectImpl::get(shard_id shard, blob_id const& blob, uint64_t off,
                                                                     uint64_t len) const {
    return folly::makeFuture(BlobError::UNKNOWN_SHARD);
}

folly::Future< BlobError > HomeObjectImpl::del(shard_id shard, blob_id const& blob) {
    return folly::makeFuture(BlobError::UNKNOWN_SHARD);
}

} // namespace homeobject
