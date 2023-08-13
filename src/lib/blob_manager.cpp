#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< BlobManager > HomeObjectImpl::blob_manager() { return shared_from_this(); }

folly::SemiFuture< std::variant< blob_id, BlobError > > HomeObjectImpl::put(shard_id shard, Blob&&) {
    return folly::makeSemiFuture(std::variant< blob_id, BlobError >(BlobError::UNKNOWN_SHARD));
}

folly::SemiFuture< std::variant< Blob, BlobError > > HomeObjectImpl::get(shard_id shard, blob_id const& blob,
                                                                         uint64_t off, uint64_t len) const {
    return folly::makeSemiFuture(std::variant< Blob, BlobError >(BlobError::UNKNOWN_SHARD));
}

folly::SemiFuture< BlobError > HomeObjectImpl::del(shard_id shard, blob_id const& blob) {
    return folly::makeSemiFuture(BlobError::UNKNOWN_SHARD);
}

} // namespace homeobject
