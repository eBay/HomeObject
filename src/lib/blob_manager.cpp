#include "homeobject_impl.hpp"

namespace homeobject {

std::shared_ptr< BlobManager > HomeObjectImpl::blob_manager() { return shared_from_this(); }

void HomeObjectImpl::put(shard_id shard, Blob&&, BlobManager::id_cb const& cb) {
    cb(BlobError::UNKNOWN_SHARD, std::nullopt);
}

void HomeObjectImpl::get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len,
                         BlobManager::get_cb const& cb) const {
    cb(BlobError::UNKNOWN_SHARD, std::nullopt);
}

void HomeObjectImpl::del(shard_id shard, blob_id const& blob, BlobManager::ok_cb const& cb) {
    cb(BlobError::UNKNOWN_SHARD, std::nullopt);
}

} // namespace homeobject
