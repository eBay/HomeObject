#include "homeobject.hpp"

namespace homeobject {

BlobManager::Result< blob_id > HSHomeObject::_put_blob(shard_id, Blob&&) {
    return folly::makeUnexpected(BlobError::UNKNOWN);
}

BlobManager::Result< Blob > HSHomeObject::_get_blob(shard_id id, blob_id blob) const {
    return folly::makeUnexpected(BlobError::UNKNOWN);
}

BlobManager::NullResult HSHomeObject::_del_blob(shard_id id, blob_id blob) {
    return folly::makeUnexpected(BlobError::UNKNOWN);
}

} // namespace homeobject
