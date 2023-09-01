#include "homeobject.hpp"

namespace homeobject {

BlobManager::Result< blob_id > HSHomeObject::_put_blob(ShardInfo const&, Blob&&) {
    return folly::makeUnexpected(BlobError::UNKNOWN);
}

BlobManager::Result< Blob > HSHomeObject::_get_blob(ShardInfo const&, blob_id) const {
    return folly::makeUnexpected(BlobError::UNKNOWN);
}

BlobManager::NullResult HSHomeObject::_del_blob(ShardInfo const&, blob_id) {
    return folly::makeUnexpected(BlobError::UNKNOWN);
}

} // namespace homeobject
