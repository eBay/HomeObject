#include "mock_homeobject.hpp"

namespace homeobject {

void MockHomeObject::put(shard_id shard, Blob const&, id_cb cb) {}
void MockHomeObject::get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len, get_cb) const {}
void MockHomeObject::del(shard_id shard, blob_id const& blob, BlobManager::ok_cb cb) {
    cb(BlobError::OK, std::nullopt);
}

} // namespace homeobject
