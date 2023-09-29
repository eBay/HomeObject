#include <fcntl.h>
#include <unistd.h>

#include "file_homeobject.hpp"

namespace homeobject {

using std::filesystem::path;

#define WITH_SHARD                                                                                                     \
    auto index_it = index_.find(_shard.id);                                                                            \
    RELEASE_ASSERT(index_.end() != index_it, "Missing BTree!!");                                                       \
    auto& shard = *index_it->second;

#define WITH_ROUTE(blob)                                                                                               \
    auto route = BlobRoute{_shard.id, (blob)};                                                                         \
    LOGTRACEMOD(homeobject, "[route={}]", route);

#define IF_BLOB_ALIVE                                                                                                  \
    if (auto blob_it = shard.btree_.find(route); shard.btree_.end() == blob_it || !blob_it->second) {                  \
        LOGWARNMOD(homeobject, "[route={}] missing", route);                                                           \
        return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);                                                         \
    } else

// Write (move) Blob to FILE
BlobManager::Result< blob_id_t > FileHomeObject::_put_blob(ShardInfo const& _shard, Blob&& _blob) {
    WITH_SHARD

    nlohmann::json j;
    j["user_key"] = _blob.user_key;
    j["object_off"] = _blob.object_off;
    j["body_size"] = _blob.body.size;
    auto serialize = j.dump();
    auto const h_size = serialize.size();
    auto const t_size = sizeof(size_t) + h_size + _blob.body.size;

    WITH_ROUTE(shard.shard_offset_.fetch_add(t_size, std::memory_order_relaxed))
    if (route.blob + t_size > max_shard_size()) return folly::makeUnexpected(BlobError::INVALID_ARG);

    auto err = pwrite(shard.fd_, &h_size, sizeof(h_size), route.blob);
    RELEASE_ASSERT(0 < err, "failed to write to: {route=}", route);
    err = pwrite(shard.fd_, serialize.c_str(), h_size, sizeof(h_size) + route.blob);
    RELEASE_ASSERT(0 < err, "failed to write to: {route=}", route);
    err = pwrite(shard.fd_, _blob.body.bytes, _blob.body.size, sizeof(h_size) + h_size + route.blob);
    RELEASE_ASSERT(0 < err, "failed to write to: {route=}", route);
    auto [_, happened] = shard.btree_.try_emplace(route, true);
    RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
    return route.blob;
}

nlohmann::json FileHomeObject::_read_blob_header(int shard_fd, blob_id_t& blob_id) {
    size_t h_size = 0ull;
    auto err = pread(shard_fd, &h_size, sizeof(h_size), blob_id);
    RELEASE_ASSERT(0 < err, "failed to read from shard");
    blob_id += sizeof(h_size);

    auto j_str = std::string(h_size, '\0');
    err = pread(shard_fd, const_cast< char* >(j_str.c_str()), h_size, blob_id);
    blob_id += h_size;
    try {
        if (0 <= err) return nlohmann::json::parse(j_str);
        LOGE("failed to read: {}", strerror(errno));
    } catch (nlohmann::json::exception const&) { LOGT("no blob @ [blob_id={}]", blob_id); }
    return nlohmann::json{};
}

// Lookup and duplicate underyling Blob for user; only *safe* because we defer GC.
BlobManager::Result< Blob > FileHomeObject::_get_blob(ShardInfo const& _shard, blob_id_t _blob) const {
    WITH_SHARD
    WITH_ROUTE(_blob)
    IF_BLOB_ALIVE {
        auto blob_json = _read_blob_header(shard.fd_, route.blob);

        auto const body_size = blob_json["body_size"].get< uint64_t >();
        auto b = Blob{sisl::io_blob_safe(body_size), "", 0};
        auto err = pread(shard.fd_, b.body.bytes, body_size, route.blob);
        RELEASE_ASSERT(0 < err, "Failed to read from: [route={}]", route);

        b.user_key = blob_json["user_key"].get< std::string >();
        b.object_off = blob_json["object_off"].get< uint64_t >();
        return b;
    }
}

// Tombstone entry
BlobManager::NullResult FileHomeObject::_del_blob(ShardInfo const& _shard, blob_id_t _blob) {
    WITH_SHARD
    WITH_ROUTE(_blob)
    IF_BLOB_ALIVE {
        shard.btree_.assign_if_equal(route, blob_it->second, false);
        return folly::Unit();
    }
}

} // namespace homeobject
