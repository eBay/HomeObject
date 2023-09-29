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
    auto const route = BlobRoute{_shard.id, (blob)};                                                                   \
    LOGTRACEMOD(homeobject, "[route={}]", route);

#define IF_BLOB_ALIVE                                                                                                  \
    if (auto blob_it = shard.btree_.find(route); shard.btree_.end() == blob_it || !blob_it->second) {                  \
        LOGWARNMOD(homeobject, "[route={}] missing", route);                                                           \
        return folly::makeUnexpected(BlobError::UNKNOWN_BLOB);                                                         \
    } else

#define WITH_SHARD_FILE(mode)                                                                                          \
    auto const shard_file = file_store_ / path(fmt::format("{:04x}", (_shard.id >> homeobject::shard_width))) /        \
        path(fmt::format("{:012x}", (_shard.id & homeobject::shard_mask)));                                            \
    auto shard_fd = open(shard_file.string().c_str(), (mode));                                                         \
    RELEASE_ASSERT(shard_fd >= 0, "Failed to open Shard {}", shard_file.string());

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
    WITH_SHARD_FILE(O_WRONLY)

    auto err = pwrite(shard_fd, &h_size, sizeof(h_size), route.blob);
    RELEASE_ASSERT(0 < err, "Failed to write to: {}", shard_file.string());
    err = pwrite(shard_fd, serialize.c_str(), h_size, sizeof(h_size) + route.blob);
    RELEASE_ASSERT(0 < err, "Failed to write to: {}", shard_file.string());
    err = pwrite(shard_fd, _blob.body.bytes, _blob.body.size, sizeof(h_size) + h_size + route.blob);
    RELEASE_ASSERT(0 < err, "Failed to write to: {}", shard_file.string());
    auto [_, happened] = shard.btree_.try_emplace(route, true);
    RELEASE_ASSERT(happened, "Generated duplicate BlobRoute!");
    close(shard_fd);
    return route.blob;
}

// Lookup and duplicate underyling Blob for user; only *safe* because we defer GC.
BlobManager::Result< Blob > FileHomeObject::_get_blob(ShardInfo const& _shard, blob_id_t _blob) const {
    WITH_SHARD
    WITH_ROUTE(_blob)
    IF_BLOB_ALIVE {
        WITH_SHARD_FILE(O_RDONLY)
        size_t h_size = 0ull;
        auto err = pread(shard_fd, &h_size, sizeof(h_size), route.blob);
        RELEASE_ASSERT(0 < err, "Failed to read from: {}", shard_file.string());

        auto j_str = std::string(h_size, '\0');
        err = pread(shard_fd, const_cast< char* >(j_str.c_str()), h_size, sizeof(h_size) + route.blob);
        RELEASE_ASSERT(0 < err, "Failed to read from: {}", shard_file.string());
        auto blob_json = nlohmann::json::parse(j_str);

        auto const body_size = blob_json["body_size"].get< uint64_t >();
        auto b = Blob{sisl::io_blob_safe(body_size), "", 0};
        err = pread(shard_fd, b.body.bytes, body_size, sizeof(h_size) + h_size + route.blob);
        RELEASE_ASSERT(0 < err, "Failed to read from: {}", shard_file.string());

        b.user_key = blob_json["user_key"].get< std::string >();
        b.object_off = blob_json["object_off"].get< uint64_t >();
        close(shard_fd);
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
