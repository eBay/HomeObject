#include <chrono>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>

#include "file_homeobject.hpp"

namespace homeobject {

using std::filesystem::path;

uint64_t ShardManager::max_shard_size() {
    return (SISL_OPTIONS.count("max_filesize") > 0) ? SISL_OPTIONS["max_filesize"].as< uint32_t >() * Mi : 1 * Gi;
}

///
// Each Shard is stored as a FILE on the system. We defer creating the "PG" (directory) until
// the first Shard is created
ShardManager::AsyncResult< ShardInfo > FileHomeObject::_create_shard(pg_id_t pg_owner, uint64_t size_bytes) {
    auto const now = get_current_timestamp();
    auto info = ShardInfo(0ull, pg_owner, ShardInfo::State::OPEN, now, now, size_bytes, size_bytes, 0);
    {
        auto lg = std::scoped_lock(_pg_lock, _shard_lock);
        auto pg_it = _pg_map.find(pg_owner);
        if (_pg_map.end() == pg_it) return folly::makeUnexpected(ShardError::UNKNOWN_PG);

        auto& s_list = pg_it->second->shards_;
        info.id = make_new_shard_id(pg_owner, s_list.size());
        auto iter = s_list.emplace(s_list.end(), std::make_unique< Shard >(info));
        LOGDEBUG("Creating Shard [{}]: in Pg [{}] of Size [{}b]", info.id & shard_mask, pg_owner, size_bytes);
        auto [_, s_happened] = _shard_map.emplace(info.id, iter);
        RELEASE_ASSERT(s_happened, "Duplicate Shard insertion!");
    }

    auto const pg_path = file_store_ / path(fmt::format("{:04x}", (info.id >> homeobject::shard_width)));
    auto const shard_file = pg_path / path(fmt::format("{:012x}", (info.id & homeobject::shard_mask)));
    RELEASE_ASSERT(!std::filesystem::exists(shard_file), "Shard Path Exists! [path={}]", shard_file.string());
    std::ofstream ofs{shard_file, std::ios::binary | std::ios::out | std::ios::trunc};
    std::filesystem::resize_file(shard_file, max_shard_size());
    RELEASE_ASSERT(std::filesystem::exists(shard_file), "Shard Path Failed Creation! [path={}]", shard_file.string());

    auto shard_fd = open(shard_file.string().c_str(), O_RDWR);
    RELEASE_ASSERT(shard_fd >= 0, "Failed to open Shard {}", shard_file.string());

    nlohmann::json j;
    j["shard_id"] = info.id;
    j["pg_id"] = info.placement_group;
    j["state"] = info.state;
    j["created_time"] = info.created_time;
    j["modified_time"] = info.last_modified_time;
    j["total_capacity"] = info.total_capacity_bytes;
    j["available_capacity"] = info.available_capacity_bytes;
    j["deleted_capacity"] = info.deleted_capacity_bytes;
    auto serialize = j.dump();
    auto hdr_len = serialize.size();
    auto err = pwrite(shard_fd, &hdr_len, sizeof(hdr_len), 0ull);
    RELEASE_ASSERT(0 < err, "Failed to write to: {}", shard_file.string());
    err = pwrite(shard_fd, serialize.c_str(), serialize.size(), sizeof(hdr_len));
    RELEASE_ASSERT(0 < err, "Failed to write to: {}", shard_file.string());

    auto [it, happened] = index_.try_emplace(info.id, std::make_unique< ShardIndex >());
    it->second->fd_ = shard_fd;
    RELEASE_ASSERT(happened, "Could not create BTree!");
    it->second->shard_offset_.store(sizeof(hdr_len) + serialize.size());
    return info;
}

///
// Shard STATE is managed through the FILE stat (rw/ro) TODO
ShardManager::AsyncResult< ShardInfo > FileHomeObject::_seal_shard(ShardInfo const& shard) {
    auto lg = std::scoped_lock(_shard_lock);
    auto shard_it = _shard_map.find(shard.id);
    RELEASE_ASSERT(_shard_map.end() != shard_it, "Missing ShardIterator!");
    auto& shard_info = (*shard_it->second)->info;
    shard_info.state = ShardInfo::State::SEALED;
    return shard_info;
}

ShardIndex::~ShardIndex() {
    if (fd_ > 0) close(fd_);
}

} // namespace homeobject
