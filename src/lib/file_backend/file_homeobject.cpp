#include "file_homeobject.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <system_error>

SISL_OPTION_GROUP(homeobject_file,
                  (max_filesize, "", "max_filesize", "Maximum File (Shard) size",
                   cxxopts::value< uint32_t >()->default_value("1024"), "mb"))

namespace homeobject {

/// NOTE: We give ourselves the option to provide a different HR instance here than libhomeobject.a
extern std::shared_ptr< HomeObject > init_homeobject(std::weak_ptr< HomeObjectApplication >&& application) {
    auto devices = application.lock()->devices();
    auto instance = std::make_shared< FileHomeObject >(std::move(application), *devices.begin());
    return instance;
}
void FileHomeObject::_recover() {
    for (auto const& pg_dir_e : std::filesystem::directory_iterator{file_store_}) {
        auto pg_dir = pg_dir_e.path();
        if (pg_dir.filename().string() == "svc_id") continue;
        LOGI("discovered [pg_dir={}]", pg_dir.string());
        auto pgid = std::stoul(pg_dir.filename().string());
        LOGI("discovered [pg_dir={}] [pg_id={}]", pg_dir.string(), pgid);
        auto [it, happened] = _pg_map.try_emplace(pgid, std::make_unique< PG >(PGInfo(pgid)));
        auto& s_list = it->second->shards_;
        RELEASE_ASSERT(happened, "Unknown map insert error!");
        for (auto const& shard_file_e : std::filesystem::directory_iterator{pg_dir_e}) {
            auto shard_file = shard_file_e.path().string();
            LOGI("discovered [shard_file={}]", shard_file);
            auto shard_fd = open(shard_file.c_str(), O_RDONLY);
            RELEASE_ASSERT(shard_fd >= 0, "Failed to open Shard {}", shard_file);

            size_t h_size = 0ull;
            auto err = pread(shard_fd, &h_size, sizeof(h_size), 0ull);
            RELEASE_ASSERT(0 < err, "Failed to read from: {}", shard_file);

            auto j_str = std::string(h_size, '\0');
            err = pread(shard_fd, const_cast< char* >(j_str.c_str()), h_size, sizeof(h_size));
            RELEASE_ASSERT(0 < err, "Failed to read from: {}", shard_file);
            auto shard_json = nlohmann::json::parse(j_str);

            auto info = ShardInfo();
            info.id = shard_json["shard_id"].get< shard_id_t >();
            info.placement_group = shard_json["pg_id"].get< pg_id_t >();
            info.state = shard_json["state"].get< ShardInfo::State >();
            info.created_time = shard_json["created_time"].get< uint64_t >();
            info.last_modified_time = shard_json["modified_time"].get< uint64_t >();
            info.available_capacity_bytes = shard_json["available_capacity"].get< uint64_t >();
            info.total_capacity_bytes = shard_json["total_capacity"].get< uint64_t >();
            info.deleted_capacity_bytes = shard_json["deleted_capacity"].get< uint64_t >();

            auto iter = s_list.emplace(s_list.end(), Shard(info));
            auto [_, s_happened] = _shard_map.emplace(info.id, iter);
            RELEASE_ASSERT(s_happened, "Duplicate Shard insertion!");
            close(shard_fd);
        }
    }
}

FileHomeObject::FileHomeObject(std::weak_ptr< HomeObjectApplication >&& application,
                               std::filesystem::path const& root) :
        HomeObjectImpl::HomeObjectImpl(std::move(application)), file_store_(root) {
    auto const id_file = file_store_ / "svc_id";
    if (std::filesystem::exists(file_store_)) {
        auto id_fd = open(id_file.string().c_str(), O_RDONLY);
        auto err = pread(id_fd, &_our_id, sizeof(_our_id), 0ull);
        RELEASE_ASSERT(0 < err, "Failed to write to: {}", id_file.string());
        LOGI("recovering: {}", to_string(_our_id));
        _recover();
    }
    _our_id = _application.lock()->discover_svcid(_our_id);
    std::filesystem::create_directories(file_store_);
    std::ofstream ofs{id_file, std::ios::binary | std::ios::out | std::ios::trunc};
    std::filesystem::resize_file(id_file, 4 * Ki);
    auto id_fd = open(id_file.string().c_str(), O_WRONLY);
    RELEASE_ASSERT(0 < id_fd, "Failed to open: {}", id_file.string());
    auto err = pwrite(id_fd, &_our_id, sizeof(_our_id), 0ull);
    RELEASE_ASSERT(0 < err, "Failed to write to: {}", id_file.string());
}

} // namespace homeobject
