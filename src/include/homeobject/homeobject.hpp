#pragma once
#include <filesystem>
#include <list>
#include <memory>
#include <optional>
#include <string>

#include "common.hpp"
#include <sisl/utility/enum.hpp>

namespace homeobject {

class BlobManager;
class PGManager;
class ShardManager;
ENUM(DevType, uint8_t, AUTO_DETECT = 1, HDD, NVME, UNSUPPORTED);
struct device_info_t {
    explicit device_info_t(std::string name, DevType dtype = DevType::AUTO_DETECT) :
            path{std::filesystem::canonical(name)}, type{dtype} {}
    device_info_t() = default;
    bool operator==(device_info_t const& rhs) const { return path == rhs.path && type == rhs.type; }
    friend std::istream& operator>>(std::istream& input, device_info_t& di) {
        std::string i_path, i_type;
        std::getline(input, i_path, ':');
        std::getline(input, i_type);
        di.path = std::filesystem::canonical(i_path);
        if (i_type == "HDD") {
            di.type = DevType::HDD;
        } else if (i_type == "NVME") {
            di.type = DevType::NVME;
        } else {
            di.type = DevType::AUTO_DETECT;
        }
        return input;
    }
    std::filesystem::path path;
    DevType type;
};

class HomeObjectApplication {
public:
    virtual ~HomeObjectApplication() = default;

    virtual bool spdk_mode() const = 0;
    virtual uint32_t threads() const = 0;
    virtual std::list< device_info_t > devices() const = 0;
    // Memory size in bytes
    virtual uint64_t mem_size() const = 0;

    // Callback made after determining if a SvcId exists or not during initialization, will consume response
    virtual peer_id_t discover_svcid(std::optional< peer_id_t > const& found) const = 0;

    // When RAFT operations take place, we must map the SvcId to a gethostbyaddr() value (IP)
    virtual std::string lookup_peer(peer_id_t const&) const = 0;
};

struct HomeObjectStats {
    uint64_t total_capacity_bytes{0};
    uint64_t used_capacity_bytes{0};
    uint32_t num_open_shards{0};
    uint32_t avail_open_shards{0};
    uint32_t num_disks{0};
    std::string to_string() const {
        return fmt::format("total_capacity_bytes={}, used_capacity_bytes={}, num_open_shards={}, avail_open_shards={}, num_disks={}",
                           total_capacity_bytes, used_capacity_bytes, num_open_shards, avail_open_shards, num_disks);
    }
};

class HomeObject {
public:
    virtual ~HomeObject() = default;
    virtual peer_id_t our_uuid() const = 0;
    virtual std::shared_ptr< BlobManager > blob_manager() = 0;
    virtual std::shared_ptr< PGManager > pg_manager() = 0;
    virtual std::shared_ptr< ShardManager > shard_manager() = 0;
    virtual HomeObjectStats get_stats() const = 0;
};

extern std::shared_ptr< HomeObject > init_homeobject(std::weak_ptr< HomeObjectApplication >&& application);

} // namespace homeobject
  //

namespace fmt {
template <>
struct formatter< homeobject::device_info_t > {
    template < typename ParseContext >
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template < typename FormatContext >
    auto format(homeobject::device_info_t const& device, FormatContext& ctx) {
        std::string type;
        switch (device.type) {
        case homeobject::DevType::HDD:
            type = "HDD";
            break;
        case homeobject::DevType::NVME:
            type = "NVME";
            break;
        case homeobject::DevType::UNSUPPORTED:
            type = "UNSUPPORTED";
            break;
        case homeobject::DevType::AUTO_DETECT:
            type = "AUTO_DETECT";
            break;
        default:
            type = "UNKNOWN";
        }
        return fmt::format_to(ctx.out(), "Path={}, Type={}", device.path.string(), type);
    }
};
} // namespace fmt
