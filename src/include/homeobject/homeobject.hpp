#pragma once
#include <filesystem>
#include <list>
#include <memory>
#include <optional>
#include <string>

#include "common.hpp"

namespace homeobject {

class BlobManager;
class PGManager;
class ShardManager;

class HomeObjectApplication {
public:
    virtual ~HomeObjectApplication() = default;

    virtual bool spdk_mode() const = 0;
    virtual uint32_t threads() const = 0;
    virtual std::list< std::filesystem::path > devices() const = 0;

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
    std::string to_string() const {
        return fmt::format("total_capacity_bytes={}, used_capacity_bytes={}, num_open_shards={}, avail_open_shards={}",
                           total_capacity_bytes, used_capacity_bytes, num_open_shards, avail_open_shards);
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
