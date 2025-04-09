#pragma once

#include <filesystem>
#include <list>
#include <memory>
#include <string>
#include <optional>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homeobject/homeobject.hpp>
#include <homeobject/pg_manager.hpp>
#include <homeobject/shard_manager.hpp>
#include <homeobject/blob_manager.hpp>

using homeobject::blob_id_t;
using homeobject::peer_id_t;

class FixtureApp : public homeobject::HomeObjectApplication {
    std::string path_hdd_{"/tmp/homeobject_test.hdd"};
    std::string path_ssd_{"/tmp/homeobject_test.ssd"};
    bool is_hybrid_{false};

public:
    FixtureApp(bool is_hybrid = false);
    ~FixtureApp() = default;

    bool spdk_mode() const override { return false; }
    uint32_t threads() const override { return 2; }

    void clean() {
        if (std::filesystem::exists(path_hdd_)) std::filesystem::remove(path_hdd_);
        if (std::filesystem::exists(path_ssd_)) std::filesystem::remove(path_ssd_);
    }

    std::list< homeobject::device_info_t > devices() const override {
        auto device_info = std::list< homeobject::device_info_t >();
        // add HDD
        device_info.emplace_back(path_hdd_, homeobject::DevType::HDD);
        if (is_hybrid_) {
            // add SSD
            device_info.emplace_back(path_ssd_, homeobject::DevType::NVME);
        }
        return device_info;
    }

    uint64_t mem_size() const override { return 2 * Gi; }
    int max_data_size() const override { return 4 * Mi; }

    homeobject::peer_id_t discover_svcid(std::optional< homeobject::peer_id_t > const& p) const override;
    std::string lookup_peer(homeobject::peer_id_t const&) const override { return "127.0.0.1:4000"; }
};

class TestFixture : public ::testing::Test {
    std::shared_ptr< homeobject::HomeObjectApplication > app;

public:
    homeobject::ShardInfo _shard_1;
    homeobject::ShardInfo _shard_2;
    homeobject::pg_id_t _pg_id{1u};
    peer_id_t _peer1;
    peer_id_t _peer2;
    blob_id_t _blob_id;

    void SetUp() override;
    void TearDown() override;

protected:
    std::shared_ptr< homeobject::HomeObject > homeobj_;
};
