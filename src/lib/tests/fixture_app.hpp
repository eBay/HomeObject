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
    std::string path_{"/tmp/homeobject_test.data"};

public:
    FixtureApp();
    ~FixtureApp() = default;

    bool spdk_mode() const override { return false; }
    uint32_t threads() const override { return 2; }

    void clean() {
        if (std::filesystem::exists(path_)) std::filesystem::remove(path_);
    }

    std::list< std::filesystem::path > devices() const override {
        auto device_info = std::list< std::filesystem::path >();
        device_info.emplace_back(std::filesystem::canonical(path_));
        return device_info;
    }

    homeobject::peer_id_t discover_svcid(std::optional< homeobject::peer_id_t > const& p) const override;
    std::string lookup_peer(homeobject::peer_id_t const&) const override { return "test_fixture.com"; }
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
