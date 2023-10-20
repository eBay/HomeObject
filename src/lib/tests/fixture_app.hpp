#include <filesystem>
#include <list>
#include <memory>
#include <string>
#include <optional>
#include <ostream>

#include <gtest/gtest.h>
#include <boost/uuid/random_generator.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homeobject/homeobject.hpp>
#include <homeobject/pg_manager.hpp>
#include <homeobject/shard_manager.hpp>
#include <homeobject/blob_manager.hpp>

SISL_LOGGING_INIT(logging, HOMEOBJECT_LOG_MODS)

SISL_OPTION_GROUP(blob_manager_test,
                  (num_iters, "", "num_iters", "number of iterations per loop",
                   ::cxxopts::value< uint64_t >()->default_value("100000"), "number"));

SISL_OPTIONS_ENABLE(logging, blob_manager_test)

using homeobject::Blob;
using homeobject::blob_id_t;
using homeobject::BlobError;
using homeobject::peer_id_t;

class FixtureApp : public homeobject::HomeObjectApplication {
    std::string path_{"/tmp/test_homestore.data." + std::to_string(rand())};

public:
    FixtureApp() {
        clean();
        LOGINFO("creating device {} file with size {} ", path_, homestore::in_bytes(2 * Gi));
        std::ofstream ofs{path_, std::ios::binary | std::ios::out | std::ios::trunc};
        std::filesystem::resize_file(path_, 2 * Gi);
    }

    ~FixtureApp() override { clean(); }

    bool spdk_mode() const override { return false; }
    uint32_t threads() const override { return 2; }

    void clean() {
        if (std::filesystem::exists(path_)) { std::filesystem::remove(path_); }
    }

    std::list< std::filesystem::path > devices() const override {
        auto device_info = std::list< std::filesystem::path >();
        device_info.emplace_back(std::filesystem::canonical(path_));
        return device_info;
    }

    homeobject::peer_id_t discover_svcid(std::optional< homeobject::peer_id_t > const& p) const override {
        return p.value();
    }
    std::string lookup_peer(homeobject::peer_id_t const&) const override { return "test_fixture.com"; }
};

class TestFixture : public ::testing::Test {
    std::shared_ptr< FixtureApp > app;

public:
    homeobject::ShardInfo _shard_1;
    homeobject::ShardInfo _shard_2;
    homeobject::pg_id_t _pg_id{1u};
    peer_id_t _peer1;
    peer_id_t _peer2;
    blob_id_t _blob_id;

    void SetUp() override {
        app = std::make_shared< FixtureApp >();
        homeobj_ = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
        _peer1 = homeobj_->our_uuid();
        _peer2 = boost::uuids::random_generator()();

        auto info = homeobject::PGInfo(_pg_id);
        info.members.insert(homeobject::PGMember{_peer1, "peer1", 1});
        info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});

        LOGDEBUG("Setup Pg");
        EXPECT_TRUE(homeobj_->pg_manager()->create_pg(std::move(info)).get());

        LOGDEBUG("Setup Shards");
        auto s_e = homeobj_->shard_manager()->create_shard(_pg_id, Mi).get();
        ASSERT_TRUE(!!s_e);
        s_e.then([this](auto&& i) { _shard_1 = std::move(i); });

        s_e = homeobj_->shard_manager()->create_shard(_pg_id, Mi).get();
        ASSERT_TRUE(!!s_e);
        s_e.then([this](auto&& i) { _shard_2 = std::move(i); });

        LOGDEBUG("Get on empty Shard: {}", _shard_1.id);
        auto g_e = homeobj_->blob_manager()->get(_shard_1.id, 0).get();
        ASSERT_FALSE(g_e);
        EXPECT_EQ(BlobError::UNKNOWN_BLOB, g_e.error());

        LOGDEBUG("Insert Blob to: {}", _shard_1.id);
        auto o_e = homeobj_->blob_manager()
                       ->put(_shard_1.id, Blob{sisl::io_blob_safe(4 * Ki, 512u), "test_blob", 4 * Mi})
                       .get();
        EXPECT_TRUE(!!o_e);
        o_e.then([this](auto&& b) mutable { _blob_id = std::move(b); });

        g_e = homeobj_->blob_manager()->get(_shard_1.id, _blob_id).get();
        EXPECT_TRUE(!!g_e);
        g_e.then([](auto&& blob) {
            EXPECT_STREQ(blob.user_key.c_str(), "test_blob");
            EXPECT_EQ(blob.object_off, 4 * Mi);
        });
    }

protected:
    std::shared_ptr< homeobject::HomeObject > homeobj_;
};
