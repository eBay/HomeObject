#include <string>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "lib/memory/homeobject.hpp"

using homeobject::shard_id;
using homeobject::ShardError;
using homeobject::ShardInfo;

SISL_LOGGING_INIT(logging, HOMEOBJECT_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

class FixtureApp : public homeobject::HomeObjectApplication {
public:
    bool spdk_mode() const override { return false; }
    uint32_t threads() const override { return 2; }
    std::list< std::filesystem::path > devices() const override { return std::list< std::filesystem::path >(); }
    homeobject::peer_id discover_svcid(std::optional< homeobject::peer_id > const& p) const override {
        return p.value();
    }
    std::string lookup_peer(homeobject::peer_id const&) const override { return "test_fixture.com"; }
};

class ShardManagerFixture : public ::testing::Test {
public:
    homeobject::pg_id _pg_id{1u};
    homeobject::peer_id _peer1;
    homeobject::peer_id _peer2;

    void SetUp() override {
        app = std::make_shared< FixtureApp >();
        m_memory_homeobj = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
        _peer1 = m_memory_homeobj->our_uuid();
        _peer2 = boost::uuids::random_generator()();

        auto info = homeobject::PGInfo(_pg_id);
        info.members.insert(homeobject::PGMember{_peer1, "peer1", 1});
        info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});
        EXPECT_TRUE(m_memory_homeobj->pg_manager()->create_pg(std::move(info)).get());
    }

protected:
    std::shared_ptr< FixtureApp > app;
    std::shared_ptr< homeobject::HomeObject > m_memory_homeobj;
};

TEST_F(ShardManagerFixture, CreateShardTooBig) {
    EXPECT_EQ(ShardError::INVALID_ARG,
              m_memory_homeobj->shard_manager()
                  ->create_shard(_pg_id, homeobject::ShardManager::max_shard_size() + 1)
                  .get()
                  .error());
}

TEST_F(ShardManagerFixture, CreateShardTooSmall) {
    EXPECT_EQ(ShardError::INVALID_ARG, m_memory_homeobj->shard_manager()->create_shard(_pg_id, 0ul).get().error());
}

TEST_F(ShardManagerFixture, CreateShardNoPg) {
    EXPECT_EQ(ShardError::UNKNOWN_PG, m_memory_homeobj->shard_manager()->create_shard(_pg_id + 1, Mi).get().error());
}

class ShardManagerFixtureWShard : public ShardManagerFixture {
public:
    ShardInfo _shard;
    void SetUp() override {
        ShardManagerFixture::SetUp();
        auto e = m_memory_homeobj->shard_manager()->create_shard(_pg_id, Mi).get();
        ASSERT_TRUE(!!e);
        e.then([this](auto&& i) { _shard = std::move(i); });
        EXPECT_EQ(ShardInfo::State::OPEN, _shard.state);
        EXPECT_EQ(Mi, _shard.total_capacity_bytes);
        EXPECT_EQ(Mi, _shard.available_capacity_bytes);
        EXPECT_EQ(0ul, _shard.deleted_capacity_bytes);
        EXPECT_EQ(_pg_id, _shard.placement_group);
    }
};

TEST_F(ShardManagerFixtureWShard, GetUnknownShard) {
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, m_memory_homeobj->shard_manager()->get_shard(_shard.id + 1).get().error());
}

TEST_F(ShardManagerFixtureWShard, GetKnownShard) {
    auto e = m_memory_homeobj->shard_manager()->get_shard(_shard.id).get();
    ASSERT_TRUE(!!e);
    e.then([this](auto const& info) {
        EXPECT_TRUE(info.id == _shard.id);
        EXPECT_TRUE(info.placement_group == _shard.placement_group);
        EXPECT_EQ(info.state, ShardInfo::State::OPEN);
    });
}

TEST_F(ShardManagerFixtureWShard, ListShardsNoPg) {
    EXPECT_EQ(ShardError::UNKNOWN_PG, m_memory_homeobj->shard_manager()->list_shards(_pg_id + 1).get().error());
}

TEST_F(ShardManagerFixtureWShard, ListShards) {
    auto e = m_memory_homeobj->shard_manager()->list_shards(_pg_id).get();
    ASSERT_TRUE(!!e);
    e.then([this](auto const& info_list) {
        ASSERT_EQ(info_list.size(), 1);
        EXPECT_TRUE(info_list.begin()->id == _shard.id);
        EXPECT_TRUE(info_list.begin()->placement_group == _shard.placement_group);
        EXPECT_EQ(info_list.begin()->state, ShardInfo::State::OPEN);
    });
}

TEST_F(ShardManagerFixtureWShard, SealShardNoShard) {
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, m_memory_homeobj->shard_manager()->seal_shard(_shard.id + 1).get().error());
}

TEST_F(ShardManagerFixtureWShard, SealShard) {
    for (auto i = 0; 2 > i; ++i) {
        auto e = m_memory_homeobj->shard_manager()->seal_shard(_shard.id).get();
        ASSERT_TRUE(!!e);
        e.then([this](auto const& info) {
            EXPECT_TRUE(info.id == _shard.id);
            EXPECT_TRUE(info.placement_group == _shard.placement_group);
            EXPECT_EQ(info.state, ShardInfo::State::SEALED);
        });
    }
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);
    return RUN_ALL_TESTS();
}
