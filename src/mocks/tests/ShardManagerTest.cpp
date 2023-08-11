#include <chrono>
#include <string>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "mocks/mock_homeobject.hpp"

using namespace std::chrono_literals;
using homeobject::shard_id;
using homeobject::ShardError;
using homeobject::ShardInfo;

SISL_LOGGING_INIT(logging, homeobject)
SISL_OPTIONS_ENABLE(logging)

class ShardManagerFixture : public ::testing::Test {
public:
    homeobject::pg_id _pg_id{1u};
    homeobject::peer_id _peer1;
    homeobject::peer_id _peer2;

    void SetUp() override {
        auto params = homeobject::init_params{[](homeobject::peer_id const&) { return std::string(); }};
        m_mock_homeobj = homeobject::init_homeobject(params);

        _peer1 = boost::uuids::random_generator()();
        _peer2 = boost::uuids::random_generator()();

        auto info = homeobject::PGInfo(_pg_id);
        info.members.insert(homeobject::PGMember{_peer1, "peer1", 1});
        info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});
        m_mock_homeobj->pg_manager()->create_pg(info).thenValue(
            [](homeobject::PGError e) { EXPECT_EQ(homeobject::PGError::OK, e); });
    }

protected:
    std::shared_ptr< homeobject::HomeObject > m_mock_homeobj;
};

TEST_F(ShardManagerFixture, CreateShardTooBig) {
    m_mock_homeobj->shard_manager()
        ->create_shard(_pg_id, homeobject::ShardManager::max_shard_size() + 1)
        .thenValue([](std::variant< ShardInfo, ShardError > const& v) {
            ASSERT_TRUE(std::holds_alternative< ShardError >(v));
            EXPECT_EQ(std::get< ShardError >(v), ShardError::INVALID_ARG);
        });
}

TEST_F(ShardManagerFixture, CreateShardTooSmall) {
    m_mock_homeobj->shard_manager()
        ->create_shard(_pg_id, 0ul)
        .thenValue([](std::variant< ShardInfo, ShardError > const& v) {
            ASSERT_TRUE(std::holds_alternative< ShardError >(v));
            EXPECT_EQ(std::get< ShardError >(v), ShardError::INVALID_ARG);
        });
}

TEST_F(ShardManagerFixture, CreateShardNoPg) {
    m_mock_homeobj->shard_manager()
        ->create_shard(_pg_id + 1, Mi)
        .thenValue([](std::variant< ShardInfo, ShardError > const& v) {
            ASSERT_TRUE(std::holds_alternative< ShardError >(v));
            EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_PG);
        });
}

class ShardManagerFixtureWShard : public ShardManagerFixture {
public:
    ShardInfo _shard;
    void SetUp() override {
        ShardManagerFixture::SetUp();
        m_mock_homeobj->shard_manager()
            ->create_shard(_pg_id, Mi)
            .thenValue([this](std::variant< ShardInfo, ShardError > const& v) mutable {
                ASSERT_TRUE(std::holds_alternative< ShardInfo >(v));
                _shard = std::get< ShardInfo >(v);
                EXPECT_EQ(ShardInfo::State::OPEN, _shard.state);
                EXPECT_EQ(Mi, _shard.total_capacity_bytes);
                EXPECT_EQ(Mi, _shard.available_capacity_bytes);
                EXPECT_EQ(0ul, _shard.deleted_capacity_bytes);
                EXPECT_EQ(_pg_id, _shard.placement_group);
            });
    }
};

TEST_F(ShardManagerFixtureWShard, GetUnknownShard) {
    auto v = m_mock_homeobj->shard_manager()->get_shard(_shard.id + 1);
    ASSERT_TRUE(std::holds_alternative< ShardError >(v));
    EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_SHARD);
}

TEST_F(ShardManagerFixtureWShard, GetKnownShard) {
    auto v = m_mock_homeobj->shard_manager()->get_shard(_shard.id);
    ASSERT_TRUE(std::holds_alternative< ShardInfo >(v));
    auto const& info = std::get< ShardInfo >(v);
    EXPECT_TRUE(info.id == _shard.id);
    EXPECT_TRUE(info.placement_group == _shard.placement_group);
    EXPECT_EQ(info.state, ShardInfo::State::OPEN);
}

TEST_F(ShardManagerFixtureWShard, ListShardsNoPg) {
    m_mock_homeobj->shard_manager()
        ->list_shards(_pg_id + 1)
        .thenValue([this](std::variant< std::vector< ShardInfo >, ShardError > const& v) {
            ASSERT_TRUE(std::holds_alternative< ShardError >(v));
            EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_PG);
        });
}

TEST_F(ShardManagerFixtureWShard, ListShards) {
    m_mock_homeobj->shard_manager()->list_shards(_pg_id).thenValue(
        [this](std::variant< std::vector< ShardInfo >, ShardError > const& v) {
            ASSERT_TRUE(std::holds_alternative< std::vector< ShardInfo > >(v));
            auto const& info_vec = std::get< std::vector< ShardInfo > >(v);
            EXPECT_EQ(info_vec.size(), 1);
            EXPECT_TRUE(info_vec.begin()->id == _shard.id);
            EXPECT_TRUE(info_vec.begin()->placement_group == _shard.placement_group);
            EXPECT_EQ(info_vec.begin()->state, ShardInfo::State::OPEN);
        });
}

TEST_F(ShardManagerFixtureWShard, SealShardNoShard) {
    m_mock_homeobj->shard_manager()
        ->seal_shard(_shard.id + 1)
        .thenValue([this](std::variant< ShardInfo, ShardError > const& v) {
            ASSERT_TRUE(std::holds_alternative< ShardError >(v));
            EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_SHARD);
        });
}

TEST_F(ShardManagerFixtureWShard, SealShard) {
    m_mock_homeobj->shard_manager()->seal_shard(_shard.id).thenValue(
        [this](std::variant< ShardInfo, ShardError > const& v) {
            ASSERT_TRUE(std::holds_alternative< ShardInfo >(v));
            auto const& info = std::get< ShardInfo >(v);
            EXPECT_TRUE(info.id == _shard.id);
            EXPECT_TRUE(info.placement_group == _shard.placement_group);
            EXPECT_EQ(info.state, ShardInfo::State::SEALED);
        });
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    return RUN_ALL_TESTS();
}
