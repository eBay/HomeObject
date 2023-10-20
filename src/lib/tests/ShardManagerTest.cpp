#include <folly/init/Init.h>
#include <folly/executors/GlobalExecutor.h>

#include <homeobject/shard_manager.hpp>
#include "lib/tests/fixture_app.hpp"

using homeobject::shard_id_t;
using homeobject::ShardError;
using homeobject::ShardInfo;

TEST_F(TestFixture, CreateShardTooBig) {
    EXPECT_EQ(
        ShardError::INVALID_ARG,
        homeobj_->shard_manager()->create_shard(_pg_id, homeobject::ShardManager::max_shard_size() + 1).get().error());
}

TEST_F(TestFixture, CreateShardTooSmall) {
    EXPECT_EQ(ShardError::INVALID_ARG, homeobj_->shard_manager()->create_shard(_pg_id, 0ul).get().error());
}

TEST_F(TestFixture, CreateShardNoPg) {
    EXPECT_EQ(ShardError::UNKNOWN_PG, homeobj_->shard_manager()->create_shard(_pg_id + 1, Mi).get().error());
}

TEST_F(TestFixture, GetUnknownShard) {
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, homeobj_->shard_manager()->get_shard(_shard_2.id + 1).get().error());
}

TEST_F(TestFixture, GetKnownShard) {
    auto e = homeobj_->shard_manager()->get_shard(_shard_1.id).get();
    ASSERT_TRUE(!!e);
    e.then([this](auto const& info) {
        EXPECT_TRUE(info.id == _shard_1.id);
        EXPECT_TRUE(info.placement_group == _shard_1.placement_group);
        EXPECT_EQ(info.state, ShardInfo::State::OPEN);
    });
}

TEST_F(TestFixture, ListShardsNoPg) {
    EXPECT_EQ(ShardError::UNKNOWN_PG, homeobj_->shard_manager()->list_shards(_pg_id + 1).get().error());
}

TEST_F(TestFixture, ListShards) {
    auto e = homeobj_->shard_manager()->list_shards(_pg_id).get();
    ASSERT_TRUE(!!e);
    e.then([this](auto const& info_list) {
        ASSERT_EQ(info_list.size(), 2);
        EXPECT_TRUE(info_list.begin()->id == _shard_1.id);
        EXPECT_TRUE(info_list.begin()->placement_group == _shard_1.placement_group);
        EXPECT_EQ(info_list.begin()->state, ShardInfo::State::OPEN);
    });
}

TEST_F(TestFixture, SealShardNoShard) {
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, homeobj_->shard_manager()->seal_shard(_shard_2.id + 1).get().error());
}

TEST_F(TestFixture, SealShard) {
    for (auto i = 0; 2 > i; ++i) {
        auto e = homeobj_->shard_manager()->seal_shard(_shard_1.id).get();
        ASSERT_TRUE(!!e);
        e.then([this](auto const& info) {
            EXPECT_TRUE(info.id == _shard_1.id);
            EXPECT_TRUE(info.placement_group == _shard_1.placement_group);
            EXPECT_EQ(info.state, ShardInfo::State::SEALED);
        });
    }
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, homeobject_options, test_home_object);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);
    return RUN_ALL_TESTS();
}
