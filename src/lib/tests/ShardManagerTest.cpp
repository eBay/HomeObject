#include <homeobject/shard_manager.hpp>
#include "lib/tests/fixture_app.hpp"

using homeobject::shard_id_t;
using homeobject::ShardError;
using homeobject::ShardErrorCode;
using homeobject::ShardInfo;

TEST_F(TestFixture, CreateShardTooBig) {
    EXPECT_EQ(ShardErrorCode::INVALID_ARG,
              homeobj_->shard_manager()
                  ->create_shard(_pg_id, homeobject::ShardManager::max_shard_size() + 1, "shard meta")
                  .get()
                  .error()
                  .getCode());
}

TEST_F(TestFixture, CreateShardTooSmall) {
    EXPECT_EQ(ShardErrorCode::INVALID_ARG,
              homeobj_->shard_manager()->create_shard(_pg_id, 0ul, "shard meta").get().error().getCode());
}

TEST_F(TestFixture, CreateShardNoPg) {
    EXPECT_EQ(ShardErrorCode::UNKNOWN_PG,
              homeobj_->shard_manager()->create_shard(_pg_id + 1, Mi, "shard meta").get().error().getCode());
}

TEST_F(TestFixture, GetUnknownShard) {
    EXPECT_EQ(ShardErrorCode::UNKNOWN_SHARD,
              homeobj_->shard_manager()->get_shard(_shard_2.id + 1).get().error().getCode());
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
    EXPECT_EQ(ShardErrorCode::UNKNOWN_PG, homeobj_->shard_manager()->list_shards(_pg_id + 1).get().error().getCode());
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
    EXPECT_EQ(ShardErrorCode::UNKNOWN_SHARD,
              homeobj_->shard_manager()->seal_shard(_shard_2.id + 1).get().error().getCode());
}

TEST_F(TestFixture, SealShard) {
    for (auto i = 0; 2 > i; ++i) {
        auto tid = homeobject::generateRandomTraceId();
        auto e = homeobj_->shard_manager()->seal_shard(_shard_1.id, tid).get();
        ASSERT_TRUE(!!e);
        e.then([this](auto const& info) {
            EXPECT_TRUE(info.id == _shard_1.id);
            EXPECT_TRUE(info.placement_group == _shard_1.placement_group);
            EXPECT_EQ(info.state, ShardInfo::State::SEALED);
        });
    }
}
