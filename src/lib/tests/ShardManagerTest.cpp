#include <sisl/async/coro.hpp>

#include <homeobject/shard_manager.hpp>
#include "lib/tests/fixture_app.hpp"

using homeobject::shard_id_t;
using homeobject::ShardError;
using homeobject::ShardErrorCode;
using homeobject::ShardInfo;

TEST_F(TestFixture, CreateShardTooBig) {
    EXPECT_EQ(ShardErrorCode::INVALID_ARG,
              sisl::async::sync_get(homeobj_->shard_manager()->create_shard(
                                        _pg_id, homeobject::ShardManager::max_shard_size() + 1, "shard meta"))
                  .error()
                  .getCode());
}

TEST_F(TestFixture, CreateShardTooSmall) {
    EXPECT_EQ(
        ShardErrorCode::INVALID_ARG,
        sisl::async::sync_get(homeobj_->shard_manager()->create_shard(_pg_id, 0ul, "shard meta")).error().getCode());
}

TEST_F(TestFixture, CreateShardNoPg) {
    EXPECT_EQ(
        ShardErrorCode::UNKNOWN_PG,
        sisl::async::sync_get(homeobj_->shard_manager()->create_shard(_pg_id + 1, Mi, "shard meta")).error().getCode());
}

TEST_F(TestFixture, GetUnknownShard) {
    EXPECT_EQ(ShardErrorCode::UNKNOWN_SHARD,
              sisl::async::sync_get(homeobj_->shard_manager()->get_shard(_shard_2.id + 1)).error().getCode());
}

TEST_F(TestFixture, GetKnownShard) {
    auto e = sisl::async::sync_get(homeobj_->shard_manager()->get_shard(_shard_1.id));
    ASSERT_TRUE(!!e);
    EXPECT_TRUE(e->id == _shard_1.id);
    EXPECT_TRUE(e->placement_group == _shard_1.placement_group);
    EXPECT_EQ(e->state, ShardInfo::State::OPEN);
}

TEST_F(TestFixture, ListShardsNoPg) {
    EXPECT_EQ(ShardErrorCode::UNKNOWN_PG,
              sisl::async::sync_get(homeobj_->shard_manager()->list_shards(_pg_id + 1)).error().getCode());
}

TEST_F(TestFixture, ListShards) {
    auto e = sisl::async::sync_get(homeobj_->shard_manager()->list_shards(_pg_id));
    ASSERT_TRUE(!!e);
    ASSERT_EQ(e->size(), 2);
    EXPECT_TRUE(e->begin()->id == _shard_1.id);
    EXPECT_TRUE(e->begin()->placement_group == _shard_1.placement_group);
    EXPECT_EQ(e->begin()->state, ShardInfo::State::OPEN);
}

TEST_F(TestFixture, SealShardNoShard) {
    EXPECT_EQ(ShardErrorCode::UNKNOWN_SHARD,
              sisl::async::sync_get(homeobj_->shard_manager()->seal_shard(_shard_2.id + 1)).error().getCode());
}

TEST_F(TestFixture, SealShard) {
    for (auto i = 0; 2 > i; ++i) {
        auto tid = homeobject::generateRandomTraceId();
        auto e = sisl::async::sync_get(homeobj_->shard_manager()->seal_shard(_shard_1.id, tid));
        ASSERT_TRUE(!!e);
        EXPECT_TRUE(e->id == _shard_1.id);
        EXPECT_TRUE(e->placement_group == _shard_1.placement_group);
        EXPECT_EQ(e->state, ShardInfo::State::SEALED);
    }
}
