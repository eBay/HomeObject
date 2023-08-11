#include <chrono>
#include <condition_variable>
#include <mutex>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <boost/uuid/random_generator.hpp>

#include "homeobject/homeobject.hpp"
#include "homeobject/blob_manager.hpp"
#include "homeobject/pg_manager.hpp"
#include "homeobject/shard_manager.hpp"

using namespace std::chrono_literals;

using homeobject::BlobError;
using homeobject::PGError;
using homeobject::ShardError;

SISL_LOGGING_INIT(logging, homeobject)
SISL_OPTIONS_ENABLE(logging)

TEST(HomeObject, BasicEquivalence) {
    auto obj_inst = homeobject::init_homeobject(
        homeobject::init_params{[](homeobject::peer_id const&) -> std::string { return "test"; }});
    ASSERT_TRUE(!!obj_inst);
    auto shard_mgr = obj_inst->shard_manager();
    auto pg_mgr = obj_inst->pg_manager();
    auto blob_mgr = obj_inst->blob_manager();
    EXPECT_EQ(obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(shard_mgr.get()));
    EXPECT_EQ(obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(pg_mgr.get()));
    EXPECT_EQ(obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(blob_mgr.get()));
}

class HomeObjectFixture : public ::testing::Test {
public:
    std::shared_ptr< homeobject::HomeObject > _obj_inst;

    void SetUp() override {
        _obj_inst = homeobject::init_homeobject(
            homeobject::init_params{[](homeobject::peer_id const&) -> std::string { return "test_fixture"; }});
    }
};

// TODO: This test should actually not fail assuming initialization succeeded,
// but until it is implemented we will just assume the RAFT group was
// unresponsive and timed-out.
TEST_F(HomeObjectFixture, CreatePgTimeout) {
    EXPECT_EQ(_obj_inst->pg_manager()->create_pg(homeobject::PGInfo{0l}).get(), PGError::TIMEOUT);
}

TEST_F(HomeObjectFixture, ReplaceMemberMissingPg) {
    EXPECT_EQ(_obj_inst->pg_manager()
                  ->replace_member(0, boost::uuids::random_generator()(),
                                   homeobject::PGMember{boost::uuids::random_generator()(), "new_member", 1})
                  .get(),
              PGError::UNKNOWN_PG);
}

TEST_F(HomeObjectFixture, CreateShardMissingPg) {
    auto v = _obj_inst->shard_manager()->create_shard(1, 1000).get();
    ASSERT_TRUE(std::holds_alternative< ShardError >(v));
    EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_PG);
}

TEST_F(HomeObjectFixture, CreateShardZeroSize) {
    auto v = _obj_inst->shard_manager()->create_shard(1, 0).get();
    ASSERT_TRUE(std::holds_alternative< ShardError >(v));
    EXPECT_EQ(std::get< ShardError >(v), ShardError::INVALID_ARG);
}

TEST_F(HomeObjectFixture, CreateShardTooBig) {
    auto v = _obj_inst->shard_manager()->create_shard(1, 2 * Gi).get();
    ASSERT_TRUE(std::holds_alternative< ShardError >(v));
    EXPECT_EQ(std::get< ShardError >(v), ShardError::INVALID_ARG);
}

TEST_F(HomeObjectFixture, ListShardsUnknownPg) {
    auto v = _obj_inst->shard_manager()->list_shards(1).get();
    ASSERT_TRUE(std::holds_alternative< ShardError >(v));
    EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_PG);
}

TEST_F(HomeObjectFixture, GetUnknownShard) {
    auto v = _obj_inst->shard_manager()->get_shard(1);
    ASSERT_TRUE(std::holds_alternative< ShardError >(v));
    EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_SHARD);
}

TEST_F(HomeObjectFixture, SealUnknownShard) {
    auto v = _obj_inst->shard_manager()->seal_shard(1).get();
    ASSERT_TRUE(std::holds_alternative< ShardError >(v));
    EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_SHARD);
}

TEST_F(HomeObjectFixture, PutBlobMissingShard) {
    auto v = _obj_inst->blob_manager()
                 ->put(1, homeobject::Blob{std::make_unique< sisl::byte_array_impl >(4096), "user_key", 0ul})
                 .get();
    ASSERT_TRUE(std::holds_alternative< BlobError >(v));
    EXPECT_EQ(std::get< BlobError >(v), BlobError::UNKNOWN_SHARD);
}

TEST_F(HomeObjectFixture, GetBlobMissingShard) {
    auto v = _obj_inst->blob_manager()->get(1, 0u, 0ul, UINT64_MAX).get();
    ASSERT_TRUE(std::holds_alternative< BlobError >(v));
    EXPECT_EQ(std::get< BlobError >(v), BlobError::UNKNOWN_SHARD);
}

TEST_F(HomeObjectFixture, DeleteBlobMissingShard) {
    EXPECT_EQ(_obj_inst->blob_manager()->del(1, 0u).get(), BlobError::UNKNOWN_SHARD);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger("test_homeobject");

    return RUN_ALL_TESTS();
}
