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
using homeobject::PGInfo;
using homeobject::PGMember;
using homeobject::ShardError;

SISL_LOGGING_INIT(logging, HOMEOBJECT_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

TEST(HomeObject, BasicEquivalence) {
    auto obj_inst = homeobject::init_homeobject(
        homeobject::HomeObject::init_params{[](std::optional< homeobject::peer_id > const&) {
                                                return folly::makeSemiFuture(boost::uuids::random_generator()());
                                            },
                                            [](homeobject::peer_id const&) { return "test"; }});
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
            homeobject::HomeObject::init_params{[](std::optional< homeobject::peer_id > const&) {
                                                    return folly::makeSemiFuture(boost::uuids::random_generator()());
                                                },
                                                [](homeobject::peer_id const&) { return "test_fixture"; }});
    }
};

TEST_F(HomeObjectFixture, TestValidations) {
    EXPECT_EQ(_obj_inst->pg_manager()->create_pg(PGInfo(0u)).get().error(), PGError::INVALID_ARG);
    auto info = PGInfo(0u);
    info.members.insert(PGMember{boost::uuids::random_generator()()});
    EXPECT_EQ(_obj_inst->pg_manager()->create_pg(std::move(info)).get().error(), PGError::INVALID_ARG);
    EXPECT_EQ(_obj_inst->pg_manager()
                  ->replace_member(0, boost::uuids::random_generator()(),
                                   homeobject::PGMember{boost::uuids::random_generator()(), "new_member", 1})
                  .get()
                  .error(),
              PGError::UNKNOWN_PG);
    EXPECT_EQ(ShardError::UNKNOWN_PG, _obj_inst->shard_manager()->create_shard(1, 1000).get().error());
    EXPECT_EQ(ShardError::INVALID_ARG, _obj_inst->shard_manager()->create_shard(1, 0).get().error());
    EXPECT_EQ(ShardError::INVALID_ARG, _obj_inst->shard_manager()->create_shard(1, 2 * Gi).get().error());
    EXPECT_EQ(ShardError::UNKNOWN_PG, _obj_inst->shard_manager()->list_shards(1).error());
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, _obj_inst->shard_manager()->get_shard(1).error());
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, _obj_inst->shard_manager()->seal_shard(1).get().error());
}

TEST_F(HomeObjectFixture, PutBlobMissingShard) {
    EXPECT_EQ(BlobError::UNKNOWN_SHARD,
              _obj_inst->blob_manager()
                  ->put(1, homeobject::Blob{std::make_unique< sisl::byte_array_impl >(4096), "user_key", 0ul})
                  .get()
                  .error());
}

TEST_F(HomeObjectFixture, GetBlobMissingShard) {
    EXPECT_EQ(BlobError::UNKNOWN_SHARD, _obj_inst->blob_manager()->get(1, 0u, 0ul, UINT64_MAX).get().error());
}

TEST_F(HomeObjectFixture, DeleteBlobMissingShard) {
    EXPECT_EQ(BlobError::UNKNOWN_SHARD, _obj_inst->blob_manager()->del(1, 0u).get().error());
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger("test_homeobject");

    return RUN_ALL_TESTS();
}
