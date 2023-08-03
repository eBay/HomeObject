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
    struct call_tracker {
        std::mutex call_lock;
        std::condition_variable call_cond;
        bool called{false};

        void signal() {
            auto lg = std::scoped_lock< std::mutex >(call_lock);
            called = true;
            call_cond.notify_all();
        }

        bool wait(auto const& dur) {
            auto lg = std::unique_lock< std::mutex >(call_lock);
            return call_cond.wait_for(lg, dur, [this] { return called; });
        }
    };

public:
    call_tracker _t;
    std::shared_ptr< homeobject::HomeObject > _obj_inst;

    void SetUp() override {
        _obj_inst = homeobject::init_homeobject(
            homeobject::init_params{[](homeobject::peer_id const&) -> std::string { return "test_fixture"; }});
    }
    void TearDown() override { EXPECT_TRUE(_t.wait(100ms)); }
};

// TODO: This test should actually not fail assuming initialization succeeded,
// but until it is implemented we will just assume the RAFT group was
// unresponsive and timed-out.
TEST_F(HomeObjectFixture, CreatePgTimeout) {
    _obj_inst->pg_manager()->create_pg(0l, [this](auto const& e) {
        EXPECT_EQ(e, PGError::TIMEOUT);
        _t.signal();
    });
}

TEST_F(HomeObjectFixture, ReplaceMemberMissingPg) {
    _obj_inst->pg_manager()->replace_member(0, boost::uuids::random_generator()(),
                                            homeobject::PGMember{boost::uuids::random_generator()(), "new_member", 1},
                                            [this](auto const& e) {
                                                EXPECT_EQ(e, PGError::UNKNOWN_PG);
                                                _t.signal();
                                            });
}

TEST_F(HomeObjectFixture, CreateShardMissingPg) {
    _obj_inst->shard_manager()->create_shard(1, 1000, [this](auto const& v, auto opt) {
        ASSERT_TRUE(std::holds_alternative< ShardError >(v));
        EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_PG);
        _t.signal();
    });
}

TEST_F(HomeObjectFixture, CreateShardZeroSize) {
    _obj_inst->shard_manager()->create_shard(1, 0, [this](auto const& v, auto opt) {
        ASSERT_TRUE(std::holds_alternative< ShardError >(v));
        EXPECT_EQ(std::get< ShardError >(v), ShardError::INVALID_ARG);
        _t.signal();
    });
}

TEST_F(HomeObjectFixture, CreateShardTooBig) {
    _obj_inst->shard_manager()->create_shard(1, 2 * Gi, [this](auto const& v, auto opt) {
        ASSERT_TRUE(std::holds_alternative< ShardError >(v));
        EXPECT_EQ(std::get< ShardError >(v), ShardError::INVALID_ARG);
        _t.signal();
    });
}

TEST_F(HomeObjectFixture, ListShardsUnknownPg) {
    _obj_inst->shard_manager()->list_shards(1, [this](auto const& v, auto opt) {
        ASSERT_TRUE(std::holds_alternative< ShardError >(v));
        EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_PG);
        _t.signal();
    });
}

TEST_F(HomeObjectFixture, GetUnknownShard) {
    _obj_inst->shard_manager()->get_shard(1, [this](auto const& v, auto opt) {
        ASSERT_TRUE(std::holds_alternative< ShardError >(v));
        EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_SHARD);
        _t.signal();
    });
}

TEST_F(HomeObjectFixture, SealUnknownShard) {
    _obj_inst->shard_manager()->seal_shard(1, [this](auto const& v, auto opt) {
        ASSERT_TRUE(std::holds_alternative< ShardError >(v));
        EXPECT_EQ(std::get< ShardError >(v), ShardError::UNKNOWN_SHARD);
        _t.signal();
    });
}

TEST_F(HomeObjectFixture, PutBlobMissingShard) {
    _obj_inst->blob_manager()->put(1,
                                   homeobject::Blob{std::make_unique< sisl::byte_array_impl >(4096), "user_key", 0ul},
                                   [this](auto const& v, auto opt) {
                                       ASSERT_TRUE(std::holds_alternative< BlobError >(v));
                                       EXPECT_EQ(std::get< BlobError >(v), BlobError::UNKNOWN_SHARD);
                                       _t.signal();
                                   });
}

TEST_F(HomeObjectFixture, GetBlobMissingShard) {
    _obj_inst->blob_manager()->get(1, 0u, 0ul, UINT64_MAX, [this](auto const& v, auto opt) {
        ASSERT_TRUE(std::holds_alternative< BlobError >(v));
        EXPECT_EQ(std::get< BlobError >(v), BlobError::UNKNOWN_SHARD);
        _t.signal();
    });
}

TEST_F(HomeObjectFixture, DeleteBlobMissingShard) {
    _obj_inst->blob_manager()->del(1, 0u, [this](auto const& e, auto opt) {
        EXPECT_EQ(e, BlobError::UNKNOWN_SHARD);
        _t.signal();
    });
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger("test_homeobject");

    return RUN_ALL_TESTS();
}
