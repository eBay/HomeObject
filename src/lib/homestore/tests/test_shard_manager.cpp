#include <string>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "lib/homestore/homeobject.hpp"
#include "lib/homestore/replication_state_machine.hpp"
#include "mocks/mock_replica_set.hpp"

using homeobject::shard_id;
using homeobject::ShardError;
using homeobject::ShardInfo;

SISL_LOGGING_INIT(logging, HOMEOBJECT_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

class FixtureApp : public homeobject::HomeObjectApplication {
public:
    bool spdk_mode() const override { return false; }
    uint32_t threads() const override { return 2; }
    std::list< std::filesystem::path > devices() const override {
        /* create files */
        LOGINFO("creating device files with size {} ", 1, homestore::in_bytes(2 * Gi));
        const std::string fpath{"/tmp/test_homestore.data"};
        LOGINFO("creating {} device file", fpath);
        if (std::filesystem::exists(fpath)) { std::filesystem::remove(fpath); }
        std::ofstream ofs{fpath, std::ios::binary | std::ios::out | std::ios::trunc};
        std::filesystem::resize_file(fpath, 2 * Gi);

        auto device_info = std::list< std::filesystem::path >();
        device_info.emplace_back(std::filesystem::canonical(fpath));
        return device_info;
    }
    homeobject::peer_id discover_svcid(std::optional< homeobject::peer_id > const&) const override {
        return boost::uuids::random_generator()();
    }
    std::string lookup_peer(homeobject::peer_id const&) const override { return "test_fixture.com"; }
};

class ShardManagerTesting : public ::testing::Test {
public:
    homeobject::pg_id _pg_id{1u};
    homeobject::peer_id _peer1;
    homeobject::peer_id _peer2;
    homeobject::shard_id _shard_id{100u};

    void SetUp() override {
        app = std::make_shared< FixtureApp >();
        _home_object = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
        _peer1 = _home_object->our_uuid();
        _peer2 = boost::uuids::random_generator()();

        auto info = homeobject::PGInfo(_pg_id);
        info.members.insert(homeobject::PGMember{_peer1, "peer1", 1});
        info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});
        EXPECT_TRUE(_home_object->pg_manager()->create_pg(std::move(info)).get());
    }
protected:
    std::shared_ptr< FixtureApp > app;
    std::shared_ptr< homeobject::HomeObject > _home_object;
};


TEST_F(ShardManagerTesting, CreateShardTooBig) {
    EXPECT_EQ(ShardError::INVALID_ARG,
              _home_object->shard_manager()
                  ->create_shard(_pg_id, homeobject::ShardManager::max_shard_size() + 1)
                  .get()
                  .error());
}

TEST_F(ShardManagerTesting, CreateShardTooSmall) {
    EXPECT_EQ(ShardError::INVALID_ARG, _home_object->shard_manager()->create_shard(_pg_id, 0ul).get().error());
}

TEST_F(ShardManagerTesting, CreateShardWithUnknownPG) {
    EXPECT_EQ(ShardError::UNKNOWN_PG, _home_object->shard_manager()->create_shard(_pg_id + 1, Mi).get().error());
}

TEST_F(ShardManagerTesting, CreateShardWithPGNotLeader) {
    homeobject::HSHomeObject* ho = dynamic_cast<homeobject::HSHomeObject*>(_home_object.get());
    EXPECT_TRUE(ho != nullptr);
    auto rs = ho->get_repl_svc()->get_replica_set(fmt::format("{}", _pg_id));
    EXPECT_TRUE(std::holds_alternative<home_replication::rs_ptr_t>(rs));
    auto replicaset = dynamic_cast<home_replication::MockReplicaSet*>(std::get<home_replication::rs_ptr_t>(rs).get());
    replicaset->set_follower();
    EXPECT_EQ(ShardError::NOT_LEADER, _home_object->shard_manager()->create_shard(_pg_id, Mi).get().error());
}

TEST_F(ShardManagerTesting, GetUnknownShard) {
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, _home_object->shard_manager()->get_shard(_shard_id).get().error());
}

TEST_F(ShardManagerTesting, ListShardsNoPg) {
    EXPECT_EQ(ShardError::UNKNOWN_PG, _home_object->shard_manager()->list_shards(_pg_id + 1).get().error());
}

TEST_F(ShardManagerTesting, ListShardsOnEmptyPg) {
    auto e = _home_object->shard_manager()->list_shards(_pg_id).get();
    ASSERT_TRUE(!!e);
    e.then([this](auto const& info_list) {
        ASSERT_EQ(info_list.size(), 0);
    });
}

class ShardManagerWithShardsTesting : public ShardManagerTesting {
    std::shared_ptr<home_replication::ReplicaSetListener> _listener;
public:
    void SetUp() override {
        ShardManagerTesting::SetUp();
        homeobject::HSHomeObject* ho = dynamic_cast<homeobject::HSHomeObject*>(_home_object.get());
        EXPECT_TRUE(ho != nullptr);
        auto rs = ho->get_repl_svc()->get_replica_set(fmt::format("{}", _pg_id));
        EXPECT_TRUE(std::holds_alternative<home_replication::rs_ptr_t>(rs));
        auto replica_set = dynamic_cast<home_replication::MockReplicaSet*>(std::get<home_replication::rs_ptr_t>(rs).get());
        _listener = std::make_shared<homeobject::ReplicationStateMachine>(ho);
        replica_set->set_listener(_listener);
    }
};

TEST_F(ShardManagerWithShardsTesting, CreateShardSuccess) {
     auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
     ASSERT_TRUE(!!e);
     ShardInfo shard_info = e.value();
     EXPECT_EQ(ShardInfo::State::OPEN, shard_info.state);
     EXPECT_EQ(Mi, shard_info.total_capacity_bytes);
     EXPECT_EQ(Mi, shard_info.available_capacity_bytes);
     EXPECT_EQ(0ul, shard_info.deleted_capacity_bytes);
     EXPECT_EQ(_pg_id, shard_info.placement_group);    
}

TEST_F(ShardManagerWithShardsTesting, GetKnownShard) {
     auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
     ASSERT_TRUE(!!e);
     ShardInfo shard_info = e.value();
     auto future = _home_object->shard_manager()->get_shard(shard_info.id).get();
     ASSERT_TRUE(!!e);
     future.then([this, shard_info](auto const& info) {
        EXPECT_TRUE(info.id == shard_info.id);
        EXPECT_TRUE(info.placement_group == _pg_id);
        EXPECT_EQ(info.state, ShardInfo::State::OPEN);
     });
}

TEST_F(ShardManagerWithShardsTesting, ListShards) {
    auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
    ASSERT_TRUE(!!e);
    ShardInfo shard_info = e.value();  
    auto list_shard_result  = _home_object->shard_manager()->list_shards(_pg_id).get();
    ASSERT_TRUE(!!list_shard_result);
    list_shard_result.then([this, shard_info](auto const& info_list) {
        ASSERT_EQ(info_list.size(), 1);
        EXPECT_TRUE(info_list.begin()->id == shard_info.id);
        EXPECT_TRUE(info_list.begin()->placement_group == _pg_id);
        EXPECT_EQ(info_list.begin()->state, ShardInfo::State::OPEN);
    });
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
