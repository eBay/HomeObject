#include <string>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

// will allow unit tests to access object private/protected for validation;
#define protected public

#include "lib/homestore/homeobject.hpp"
#include "lib/homestore/replication_message.hpp"
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

TEST_F(ShardManagerTesting, GetUnknownShard) {
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, _home_object->shard_manager()->get_shard(_shard_id).get().error());
}

TEST_F(ShardManagerTesting, ListShardsNoPg) {
    EXPECT_EQ(ShardError::UNKNOWN_PG, _home_object->shard_manager()->list_shards(_pg_id + 1).get().error());
}

TEST_F(ShardManagerTesting, ListShardsOnEmptyPg) {
    auto e = _home_object->shard_manager()->list_shards(_pg_id).get();
    ASSERT_TRUE(!!e);
    e.then([this](auto const& info_list) { ASSERT_EQ(info_list.size(), 0); });
}

// Disable following cases temporary as they will create real shards using HS ReplDev and
// depends on another PR to create PG using HS ReplDev instead of MockReplicaSet
/*
TEST_F(ShardManagerTesting, CreateShardSuccess) {
    auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
    ASSERT_TRUE(!!e);
    ShardInfo shard_info = e.value();
    EXPECT_EQ(ShardInfo::State::OPEN, shard_info.state);
    EXPECT_EQ(Mi, shard_info.total_capacity_bytes);
    EXPECT_EQ(Mi, shard_info.available_capacity_bytes);
    EXPECT_EQ(0ul, shard_info.deleted_capacity_bytes);
    EXPECT_EQ(_pg_id, shard_info.placement_group);
}

TEST_F(ShardManagerTesting, CreateShardAndValidateMembers) {
    auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
    ASSERT_TRUE(!!e);
    ShardInfo shard_info = e.value();
    homeobject::HSHomeObject* ho = dynamic_cast< homeobject::HSHomeObject* >(_home_object.get());
    EXPECT_TRUE(ho != nullptr);
    auto pg_iter = ho->_pg_map.find(_pg_id);
    EXPECT_TRUE(pg_iter != ho->_pg_map.end());
    auto& pg = pg_iter->second;
    EXPECT_TRUE(pg.shard_sequence_num == 1);
    EXPECT_EQ(1, pg.shards.size());
    auto& shard = *pg.shards.begin();
    EXPECT_TRUE(shard.info == shard_info);
    EXPECT_TRUE(shard.metablk_cookie != nullptr);
}

TEST_F(ShardManagerTesting, GetKnownShard) {
    auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
    ASSERT_TRUE(!!e);
    ShardInfo shard_info = e.value();
    auto future = _home_object->shard_manager()->get_shard(shard_info.id).get();
    future.then([this, shard_info](auto const& info) {
        EXPECT_TRUE(info.id == shard_info.id);
        EXPECT_TRUE(info.placement_group == _pg_id);
        EXPECT_EQ(info.state, ShardInfo::State::OPEN);
    });
}

TEST_F(ShardManagerTesting, ListShards) {
    auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
    ASSERT_TRUE(!!e);
    ShardInfo shard_info = e.value();
    auto list_shard_result = _home_object->shard_manager()->list_shards(_pg_id).get();
    ASSERT_TRUE(!!list_shard_result);
    list_shard_result.then([this, shard_info](auto const& info_list) {
        ASSERT_EQ(info_list.size(), 1);
        EXPECT_TRUE(info_list.begin()->id == shard_info.id);
        EXPECT_TRUE(info_list.begin()->placement_group == _pg_id);
        EXPECT_EQ(info_list.begin()->state, ShardInfo::State::OPEN);
    });
}

TEST_F(ShardManagerTesting, SealUnknownShard) {
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, _home_object->shard_manager()->seal_shard(1000).get().error());
}

TEST_F(ShardManagerTesting, MockSealShard) {
    auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
    ASSERT_TRUE(!!e);
    ShardInfo shard_info = e.value();
    auto shard = homeobject::Shard(shard_info);
    shard.info.state = ShardInfo::State::SEALED;
    nlohmann::json j;
    j["shard_info"]["shard_id"] = shard.info.id;
    j["shard_info"]["pg_id"] = shard.info.placement_group;
    j["shard_info"]["state"] = shard.info.state;
    j["shard_info"]["created_time"] = shard.info.created_time;
    j["shard_info"]["modified_time"] = shard.info.last_modified_time;
    j["shard_info"]["total_capacity"] = shard.info.total_capacity_bytes;
    j["shard_info"]["available_capacity"] = shard.info.available_capacity_bytes;
    j["shard_info"]["deleted_capacity"] = shard.info.deleted_capacity_bytes;
    j["ext_info"]["chunk_id"] = shard.chunk_id;
    auto seal_shard_msg = j.dump();

    homeobject::ReplicationMessageHeader header;
    header.repl_group_id = _pg_id;
    header.msg_type = homeobject::ReplicationMessageType::SEAL_SHARD_MSG;
    header.payload_size = seal_shard_msg.size();
    header.payload_crc =
        crc32_ieee(homeobject::init_crc32, r_cast< const uint8_t* >(seal_shard_msg.c_str()), seal_shard_msg.size());
    header.header_crc = header.calculate_crc();
    sisl::sg_list value;
    value.size = seal_shard_msg.size();
    value.iovs.push_back(iovec(r_cast< void* >(const_cast< char* >(seal_shard_msg.c_str())), seal_shard_msg.size()));

    // header is corrupted with crc;
    ++header.header_crc;
    homeobject::HSHomeObject* ho = dynamic_cast< homeobject::HSHomeObject* >(_home_object.get());
    auto pg = ho->_get_pg(_pg_id);
    ASSERT_TRUE(!!pg);
    homestore::ReplicationService* replication_service =
        (homestore::ReplicationService*)(&homestore::HomeStore::instance()->repl_service());
    auto repl_dev = replication_service->get_replica_dev(pg.value().repl_dev_uuid);

    {
        auto [p, sf] = folly::makePromiseContract< homeobject::ShardManager::Result< ShardInfo > >();
        repl_dev.value()->async_alloc_write(sisl::blob(r_cast< uint8_t* >(&header), sizeof(header)), sisl::blob(),
                                            value, static_cast< void* >(&p));
        auto info = std::move(sf).get();
        EXPECT_FALSE(info);
    }

    // everything is fine;
    header.header_crc = header.calculate_crc();
    auto [p, sf] = folly::makePromiseContract< homeobject::ShardManager::Result< ShardInfo > >();
    repl_dev.value()->async_alloc_write(sisl::blob(r_cast< uint8_t* >(&header), sizeof(header)), sisl::blob(), value,
                                        static_cast< void* >(&p));
    auto info = std::move(sf).get();
    EXPECT_TRUE(info);
    EXPECT_TRUE(info.value().id == shard_info.id);
    EXPECT_TRUE(info.value().placement_group == _pg_id);
    EXPECT_EQ(info.value().state, ShardInfo::State::SEALED);

    auto pg_iter = ho->_pg_map.find(_pg_id);
    EXPECT_TRUE(pg_iter != ho->_pg_map.end());
    auto& pg_result = pg_iter->second;
    EXPECT_EQ(1, pg_result.shards.size());
    auto& check_shard = *pg_result.shards.begin();
    EXPECT_EQ(ShardInfo::State::SEALED, check_shard.info.state);
    EXPECT_TRUE(check_shard.metablk_cookie != nullptr);
}

TEST_F(ShardManagerTesting, ShardManagerRecovery) {
    auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
    ASSERT_TRUE(!!e);
    ShardInfo shard_info = e.value();
    EXPECT_EQ(ShardInfo::State::OPEN, shard_info.state);
    EXPECT_EQ(Mi, shard_info.total_capacity_bytes);
    EXPECT_EQ(Mi, shard_info.available_capacity_bytes);
    EXPECT_EQ(0ul, shard_info.deleted_capacity_bytes);
    EXPECT_EQ(_pg_id, shard_info.placement_group);

    nlohmann::json shard_json;
    shard_json["shard_info"]["shard_id"] = shard_info.id;
    shard_json["shard_info"]["pg_id"] = shard_info.placement_group;
    shard_json["shard_info"]["state"] = shard_info.state;
    shard_json["shard_info"]["created_time"] = shard_info.created_time;
    shard_json["shard_info"]["modified_time"] = shard_info.last_modified_time;
    shard_json["shard_info"]["total_capacity"] = shard_info.total_capacity_bytes;
    shard_json["shard_info"]["available_capacity"] = shard_info.available_capacity_bytes;
    shard_json["shard_info"]["deleted_capacity"] = shard_info.deleted_capacity_bytes;
    shard_json["ext_info"]["chunk_id"] = 100;
    auto shard_msg = shard_json.dump();

    // Manual remove shard info from home_object and relay on metablk service to replay it back;
    homeobject::HSHomeObject* ho = dynamic_cast< homeobject::HSHomeObject* >(_home_object.get());
    auto pg_iter = ho->_pg_map.find(_pg_id);
    EXPECT_TRUE(pg_iter != ho->_pg_map.end());
    auto& pg = pg_iter->second;
    EXPECT_EQ(1, pg.shards.size());
    auto& check_shard = *pg.shards.begin();
    void* saved_metablk = check_shard.metablk_cookie;
    pg_iter->second.shards.clear();
    ho->_shard_map.clear();
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, _home_object->shard_manager()->get_shard(_shard_id).get().error());

    auto buf = sisl::make_byte_array(static_cast< uint32_t >(shard_msg.size()), 0, sisl::buftag::metablk);
    std::memcpy(buf->bytes, shard_msg.c_str(), shard_msg.size());
    ho->on_shard_meta_blk_found(static_cast< homestore::meta_blk* >(saved_metablk), buf, shard_msg.size());
    // check the recover result;
    auto future = _home_object->shard_manager()->get_shard(shard_info.id).get();
    EXPECT_TRUE(!!future);
    future.then([this, shard_info](auto const& info) {
        EXPECT_TRUE(info.id == shard_info.id);
        EXPECT_TRUE(info.placement_group == _pg_id);
        EXPECT_EQ(info.state, ShardInfo::State::OPEN);
    });
}

class FixtureAppWithRecovery : public FixtureApp {
public:
    std::list< std::filesystem::path > devices() const override {
        const std::string fpath{"/tmp/test_homestore.data"};
        if (!std::filesystem::exists(fpath)) {
            LOGINFO("creating device files with size {} ", 1, homestore::in_bytes(2 * Gi));
            LOGINFO("creating {} device file", fpath);
            std::ofstream ofs{fpath, std::ios::binary | std::ios::out | std::ios::trunc};
            std::filesystem::resize_file(fpath, 2 * Gi);
        }
        auto device_info = std::list< std::filesystem::path >();
        device_info.emplace_back(std::filesystem::canonical(fpath));
        return device_info;
    }
};

class ShardManagerTestingRecovery : public ::testing::Test {
public:
    void SetUp() override { app = std::make_shared< FixtureAppWithRecovery >(); }

protected:
    std::shared_ptr< FixtureApp > app;
};

TEST_F(ShardManagerTestingRecovery, ShardManagerRecoveryV2) {
    // clear the env first;
    const std::string fpath{"/tmp/test_homestore.data"};
    LOGINFO("creating {} device file", fpath);
    if (std::filesystem::exists(fpath)) { std::filesystem::remove(fpath); }
    homeobject::pg_id _pg_id{1u};
    homeobject::peer_id _peer1;
    homeobject::peer_id _peer2;
    std::shared_ptr< homeobject::HomeObject > _home_object;
    _home_object = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
    _peer1 = _home_object->our_uuid();
    _peer2 = boost::uuids::random_generator()();

    auto info = homeobject::PGInfo(_pg_id);
    info.members.insert(homeobject::PGMember{_peer1, "peer1", 1});
    info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});
    EXPECT_TRUE(_home_object->pg_manager()->create_pg(std::move(info)).get());
    // create one shard;
    auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
    ASSERT_TRUE(!!e);

    ShardInfo shard_info = e.value();
    EXPECT_EQ(ShardInfo::State::OPEN, shard_info.state);
    EXPECT_EQ(Mi, shard_info.total_capacity_bytes);
    EXPECT_EQ(Mi, shard_info.available_capacity_bytes);
    EXPECT_EQ(0ul, shard_info.deleted_capacity_bytes);
    EXPECT_EQ(_pg_id, shard_info.placement_group);
    homeobject::HSHomeObject* ho = dynamic_cast< homeobject::HSHomeObject* >(_home_object.get());
    auto pg_iter = ho->_pg_map.find(_pg_id);
    EXPECT_TRUE(pg_iter != ho->_pg_map.end());
    auto& pg_result = pg_iter->second;
    EXPECT_EQ(1, pg_result.shards.size());
    auto check_shard = *pg_result.shards.begin();
    EXPECT_EQ(ShardInfo::State::OPEN, check_shard.info.state);
    EXPECT_TRUE(check_shard.metablk_cookie != nullptr);

    // release the homeobject and homestore will be shutdown automatically.
    _home_object.reset();

    // re-create the homeobject and pg infos and shard infos will be recover automatically.
    _home_object = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
    ho = dynamic_cast< homeobject::HSHomeObject* >(_home_object.get());
    EXPECT_TRUE(ho->_pg_map.size() == 1);

    // check basic shard info first;
    auto recovery_shard = _home_object->shard_manager()->get_shard(shard_info.id).get();
    EXPECT_TRUE(recovery_shard);
    EXPECT_TRUE(recovery_shard.value() == shard_info);

    // check shard internal state;
    pg_iter = ho->_pg_map.find(_pg_id);
    EXPECT_TRUE(pg_iter != ho->_pg_map.end());
    EXPECT_EQ(1, pg_iter->second.shards.size());
    EXPECT_EQ(check_shard.chunk_id, pg_iter->second.shards.front().chunk_id);
    EXPECT_TRUE(pg_iter->second.shards.front().metablk_cookie != nullptr);
}
*/

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
