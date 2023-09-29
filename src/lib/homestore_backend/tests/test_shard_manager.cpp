#include <string>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/init/Init.h>

#include <homestore/blkdata_service.hpp>
#include <homestore/logstore_service.hpp>

#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

// will allow unit tests to access object private/protected for validation;
#define protected public

#include "lib/homestore_backend/hs_homeobject.hpp"
#include "lib/homestore_backend/replication_message.hpp"
#include "lib/homestore_backend/replication_state_machine.hpp"

using homeobject::shard_id_t;
using homeobject::ShardError;
using homeobject::ShardInfo;
using homeobject::ShardManager;

SISL_LOGGING_INIT(logging, HOMEOBJECT_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

class FixtureApp : public homeobject::HomeObjectApplication {
private:
    std::string fpath_{"/tmp/test_shard_manager.data.{}" + std::to_string(rand())};

public:
    bool spdk_mode() const override { return false; }
    uint32_t threads() const override { return 2; }
    std::list< std::filesystem::path > devices() const override {
        LOGINFO("creating {} device file with size={}", fpath_, homestore::in_bytes(2 * Gi));
        if (std::filesystem::exists(fpath_)) { std::filesystem::remove(fpath_); }
        std::ofstream ofs{fpath_, std::ios::binary | std::ios::out | std::ios::trunc};
        std::filesystem::resize_file(fpath_, 2 * Gi);

        auto device_info = std::list< std::filesystem::path >();
        device_info.emplace_back(std::filesystem::canonical(fpath_));
        return device_info;
    }

    ~FixtureApp() {
        if (std::filesystem::exists(fpath_)) { std::filesystem::remove(fpath_); }
    }

    homeobject::peer_id_t discover_svcid(std::optional< homeobject::peer_id_t > const&) const override {
        return boost::uuids::random_generator()();
    }
    std::string lookup_peer(homeobject::peer_id_t const&) const override { return "test_fixture.com"; }
};

class ShardManagerTesting : public ::testing::Test {
public:
    homeobject::pg_id_t _pg_id{1u};
    homeobject::peer_id_t _peer1;
    homeobject::peer_id_t _peer2;
    homeobject::shard_id_t _shard_id{100u};

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
    EXPECT_TRUE(pg->shard_sequence_num_ == 1);
    EXPECT_EQ(1, pg->shards_.size());
    auto& shard = *pg->shards_.begin();
    EXPECT_TRUE(shard->info == shard_info);
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
    shard_info.state = ShardInfo::State::SEALED;

    nlohmann::json j;
    j["shard_info"]["shard_id_t"] = shard_info.id;
    j["shard_info"]["pg_id_t"] = shard_info.placement_group;
    j["shard_info"]["state"] = shard_info.state;
    j["shard_info"]["created_time"] = shard_info.created_time;
    j["shard_info"]["modified_time"] = shard_info.last_modified_time;
    j["shard_info"]["total_capacity"] = shard_info.total_capacity_bytes;
    j["shard_info"]["available_capacity"] = shard_info.available_capacity_bytes;
    j["shard_info"]["deleted_capacity"] = shard_info.deleted_capacity_bytes;
    auto seal_shard_msg = j.dump();

    homeobject::HSHomeObject* ho = dynamic_cast< homeobject::HSHomeObject* >(_home_object.get());
    auto pg = dp_cast< homeobject::HSHomeObject::HS_PG >(ho->_pg_map[_pg_id]);
    auto repl_dev = pg->repl_dev_;
    const auto msg_size = sisl::round_up(seal_shard_msg.size(), repl_dev->get_blk_size());
    auto req = homeobject::repl_result_ctx< ShardManager::Result< ShardInfo > >::make(msg_size, 512 /*alignment*/);
    auto buf_ptr = req->hdr_buf_.bytes;
    std::memset(buf_ptr, 0, msg_size);
    std::memcpy(buf_ptr, seal_shard_msg.c_str(), seal_shard_msg.size());

    req->header_.msg_type = homeobject::ReplicationMessageType::SEAL_SHARD_MSG;
    req->header_.pg_id = _pg_id;
    req->header_.shard_id = shard_info.id;
    req->header_.payload_size = msg_size;
    req->header_.payload_crc = crc32_ieee(homeobject::init_crc32, buf_ptr, msg_size);
    req->header_.seal();
    sisl::blob header;
    header.bytes = r_cast< uint8_t* >(&req->header_);
    header.size = sizeof(req->header_);
    sisl::sg_list value;
    value.size = msg_size;
    value.iovs.push_back(iovec(buf_ptr, msg_size));
    repl_dev->async_alloc_write(header, sisl::blob{}, value, req);
    auto info = req->result().get();
    EXPECT_TRUE(info);
    EXPECT_TRUE(info.value().id == shard_info.id);
    EXPECT_TRUE(info.value().placement_group == _pg_id);
    EXPECT_EQ(info.value().state, ShardInfo::State::SEALED);

    auto pg_iter = ho->_pg_map.find(_pg_id);
    EXPECT_TRUE(pg_iter != ho->_pg_map.end());
    auto pg_result = pg_iter->second;
    EXPECT_EQ(1, pg_result->shards_.size());
    auto& check_shard = pg_result->shards_.front();
    EXPECT_EQ(ShardInfo::State::SEALED, check_shard->info.state);
}

class FixtureAppWithRecovery : public FixtureApp {
    std::string fpath_{"/tmp/test_shard_manager.data.{}" + std::to_string(rand())};

public:
    std::list< std::filesystem::path > devices() const override {
        auto device_info = std::list< std::filesystem::path >();
        device_info.emplace_back(std::filesystem::canonical(fpath_));
        return device_info;
    }

    std::string path() const { return fpath_; }
};

class ShardManagerTestingRecovery : public ::testing::Test {
public:
    void SetUp() override { app = std::make_shared< FixtureAppWithRecovery >(); }

protected:
    std::shared_ptr< FixtureApp > app;
};

TEST_F(ShardManagerTestingRecovery, ShardManagerRecovery) {
    // prepare the env first;
    auto app_with_recovery = dp_cast< FixtureAppWithRecovery >(app);
    const std::string fpath = app_with_recovery->path();
    if (std::filesystem::exists(fpath)) { std::filesystem::remove(fpath); }
    LOGINFO("creating device files with size {} ", homestore::in_bytes(2 * Gi));
    LOGINFO("creating {} device file", fpath);
    std::ofstream ofs{fpath, std::ios::binary | std::ios::out | std::ios::trunc};
    std::filesystem::resize_file(fpath, 2 * Gi);

    homeobject::pg_id_t _pg_id{1u};
    homeobject::peer_id_t _peer1;
    homeobject::peer_id_t _peer2;
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
    EXPECT_EQ(1, pg_result->shards_.size());
    auto check_shard = pg_result->shards_.front();
    EXPECT_EQ(ShardInfo::State::OPEN, check_shard->info.state);
    // release the homeobject and homestore will be shutdown automatically.
    _home_object.reset();

    LOGINFO("restart home_object");
    // re-create the homeobject and pg infos and shard infos will be recover automatically.
    _home_object = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
    ho = dynamic_cast< homeobject::HSHomeObject* >(_home_object.get());
    EXPECT_TRUE(ho->_pg_map.size() == 1);

    // check shard internal state;
    pg_iter = ho->_pg_map.find(_pg_id);
    EXPECT_TRUE(pg_iter != ho->_pg_map.end());
    EXPECT_EQ(1, pg_iter->second->shards_.size());
    auto hs_shard = dp_cast< homeobject::HSHomeObject::HS_Shard >(pg_iter->second->shards_.front());
    EXPECT_TRUE(hs_shard->info == shard_info);
    EXPECT_TRUE(hs_shard->sb_->id == shard_info.id);
    EXPECT_TRUE(hs_shard->sb_->placement_group == shard_info.placement_group);
    EXPECT_TRUE(hs_shard->sb_->state == shard_info.state);
    EXPECT_TRUE(hs_shard->sb_->created_time == shard_info.created_time);
    EXPECT_TRUE(hs_shard->sb_->last_modified_time == shard_info.last_modified_time);
    EXPECT_TRUE(hs_shard->sb_->available_capacity_bytes == shard_info.available_capacity_bytes);
    EXPECT_TRUE(hs_shard->sb_->total_capacity_bytes == shard_info.total_capacity_bytes);
    EXPECT_TRUE(hs_shard->sb_->deleted_capacity_bytes == shard_info.deleted_capacity_bytes);
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
