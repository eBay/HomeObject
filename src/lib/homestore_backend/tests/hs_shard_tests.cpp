#include <string>

#include <boost/uuid/random_generator.hpp>

#include <homestore/blkdata_service.hpp>
#include <homestore/logstore_service.hpp>

#include <gtest/gtest.h>

// will allow unit tests to access object private/protected for validation;
#define protected public

#include "lib/homestore_backend/hs_homeobject.hpp"
#include "lib/homestore_backend/replication_message.hpp"
#include "lib/homestore_backend/replication_state_machine.hpp"

#include "lib/tests/fixture_app.hpp"

using homeobject::shard_id_t;
using homeobject::ShardError;
using homeobject::ShardInfo;
using homeobject::ShardManager;

TEST_F(TestFixture, CreateMultiShards) {
    homeobject::HSHomeObject* ho = dynamic_cast< homeobject::HSHomeObject* >(homeobj_.get());
    auto chunk_num_1 = ho->get_shard_chunk(_shard_1.id);
    ASSERT_TRUE(chunk_num_1.has_value());

    auto chunk_num_2 = ho->get_shard_chunk(_shard_2.id);
    ASSERT_TRUE(chunk_num_2.has_value());

    // check if both chunk is on the same pdev;
    auto alloc_hint1 = ho->chunk_selector()->chunk_to_hints(chunk_num_1.value());
    auto alloc_hint2 = ho->chunk_selector()->chunk_to_hints(chunk_num_2.value());
    ASSERT_TRUE(alloc_hint1.pdev_id_hint.has_value());
    ASSERT_TRUE(alloc_hint2.pdev_id_hint.has_value());
    ASSERT_TRUE(alloc_hint1.pdev_id_hint.value() == alloc_hint2.pdev_id_hint.value());
}

TEST_F(TestFixture, CreateMultiShardsOnMultiPG) {
    // create another PG;
    auto peer1 = homeobj_->our_uuid();
    // auto peer2 = boost::uuids::random_generator()();

    auto new_pg_id = static_cast< homeobject::pg_id_t >(_pg_id + 1);
    auto info = homeobject::PGInfo(_pg_id + 1);
    info.members.insert(homeobject::PGMember{peer1, "peer1", 1});
    // info.members.insert(homeobject::PGMember{peer2, "peer2", 0});
    EXPECT_TRUE(homeobj_->pg_manager()->create_pg(std::move(info)).get());

    std::vector< homeobject::pg_id_t > pgs{_pg_id, new_pg_id};

    for (const auto pg : pgs) {
        auto e = homeobj_->shard_manager()->create_shard(pg, Mi).get();
        ASSERT_TRUE(!!e);
        homeobject::HSHomeObject* ho = dynamic_cast< homeobject::HSHomeObject* >(homeobj_.get());
        auto chunk_num_1 = ho->get_shard_chunk(e.value().id);
        ASSERT_TRUE(chunk_num_1.has_value());

        // create another shard again.
        e = homeobj_->shard_manager()->create_shard(_pg_id, Mi).get();
        ASSERT_TRUE(!!e);
        auto chunk_num_2 = ho->get_shard_chunk(e.value().id);
        ASSERT_TRUE(chunk_num_2.has_value());

        // check if both chunk is on the same pdev;
        auto alloc_hint1 = ho->chunk_selector()->chunk_to_hints(chunk_num_1.value());
        auto alloc_hint2 = ho->chunk_selector()->chunk_to_hints(chunk_num_2.value());
        ASSERT_TRUE(alloc_hint1.pdev_id_hint.has_value());
        ASSERT_TRUE(alloc_hint2.pdev_id_hint.has_value());
        ASSERT_TRUE(alloc_hint1.pdev_id_hint.value() == alloc_hint2.pdev_id_hint.value());
    }
}

#if 0
TEST_F(TestFixture, MockSealShard) {
    ShardInfo shard_info = _shard_1;
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

    homeobject::HSHomeObject* ho = dynamic_cast< homeobject::HSHomeObject* >(homeobj_.get());
    auto* pg = s_cast< homeobject::HSHomeObject::HS_PG* >(ho->_pg_map[_pg_id].get());
    auto repl_dev = pg->repl_dev_;
    const auto msg_size = sisl::round_up(seal_shard_msg.size(), repl_dev->get_blk_size());
    auto req = homeobject::repl_result_ctx< ShardManager::Result< ShardInfo > >::make(msg_size, 512 /*alignment*/);
    auto buf_ptr = req->hdr_buf_.bytes();
    std::memset(buf_ptr, 0, msg_size);
    std::memcpy(buf_ptr, seal_shard_msg.c_str(), seal_shard_msg.size());

    req->header_.msg_type = homeobject::ReplicationMessageType::SEAL_SHARD_MSG;
    req->header_.pg_id = _pg_id;
    req->header_.shard_id = shard_info.id;
    req->header_.payload_size = msg_size;
    req->header_.payload_crc = crc32_ieee(homeobject::init_crc32, buf_ptr, msg_size);
    req->header_.seal();
    sisl::blob header;
    header.set_bytes(r_cast< uint8_t* >(&req->header_));
    header.set_size(sizeof(req->header_));
    sisl::sg_list value;
    value.size = msg_size;
    value.iovs.push_back(iovec(buf_ptr, msg_size));
    repl_dev->async_alloc_write(header, sisl::blob{buf_ptr, (uint32_t)msg_size}, value, req);
    auto info = req->result().get();
    EXPECT_TRUE(info);
    EXPECT_TRUE(info.value().id == shard_info.id);
    EXPECT_TRUE(info.value().placement_group == _pg_id);
    EXPECT_EQ(info.value().state, ShardInfo::State::SEALED);

    auto pg_iter = ho->_pg_map.find(_pg_id);
    EXPECT_TRUE(pg_iter != ho->_pg_map.end());
    auto& pg_result = pg_iter->second;
    EXPECT_EQ(2, pg_result->shards_.size());
    auto& check_shard = pg_result->shards_.front();
    EXPECT_EQ(ShardInfo::State::SEALED, check_shard->info.state);
}
#endif

class FixtureAppWithRecovery : public FixtureApp {
    std::string fpath_{"/tmp/test_shard_manager.data." + std::to_string(rand())};

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

    void TearDown() override { app->clean(); }

protected:
    std::shared_ptr< FixtureApp > app;
};

// TODO: enable the following test case after we fix raft repl dev recovery issue.
/*
TEST_F(ShardManagerTestingRecovery, ShardManagerRecovery) {
    // prepare the env first;
    auto app_with_recovery = dp_cast< FixtureAppWithRecovery >(app);
    const std::string fpath = app_with_recovery->path();
    if (std::filesystem::exists(fpath)) { std::filesystem::remove(fpath); }
    LOGI("creating device files with size {} ", homestore::in_bytes(2 * Gi));
    LOGI("creating {} device file", fpath);
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
    // info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});
    EXPECT_TRUE(_home_object->pg_manager()->create_pg(std::move(info)).get());
    // create one shard;
    auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
    ASSERT_TRUE(!!e);
    ShardInfo shard_info = e.value();
    auto shard_id = shard_info.id;
    EXPECT_EQ(ShardInfo::State::OPEN, shard_info.state);
    EXPECT_EQ(Mi, shard_info.total_capacity_bytes);
    EXPECT_EQ(Mi, shard_info.available_capacity_bytes);
    EXPECT_EQ(0ul, shard_info.deleted_capacity_bytes);
    EXPECT_EQ(_pg_id, shard_info.placement_group);

    // restart homeobject and check if pg/shard info will be recovered.
    _home_object.reset();
    LOGI("restart home_object");
    _home_object = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
    homeobject::HSHomeObject* ho = dynamic_cast< homeobject::HSHomeObject* >(_home_object.get());
    // check PG after recovery.
    EXPECT_TRUE(ho->_pg_map.size() == 1);
    auto pg_iter = ho->_pg_map.find(_pg_id);
    EXPECT_TRUE(pg_iter != ho->_pg_map.end());
    auto& pg_result = pg_iter->second;
    EXPECT_EQ(1, pg_result->shards_.size());
    // verify the sequence number is correct after recovery.
    EXPECT_EQ(1, pg_result->shard_sequence_num_);
    // check recovered shard state.
    auto check_shard = pg_result->shards_.front().get();
    EXPECT_EQ(ShardInfo::State::OPEN, check_shard->info.state);

    auto hs_shard = d_cast< homeobject::HSHomeObject::HS_Shard* >(check_shard);
    EXPECT_TRUE(hs_shard->info == shard_info);
    EXPECT_TRUE(hs_shard->sb_->id == shard_info.id);
    EXPECT_TRUE(hs_shard->sb_->placement_group == shard_info.placement_group);
    EXPECT_TRUE(hs_shard->sb_->state == shard_info.state);
    EXPECT_TRUE(hs_shard->sb_->created_time == shard_info.created_time);
    EXPECT_TRUE(hs_shard->sb_->last_modified_time == shard_info.last_modified_time);
    EXPECT_TRUE(hs_shard->sb_->available_capacity_bytes == shard_info.available_capacity_bytes);
    EXPECT_TRUE(hs_shard->sb_->total_capacity_bytes == shard_info.total_capacity_bytes);
    EXPECT_TRUE(hs_shard->sb_->deleted_capacity_bytes == shard_info.deleted_capacity_bytes);

    // seal the shard when shard is recovery
    e = _home_object->shard_manager()->seal_shard(shard_id).get();
    ASSERT_TRUE(!!e);
    EXPECT_EQ(ShardInfo::State::SEALED, e.value().state);

    // restart again to verify the shards has expected states.
    _home_object.reset();
    LOGI("restart home_object again");
    // re-create the homeobject and pg infos and shard infos will be recover automatically.
    _home_object = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
    auto s = _home_object->shard_manager()->get_shard(shard_id).get();
    ASSERT_TRUE(!!s);
    EXPECT_EQ(ShardInfo::State::SEALED, s.value().state);
    ho = dynamic_cast< homeobject::HSHomeObject* >(_home_object.get());
    pg_iter = ho->_pg_map.find(_pg_id);
    // verify the sequence number is correct after recovery.
    EXPECT_EQ(1, pg_iter->second->shard_sequence_num_);

    // re-create new shards on this pg works too even homeobject is restarted twice.
    e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
    ASSERT_TRUE(!!e);
    EXPECT_NE(shard_id, e.value().id);
    EXPECT_EQ(ShardInfo::State::OPEN, e.value().state);
    EXPECT_EQ(2, pg_iter->second->shard_sequence_num_);
    // finally close the homeobject and homestore.
    _home_object.reset();
    std::filesystem::remove(fpath);
}

TEST_F(ShardManagerTestingRecovery, SealedShardRecovery) {
    // prepare the env first;
    auto app_with_recovery = dp_cast< FixtureAppWithRecovery >(app);
    const std::string fpath = app_with_recovery->path();
    if (std::filesystem::exists(fpath)) { std::filesystem::remove(fpath); }
    LOGI("creating device files with size {} ", homestore::in_bytes(2 * Gi));
    LOGI("creating {} device file", fpath);
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
    // info.members.insert(homeobject::PGMember{_peer2, "peer2", 0});
    EXPECT_TRUE(_home_object->pg_manager()->create_pg(std::move(info)).get());
    // create one shard;
    auto e = _home_object->shard_manager()->create_shard(_pg_id, Mi).get();
    ASSERT_TRUE(!!e);
    auto shard_id = e.value().id;
    e = _home_object->shard_manager()->seal_shard(shard_id).get();
    ASSERT_TRUE(!!e);
    auto shard_info = e.value();
    EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);

    // check the shard info from ShardManager to make sure on_commit() is successfully.
    homeobject::HSHomeObject* ho = dynamic_cast< homeobject::HSHomeObject* >(_home_object.get());
    auto pg_iter = ho->_pg_map.find(_pg_id);
    EXPECT_TRUE(pg_iter != ho->_pg_map.end());
    auto& pg_result = pg_iter->second;
    EXPECT_EQ(1, pg_result->shards_.size());
    auto check_shard = pg_result->shards_.front().get();
    EXPECT_EQ(ShardInfo::State::SEALED, check_shard->info.state);
    // release the homeobject and homestore will be shutdown automatically.
    _home_object.reset();

    LOGI("restart home_object");
    // re-create the homeobject and pg infos and shard infos will be recover automatically.
    _home_object = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
    ho = dynamic_cast< homeobject::HSHomeObject* >(_home_object.get());
    EXPECT_TRUE(ho->_pg_map.size() == 1);
    // check shard internal state;
    pg_iter = ho->_pg_map.find(_pg_id);
    EXPECT_TRUE(pg_iter != ho->_pg_map.end());
    EXPECT_EQ(1, pg_iter->second->shards_.size());
    auto hs_shard = d_cast< homeobject::HSHomeObject::HS_Shard* >(pg_iter->second->shards_.front().get());
    EXPECT_TRUE(hs_shard->info == shard_info);
    EXPECT_TRUE(hs_shard->sb_->id == shard_info.id);
    EXPECT_TRUE(hs_shard->sb_->placement_group == shard_info.placement_group);
    EXPECT_TRUE(hs_shard->sb_->state == shard_info.state);
    EXPECT_TRUE(hs_shard->sb_->created_time == shard_info.created_time);
    EXPECT_TRUE(hs_shard->sb_->last_modified_time == shard_info.last_modified_time);
    EXPECT_TRUE(hs_shard->sb_->available_capacity_bytes == shard_info.available_capacity_bytes);
    EXPECT_TRUE(hs_shard->sb_->total_capacity_bytes == shard_info.total_capacity_bytes);
    EXPECT_TRUE(hs_shard->sb_->deleted_capacity_bytes == shard_info.deleted_capacity_bytes);
    // finally close the homeobject and homestore.
    _home_object.reset();
    std::filesystem::remove(fpath);
}
*/
