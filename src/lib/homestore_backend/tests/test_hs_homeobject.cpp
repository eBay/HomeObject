#include <chrono>
#include <cmath>
#include <condition_variable>
#include <mutex>

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <boost/uuid/random_generator.hpp>

#include "lib/homestore_backend/hs_homeobject.hpp"
#include "bits_generator.hpp"

using namespace std::chrono_literals;

using homeobject::BlobError;
using homeobject::PGError;
using homeobject::PGInfo;
using homeobject::PGMember;
using homeobject::ShardError;
using namespace homeobject;

#define hex_bytes(buffer, len) fmt::format("{}", spdlog::to_hex((buffer), (buffer) + (len)))

SISL_LOGGING_INIT(logging, HOMEOBJECT_LOG_MODS)
SISL_OPTIONS_ENABLE(logging, test_home_object)

SISL_OPTION_GROUP(
    test_home_object,
    (num_pgs, "", "num_pgs", "number of pgs", ::cxxopts::value< uint64_t >()->default_value("10"), "number"),
    (num_shards, "", "num_shards", "number of shards", ::cxxopts::value< uint64_t >()->default_value("20"), "number"),
    (num_blobs, "", "num_blobs", "number of blobs", ::cxxopts::value< uint64_t >()->default_value("50"), "number"));

class FixtureApp : public homeobject::HomeObjectApplication {
private:
    std::string fpath_{fmt::format("/tmp/test_home_object.data.{}", std::to_string(rand()))};

public:
    bool spdk_mode() const override { return false; }
    uint32_t threads() const override { return 2; }
    void set_restart(bool r = true) { restart_ = r; }
    std::list< std::filesystem::path > devices() const override {
        if (!restart_) {
            /* create files */
            LOGINFO("creating {} device file with size={}", fpath_, homestore::in_bytes(2 * Gi));
            if (std::filesystem::exists(fpath_)) { std::filesystem::remove(fpath_); }
            std::ofstream ofs{fpath_, std::ios::binary | std::ios::out | std::ios::trunc};
            std::filesystem::resize_file(fpath_, 2 * Gi);
        } else {
            LOGINFO("Skipping create device files");
        }

        auto device_info = std::list< std::filesystem::path >();
        device_info.emplace_back(std::filesystem::canonical(fpath_));
        return device_info;
    }

    ~FixtureApp() {
        if (!fpath_.empty()) { std::filesystem::remove(fpath_); }
    }

    homeobject::peer_id_t discover_svcid(std::optional< homeobject::peer_id_t > const&) const override {
        return boost::uuids::random_generator()();
    }

    /// TODO
    /// This will have to work if we test replication in the future
    std::string lookup_peer(homeobject::peer_id_t const&) const override { return "test_fixture.com"; }

private:
    bool restart_{false};
};

TEST(HomeObject, BasicEquivalence) {
    auto app = std::make_shared< FixtureApp >();
    auto obj_inst = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
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
    std::shared_ptr< FixtureApp > app;
    std::shared_ptr< homeobject::HomeObject > _obj_inst;
    std::random_device rnd{};
    std::default_random_engine rnd_engine{rnd()};

    void SetUp() override {
        app = std::make_shared< FixtureApp >();
        _obj_inst = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
    }

    void create_pg(pg_id_t pg_id) {
        auto info = homeobject::PGInfo(pg_id);
        auto peer1 = _obj_inst->our_uuid();
        auto peer2 = boost::uuids::random_generator()();
        auto peer3 = boost::uuids::random_generator()();
        info.members.insert(homeobject::PGMember{peer1, "peer1", 1});
        info.members.insert(homeobject::PGMember{peer2, "peer2", 0});
        info.members.insert(homeobject::PGMember{peer3, "peer3", 0});
        auto p = _obj_inst->pg_manager()->create_pg(std::move(info)).get();
        ASSERT_TRUE(!!p);
    }

    static void trigger_cp(bool wait) {
        auto fut = homestore::hs()->cp_mgr().trigger_cp_flush(true /* force */);
        auto on_complete = [&](auto success) {
            EXPECT_EQ(success, true);
            LOGINFO("CP Flush completed");
        };

        if (wait) {
            on_complete(std::move(fut).get());
        } else {
            std::move(fut).thenValue(on_complete);
        }
    }

    void restart() {
        LOGINFO("Restarting homeobject.");
        _obj_inst.reset();
        app->set_restart();
        _obj_inst = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
        std::this_thread::sleep_for(std::chrono::seconds{1});
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
              PGError::UNSUPPORTED_OP);
    EXPECT_EQ(ShardError::UNKNOWN_PG, _obj_inst->shard_manager()->create_shard(1, 1000).get().error());
    EXPECT_EQ(ShardError::INVALID_ARG, _obj_inst->shard_manager()->create_shard(1, 0).get().error());
    EXPECT_EQ(ShardError::INVALID_ARG, _obj_inst->shard_manager()->create_shard(1, 2 * Gi).get().error());
    EXPECT_EQ(ShardError::UNKNOWN_PG, _obj_inst->shard_manager()->list_shards(1).get().error());
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, _obj_inst->shard_manager()->get_shard(1).get().error());
    EXPECT_EQ(ShardError::UNKNOWN_SHARD, _obj_inst->shard_manager()->seal_shard(1).get().error());
}

TEST_F(HomeObjectFixture, PutBlobMissingShard) {
    EXPECT_EQ(
        BlobError::UNKNOWN_SHARD,
        _obj_inst->blob_manager()->put(1, homeobject::Blob{sisl::io_blob_safe(4096), "user_key", 0ul}).get().error());
}

TEST_F(HomeObjectFixture, GetBlobMissingShard) {
    EXPECT_EQ(BlobError::UNKNOWN_SHARD, _obj_inst->blob_manager()->get(1, 0u, 0ul, UINT64_MAX).get().error());
}

TEST_F(HomeObjectFixture, DeleteBlobMissingShard) {
    EXPECT_EQ(BlobError::UNKNOWN_SHARD, _obj_inst->blob_manager()->del(1, 0u).get().error());
}

TEST_F(HomeObjectFixture, BasicPutGetBlob) {
    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;
    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::vector< std::pair< pg_id_t, shard_id_t > > pg_shard_id_vec;
    std::map< std::tuple< pg_id_t, shard_id_t, blob_id_t >, homeobject::Blob > blob_map;

    // Create blob size in range (1, 16kb) and user key in range (1, 1kb)
    const uint32_t max_blob_size = 16 * 1024;
    std::uniform_int_distribution< uint32_t > rand_blob_size{1u, max_blob_size};
    std::uniform_int_distribution< uint32_t > rand_user_key_size{1u, 1 * 1024};

    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i /* pg_id */);
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = _obj_inst->shard_manager()->create_shard(i /* pg_id */, 64 * Mi).get();
            ASSERT_TRUE(!!shard);
            pg_shard_id_vec.emplace_back(i, shard->id);
            LOGINFO("pg {} shard {}", i, shard->id);
        }
    }

    // Put blob for all shards in all pg's.
    for (const auto& id : pg_shard_id_vec) {
        int64_t pg_id = id.first, shard_id = id.second;
        for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
            uint32_t alignment = 512;
            // Create non 512 byte aligned address to create copy.
            if (k % 2 == 0) alignment = 256;

            std::string user_key;
            user_key.reserve(rand_user_key_size(rnd_engine));
            BitsGenerator::gen_random_bits(user_key.size(), (uint8_t*)user_key.data());
            auto blob_size = rand_blob_size(rnd_engine);
            homeobject::Blob put_blob{sisl::io_blob_safe(blob_size, alignment), user_key, 0ul};
            BitsGenerator::gen_random_bits(put_blob.body);
            // Keep a copy of random payload to verify later.
            homeobject::Blob clone{sisl::io_blob_safe(blob_size, alignment), user_key, 0ul};
            std::memcpy(clone.body.bytes, put_blob.body.bytes, put_blob.body.size);
            auto b = _obj_inst->blob_manager()->put(shard_id, std::move(put_blob)).get();
            ASSERT_TRUE(!!b);
            auto blob_id = b.value();

            LOGINFO("Put blob pg {} shard {} blob {} data {}", pg_id, shard_id, blob_id,
                    hex_bytes(clone.body.bytes, std::min(10u, clone.body.size)));
            blob_map.insert({{pg_id, shard_id, blob_id}, std::move(clone)});
        }
    }

    // Verify all get blobs
    for (const auto& [id, blob] : blob_map) {
        int64_t pg_id = std::get< 0 >(id), shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
        ASSERT_TRUE(!!g);
        auto result = std::move(g.value());
        LOGINFO("Get blob pg {} shard id {} blob id {} size {} data {}", pg_id, shard_id, blob_id, result.body.size,
                hex_bytes(result.body.bytes, std::min(10u, result.body.size)));
        EXPECT_EQ(blob.body.size, result.body.size);
        EXPECT_EQ(std::memcmp(result.body.bytes, blob.body.bytes, result.body.size), 0);
        EXPECT_EQ(blob.user_key, result.user_key);
    }

    // for (uint64_t i = 1; i <= num_pgs; i++) {
    //     r_cast< HSHomeObject* >(_obj_inst.get())->print_btree_index(i);
    // }

    LOGINFO("Flushing CP.");
    trigger_cp(true /* wait */);

    // Restart homeobject
    restart();

    // Verify all get blobs after restart
    for (const auto& [id, blob] : blob_map) {
        int64_t pg_id = std::get< 0 >(id), shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
        ASSERT_TRUE(!!g);
        auto result = std::move(g.value());
        LOGINFO("After restart get blob pg {} shard {} blob {} data {}", pg_id, shard_id, blob_id,
                hex_bytes(result.body.bytes, std::min(10u, result.body.size)));
        EXPECT_EQ(result.body.size, blob.body.size);
        EXPECT_EQ(std::memcmp(result.body.bytes, blob.body.bytes, result.body.size), 0);
        EXPECT_EQ(blob.user_key, result.user_key);
    }

    // Verify all get blobs with random offset and length.
    for (const auto& [id, blob] : blob_map) {
        std::uniform_int_distribution< uint32_t > rand_off_gen{0u, blob.body.size - 1u};
        std::uniform_int_distribution< uint32_t > rand_len_gen{1u, blob.body.size};

        int64_t pg_id = std::get< 0 >(id), shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto off = rand_off_gen(rnd_engine);
        auto len = rand_len_gen(rnd_engine);
        if ((off + len) >= blob.body.size) { len = blob.body.size - off; }

        auto g = _obj_inst->blob_manager()->get(shard_id, blob_id, off, len).get();
        ASSERT_TRUE(!!g);
        auto result = std::move(g.value());
        LOGINFO("After restart get blob pg {} shard {} blob {} off {} len {} data {}", pg_id, shard_id, blob_id, off,
                len, hex_bytes(result.body.bytes, std::min(len, 10u)));
        EXPECT_EQ(result.body.size, len);
        EXPECT_EQ(std::memcmp(result.body.bytes, blob.body.bytes + off, result.body.size), 0);
        EXPECT_EQ(blob.user_key, result.user_key);
    }
}

TEST_F(HomeObjectFixture, SealShard) {
    // Create a pg, shard, put blob should succeed, seal and put blob again should fail.
    // Recover and put blob again should fail.
    pg_id_t pg_id{1};
    create_pg(pg_id);

    auto s = _obj_inst->shard_manager()->create_shard(pg_id, 64 * Mi).get();
    ASSERT_TRUE(!!s);
    auto shard_info = s.value();
    auto shard_id = shard_info.id;
    s = _obj_inst->shard_manager()->get_shard(shard_id).get();
    ASSERT_TRUE(!!s);

    LOGINFO("Got shard {}", shard_id);
    shard_info = s.value();
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::OPEN);
    auto b = _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
    ASSERT_TRUE(!!b);
    LOGINFO("Put blob {}", b.value());

    s = _obj_inst->shard_manager()->seal_shard(shard_id).get();
    ASSERT_TRUE(!!s);
    shard_info = s.value();
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::SEALED);
    LOGINFO("Sealed shard {}", shard_id);

    b = _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
    ASSERT_TRUE(!b);
    ASSERT_EQ(b.error(), BlobError::SEALED_SHARD);
    LOGINFO("Put blob {}", b.error());

    // Restart homeobject
    restart();

    // Verify shard is sealed.
    s = _obj_inst->shard_manager()->get_shard(shard_id).get();
    ASSERT_TRUE(!!s);

    LOGINFO("After restart shard {}", shard_id);
    shard_info = s.value();
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::SEALED);

    b = _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
    ASSERT_TRUE(!b);
    ASSERT_EQ(b.error(), BlobError::SEALED_SHARD);
    LOGINFO("Put blob {}", b.error());
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_home_object);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);
    return RUN_ALL_TESTS();
}
