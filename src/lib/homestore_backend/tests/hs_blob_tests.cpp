#include <chrono>
#include <cmath>
#include <mutex>

#include <boost/uuid/random_generator.hpp>
#include <gtest/gtest.h>

#define protected public

#include "lib/homestore_backend/hs_homeobject.hpp"
#include "lib/tests/fixture_app.hpp"
#include "bits_generator.hpp"

using namespace std::chrono_literals;

using homeobject::BlobError;
using homeobject::PGError;
using homeobject::PGInfo;
using homeobject::PGMember;
using homeobject::ShardError;
using namespace homeobject;

#define hex_bytes(buffer, len) fmt::format("{}", spdlog::to_hex((buffer), (buffer) + (len)))

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

    using blob_map_t = std::map< std::tuple< pg_id_t, shard_id_t, blob_id_t >, homeobject::Blob >;

    void put_blob(blob_map_t& blob_map, std::vector< std::pair< pg_id_t, shard_id_t > > const& pg_shard_id_vec,
                  uint64_t const num_blobs_per_shard, uint32_t const max_blob_size) {
        std::uniform_int_distribution< uint32_t > rand_blob_size{1u, max_blob_size};
        std::uniform_int_distribution< uint32_t > rand_user_key_size{1u, 1 * 1024};

        for (const auto& id : pg_shard_id_vec) {
            int64_t pg_id = id.first, shard_id = id.second;
            for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                uint32_t alignment = 512;
                // Create non 512 byte aligned address to create copy.
                if (k % 2 == 0) alignment = 256;

                std::string user_key;
                user_key.resize(rand_user_key_size(rnd_engine));
                BitsGenerator::gen_random_bits(user_key.size(), (uint8_t*)user_key.data());
                auto blob_size = rand_blob_size(rnd_engine);
                homeobject::Blob put_blob{sisl::io_blob_safe(blob_size, alignment), user_key, 42ul};
                BitsGenerator::gen_random_bits(put_blob.body);
                // Keep a copy of random payload to verify later.
                homeobject::Blob clone{sisl::io_blob_safe(blob_size, alignment), user_key, 42ul};
                std::memcpy(clone.body.bytes(), put_blob.body.cbytes(), put_blob.body.size());
                auto b = _obj_inst->blob_manager()->put(shard_id, std::move(put_blob)).get();
                ASSERT_TRUE(!!b);
                auto blob_id = b.value();

                LOGINFO("Put blob pg {} shard {} blob {} data {}", pg_id, shard_id, blob_id,
                        hex_bytes(clone.body.bytes(), std::min(10u, clone.body.size())));
                blob_map.insert({{pg_id, shard_id, blob_id}, std::move(clone)});
            }
        }
    }

    void verify_get_blob(blob_map_t const& blob_map, bool const use_random_offset = false) {
        uint32_t off = 0, len = 0;
        for (const auto& [id, blob] : blob_map) {
            int64_t pg_id = std::get< 0 >(id), shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
            len = blob.body.size();
            if (use_random_offset) {
                std::uniform_int_distribution< uint32_t > rand_off_gen{0u, blob.body.size() - 1u};
                std::uniform_int_distribution< uint32_t > rand_len_gen{1u, blob.body.size()};

                off = rand_off_gen(rnd_engine);
                len = rand_len_gen(rnd_engine);
                if ((off + len) >= blob.body.size()) { len = blob.body.size() - off; }
            }

            auto g = _obj_inst->blob_manager()->get(shard_id, blob_id, off, len).get();
            ASSERT_TRUE(!!g);
            auto result = std::move(g.value());
            LOGINFO("After restart get blob pg {} shard {} blob {} off {} len {} data {}", pg_id, shard_id, blob_id,
                    off, len, hex_bytes(result.body.bytes(), std::min(len, 10u)));
            EXPECT_EQ(result.body.size(), len);
            EXPECT_EQ(std::memcmp(result.body.bytes(), blob.body.cbytes() + off, result.body.size()), 0);
            EXPECT_EQ(result.user_key.size(), blob.user_key.size());
            EXPECT_EQ(blob.user_key, result.user_key);
            EXPECT_EQ(blob.object_off, result.object_off);
        }
    }

    void restart() {
        LOGINFO("Restarting homeobject.");
        _obj_inst.reset();
        _obj_inst = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
        std::this_thread::sleep_for(std::chrono::seconds{1});
    }
};

TEST_F(HomeObjectFixture, BasicPutGetDelBlobWRestart) {
    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;
    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::vector< std::pair< pg_id_t, shard_id_t > > pg_shard_id_vec;
    blob_map_t blob_map;

    // Create blob size in range (1, 16kb) and user key in range (1, 1kb)
    const uint32_t max_blob_size = 16 * 1024;

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
    put_blob(blob_map, pg_shard_id_vec, num_blobs_per_shard, max_blob_size);

    // Verify all get blobs
    verify_get_blob(blob_map);

    // for (uint64_t i = 1; i <= num_pgs; i++) {
    //     r_cast< HSHomeObject* >(_obj_inst.get())->print_btree_index(i);
    // }

    LOGINFO("Flushing CP.");
    trigger_cp(true /* wait */);

    // Restart homeobject
    restart();

    // Verify all get blobs after restart
    verify_get_blob(blob_map);

    // Put blob after restart to test the persistance of blob sequence number
    put_blob(blob_map, pg_shard_id_vec, num_blobs_per_shard, max_blob_size);

    // Verify all get blobs with random offset and length.
    verify_get_blob(blob_map, true /* use_random_offset */);

    // Delete all blobs
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->del(shard_id, blob_id).get();
        ASSERT_TRUE(g);
        LOGINFO("delete blob shard {} blob {}", shard_id, blob_id);
    }

    // After delete all blobs, get should fail
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
        ASSERT_TRUE(!g);
    }

    // all the deleted blobs should be tombstone in index table
    auto hs_homeobject = dynamic_cast< HSHomeObject* >(_obj_inst.get());
    for (const auto& [id, blob] : blob_map) {
        int64_t pg_id = std::get< 0 >(id), shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        shared< BlobIndexTable > index_table;
        {
            std::shared_lock lock_guard(hs_homeobject->_pg_lock);
            auto iter = hs_homeobject->_pg_map.find(pg_id);
            ASSERT_TRUE(iter != hs_homeobject->_pg_map.end());
            index_table = static_cast< HSHomeObject::HS_PG* >(iter->second.get())->index_table_;
        }

        auto g = hs_homeobject->get_blob_from_index_table(index_table, shard_id, blob_id);
        ASSERT_FALSE(!!g);
        EXPECT_EQ(BlobError::UNKNOWN_BLOB, g.error());
    }

    LOGINFO("Flushing CP.");
    trigger_cp(true /* wait */);

    // Restart homeobject
    restart();

    // After restart, for all deleted blobs, get should fail
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
        ASSERT_TRUE(!g);
    }
}

TEST_F(HomeObjectFixture, SealShardWithRestart) {
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

TEST_F(HomeObjectFixture, PGStatsTest) {
    // Create a pg, shard, put blob should succeed.
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

    // create a shard
    s = _obj_inst->shard_manager()->seal_shard(shard_id).get();
    ASSERT_TRUE(!!s);
    shard_info = s.value();
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::SEALED);
    LOGINFO("Sealed shard {}", shard_id);

    // create a 2nd shard
    auto s2 = _obj_inst->shard_manager()->create_shard(pg_id, 64 * Mi).get();
    auto shard_info2 = s2.value();
    auto shard_id2 = shard_info2.id;
    s2 = _obj_inst->shard_manager()->get_shard(shard_id2).get();
    ASSERT_TRUE(!!s);
    LOGINFO("Got shard {}", shard_id2);

    PGStats pg_stats;
    auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
    LOGINFO("stats: {}", pg_stats.to_string());

    EXPECT_EQ(res, true);
    EXPECT_EQ(pg_stats.id, pg_id);
    EXPECT_EQ(pg_stats.total_shards, 2);
    EXPECT_EQ(pg_stats.open_shards, 1);
    EXPECT_EQ(pg_stats.num_members, 3);

    auto stats = _obj_inst->get_stats();
    LOGINFO("HomeObj stats: {}", stats.to_string());
}
