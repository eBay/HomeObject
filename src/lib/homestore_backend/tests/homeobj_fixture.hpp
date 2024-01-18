#pragma once
#include <chrono>
#include <cmath>
#include <mutex>

#include <boost/uuid/random_generator.hpp>
#include <gtest/gtest.h>

#define protected public
#include <homestore/homestore.hpp>
#include "lib/homestore_backend/hs_homeobject.hpp"
#include "lib/homestore_backend/hs_hmobj_cp.hpp"
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

    void TearDown() override { app->clean(); }

    void create_pg(pg_id_t pg_id) {
        auto info = homeobject::PGInfo(pg_id);
        auto peer1 = _obj_inst->our_uuid();
        info.members.insert(homeobject::PGMember{peer1, "peer1", 1});

        // TODO:: add the following back when we have 3-replica raft test framework
        /*
        auto peer2 = boost::uuids::random_generator()();
        auto peer3 = boost::uuids::random_generator()();
        info.members.insert(homeobject::PGMember{peer2, "peer2", 0});
        info.members.insert(homeobject::PGMember{peer3, "peer3", 0});
        */

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
                std::memcpy(clone.body.bytes(), put_blob.body.bytes(), put_blob.body.size());
                auto b = _obj_inst->blob_manager()->put(shard_id, std::move(put_blob)).get();
                ASSERT_TRUE(!!b);
                auto blob_id = b.value();

                LOGINFO("Put blob pg {} shard {} blob {} data {}", pg_id, shard_id, blob_id,
                        hex_bytes(clone.body.cbytes(), std::min(10u, clone.body.size())));
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
                    off, len, hex_bytes(result.body.cbytes(), std::min(len, 10u)));
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
