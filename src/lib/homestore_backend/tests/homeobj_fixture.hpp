#pragma once
#include <chrono>
#include <cmath>
#include <mutex>

#include <boost/uuid/random_generator.hpp>
#include <gtest/gtest.h>

#include <homestore/homestore.hpp>
// will allow unit tests to access object private/protected for validation;
#define protected public
#define private public

#include "lib/homestore_backend/hs_homeobject.hpp"
#include "bits_generator.hpp"
#include "hs_repl_test_helper.hpp"

using namespace std::chrono_literals;

using homeobject::BlobError;
using homeobject::PGError;
using homeobject::PGInfo;
using homeobject::PGMember;
using homeobject::ShardError;
using namespace homeobject;

#define hex_bytes(buffer, len) fmt::format("{}", spdlog::to_hex((buffer), (buffer) + (len)))

extern std::unique_ptr< test_common::HSReplTestHelper > g_helper;

class HomeObjectFixture : public ::testing::Test {
public:
    std::shared_ptr< homeobject::HSHomeObject > _obj_inst;

    // Create blob size in range (1, 16kb) and user key in range (1, 1kb)
    HomeObjectFixture() : rand_blob_size{1u, 16 * 1024}, rand_user_key_size{1u, 1024} {}

    void SetUp() override {
        _obj_inst = std::dynamic_pointer_cast< HSHomeObject >(g_helper->build_new_homeobject());
        g_helper->sync();
    }

    void TearDown() override {
        g_helper->sync();
        _obj_inst.reset();
        g_helper->delete_homeobject();
    }

    void restart() {
        g_helper->sync();
        trigger_cp(true);
        _obj_inst.reset();
        _obj_inst = std::dynamic_pointer_cast< HSHomeObject >(g_helper->restart());
        // wait for leader to be elected
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    // schedule create_pg to replica_num
    void create_pg(pg_id_t pg_id, uint32_t replica_num = 0) {
        if (replica_num == g_helper->replica_num()) {
            auto memebers = g_helper->members();
            auto name = g_helper->name();
            auto info = homeobject::PGInfo(pg_id);
            for (const auto& member : memebers) {
                if (replica_num == member.second) {
                    // by default, leader is the first member
                    info.members.insert(homeobject::PGMember{member.first, name + std::to_string(member.second), 1});
                } else {
                    info.members.insert(homeobject::PGMember{member.first, name + std::to_string(member.second), 0});
                }
            }
            auto p = _obj_inst->pg_manager()->create_pg(std::move(info)).get();
            ASSERT_TRUE(p);
            LOGINFO("pg {} is created at leader", pg_id);
        } else {
            // follower need to wait for pg creation to complete locally
            while (!pg_exist(pg_id)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            LOGINFO("pg {} is created at follower", pg_id);
        }
    }

    ShardInfo create_shard(pg_id_t pg_id, uint64_t size_bytes) {
        g_helper->sync();
        // schedule create_shard only on leader
        run_on_pg_leader(pg_id, [&]() {
            auto s = _obj_inst->shard_manager()->create_shard(pg_id, size_bytes).get();
            RELEASE_ASSERT(!!s, "failed to create shard");
            auto ret = s.value();
            g_helper->set_uint64_id(ret.id);
        });

        // wait for create_shard finished on leader and shard_id set to the uint64_id in IPC.
        while (g_helper->get_uint64_id() == INVALID_UINT64_ID) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        // get shard_id from IPC
        auto shard_id = g_helper->get_uint64_id();

        // all the members need to wait for shard creation to complete locally
        while (!shard_exist(shard_id)) {
            // for leader, shard creation is done locally and will nor reach here. but for follower, we need to wait for
            // shard creation to complete locally
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        auto r = _obj_inst->shard_manager()->get_shard(shard_id).get();
        RELEASE_ASSERT(!!r, "failed to get shard {}", shard_id);
        return r.value();
    }

    ShardInfo seal_shard(shard_id_t shard_id) {
        // before seal shard, we need to wait all the memebers to complete shard state verification
        g_helper->sync();
        auto r = _obj_inst->shard_manager()->get_shard(shard_id).get();
        RELEASE_ASSERT(!!r, "failed to get shard {}", shard_id);
        auto pg_id = r.value().placement_group;

        run_on_pg_leader(pg_id, [&]() {
            auto s = _obj_inst->shard_manager()->seal_shard(shard_id).get();
            RELEASE_ASSERT(!!s, "failed to seal shard");
        });

        while (true) {
            auto r = _obj_inst->shard_manager()->get_shard(shard_id).get();
            RELEASE_ASSERT(!!r, "failed to get shard {}", shard_id);
            auto shard_info = r.value();
            if (shard_info.state == ShardInfo::State::SEALED) { return shard_info; }
            // for leader, shard sealing is done locally and will nor reach here. but for follower, we need to wait for
            // shard is sealed locally
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

    void put_blob(shard_id_t shard_id, Blob&& blob) {
        g_helper->sync();
        auto r = _obj_inst->shard_manager()->get_shard(shard_id).get();
        RELEASE_ASSERT(!!r, "failed to get shard {}", shard_id);
        auto pg_id = r.value().placement_group;

        run_on_pg_leader(pg_id, [&]() {
            auto b = _obj_inst->blob_manager()->put(shard_id, std::move(blob)).get();
            RELEASE_ASSERT(!!b, "failed to pub blob");
            g_helper->set_uint64_id(b.value());
        });

        while (g_helper->get_uint64_id() == INVALID_UINT64_ID) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        auto blob_id = g_helper->get_uint64_id();

        // make sure the blob is created locally
        while (!blob_exist(shard_id, blob_id)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

    // TODO:make this run in parallel
    void put_blobs(std::map< pg_id_t, std::vector< shard_id_t > > const& pg_shard_id_vec,
                   uint64_t const num_blobs_per_shard, std::map< pg_id_t, blob_id_t >& pg_blob_id) {
        g_helper->sync();
        for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
            // the blob_id of a pg is a continuous number starting from 0 and increasing by 1
            blob_id_t current_blob_id{pg_blob_id[pg_id]};

            for (const auto& shard_id : shard_vec) {
                for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                    run_on_pg_leader(pg_id, [&]() {
                        auto put_blob = build_blob(current_blob_id);

                        LOGINFO("Put blob pg {} shard {} blob {} size {} data {}", pg_id, shard_id, current_blob_id,
                                put_blob.body.size(),
                                hex_bytes(put_blob.body.cbytes(), std::min(10u, put_blob.body.size())));

                        auto b = _obj_inst->blob_manager()->put(shard_id, std::move(put_blob)).get();

                        if (!b) {
                            LOGERROR("Failed to put blob pg {} shard {} error={}", pg_id, shard_id, b.error());
                            ASSERT_TRUE(false);
                        }

                        auto blob_id = b.value();
                        ASSERT_EQ(blob_id, current_blob_id) << "the predicted blob id is not correct!";
                    });

                    current_blob_id++;
                }
            }

            // wait for the last blob to be created locally
            auto shard_id = shard_vec.back();
            pg_blob_id[pg_id] = current_blob_id;
            auto last_blob_id = pg_blob_id[pg_id] - 1;
            while (!blob_exist(shard_id, last_blob_id)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            LOGINFO("shard {} blob {} is created locally, which means all the blob before {} are created", shard_id,
                    last_blob_id, last_blob_id);
        }
    }

    // TODO:make this run in parallel
    void del_all_blobs(std::map< pg_id_t, std::vector< shard_id_t > > const& pg_shard_id_vec,
                       uint64_t const num_blobs_per_shard, std::map< pg_id_t, blob_id_t >& pg_blob_id) {
        g_helper->sync();
        for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
            run_on_pg_leader(pg_id, [&]() {
                blob_id_t current_blob_id{0};
                for (; current_blob_id < pg_blob_id[pg_id];) {
                    for (const auto& shard_id : shard_vec) {
                        for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                            auto g = _obj_inst->blob_manager()->del(shard_id, current_blob_id).get();
                            ASSERT_TRUE(g);
                            LOGINFO("delete blob shard {} blob {}", shard_id, current_blob_id);
                            current_blob_id++;
                        }
                    }
                }
            });
            auto shard_id = shard_vec.back();
            auto blob_id = pg_blob_id[pg_id] - 1;
            while (blob_exist(shard_id, blob_id)) {
                LOGINFO("waiting for shard {} blob {} to be deleted locally", shard_id, blob_id);
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
        }
    }

    // TODO:make this run in parallel
    void verify_get_blob(std::map< pg_id_t, std::vector< shard_id_t > > const& pg_shard_id_vec,
                         uint64_t const num_blobs_per_shard, bool const use_random_offset = false) {
        uint32_t off = 0, len = 0;

        for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
            blob_id_t current_blob_id{0};
            for (const auto& shard_id : shard_vec) {
                for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                    auto blob = build_blob(current_blob_id);
                    len = blob.body.size();
                    if (use_random_offset) {
                        std::uniform_int_distribution< uint32_t > rand_off_gen{0u, len - 1u};
                        std::uniform_int_distribution< uint32_t > rand_len_gen{1u, len};

                        off = rand_off_gen(rnd_engine);
                        len = rand_len_gen(rnd_engine);
                        if ((off + len) >= blob.body.size()) { len = blob.body.size() - off; }
                    }

                    auto g = _obj_inst->blob_manager()->get(shard_id, current_blob_id, off, len).get();
                    ASSERT_TRUE(!!g) << "get blob fail, shard_id " << shard_id << " blob_id " << current_blob_id
                                     << " replica number " << g_helper->replica_num();
                    auto result = std::move(g.value());
                    LOGINFO("get blob pg {} shard {} blob {} off {} len {} data {}", pg_id, shard_id, current_blob_id,
                            off, len, hex_bytes(result.body.cbytes(), std::min(len, 10u)));
                    EXPECT_EQ(result.body.size(), len);
                    EXPECT_EQ(std::memcmp(result.body.bytes(), blob.body.cbytes() + off, result.body.size()), 0);
                    EXPECT_EQ(result.user_key.size(), blob.user_key.size());
                    EXPECT_EQ(blob.user_key, result.user_key);
                    EXPECT_EQ(blob.object_off, result.object_off);
                    current_blob_id++;
                }
            }
        }
    }

    void verify_obj_count(uint32_t num_pgs, uint32_t shards_per_pg, uint32_t blobs_per_shard,
                          bool deleted_all = false) {
        uint32_t exp_active_blobs = deleted_all ? 0 : shards_per_pg * blobs_per_shard;
        uint32_t exp_tombstone_blobs = deleted_all ? shards_per_pg * blobs_per_shard : 0;

        for (uint32_t i = 1; i <= num_pgs; ++i) {
            PGStats stats;
            _obj_inst->pg_manager()->get_stats(i, stats);
            ASSERT_EQ(stats.num_active_objects, exp_active_blobs)
                << "Active objs stats not correct " << g_helper->replica_num();
            ASSERT_EQ(stats.num_tombstone_objects, exp_tombstone_blobs) << "Deleted objs stats not correct";
        }
    }

    void verify_hs_pg(HSHomeObject::HS_PG* lhs_pg, HSHomeObject::HS_PG* rhs_pg) {
        // verify index table
        EXPECT_EQ(lhs_pg->index_table_->uuid(), rhs_pg->index_table_->uuid());
        EXPECT_EQ(lhs_pg->index_table_->used_size(), rhs_pg->index_table_->used_size());

        // verify repl_dev
        EXPECT_EQ(lhs_pg->repl_dev_->group_id(), rhs_pg->repl_dev_->group_id());

        // verify pg_info_superblk
        auto lhs = lhs_pg->pg_sb_.get();
        auto rhs = rhs_pg->pg_sb_.get();

        EXPECT_EQ(lhs->id, rhs->id);
        EXPECT_EQ(lhs->num_members, rhs->num_members);
        EXPECT_EQ(lhs->replica_set_uuid, rhs->replica_set_uuid);
        EXPECT_EQ(lhs->index_table_uuid, rhs->index_table_uuid);
        EXPECT_EQ(lhs->blob_sequence_num, rhs->blob_sequence_num);
        EXPECT_EQ(lhs->active_blob_count, rhs->active_blob_count);
        EXPECT_EQ(lhs->tombstone_blob_count, rhs->tombstone_blob_count);
        EXPECT_EQ(lhs->total_occupied_blk_count, rhs->total_occupied_blk_count);
        EXPECT_EQ(lhs->tombstone_blob_count, rhs->tombstone_blob_count);
        for (uint32_t i = 0; i < lhs->num_members; i++) {
            EXPECT_EQ(lhs->members[i].id, rhs->members[i].id);
            EXPECT_EQ(lhs->members[i].priority, rhs->members[i].priority);
            EXPECT_EQ(0, std::strcmp(lhs->members[i].name, rhs->members[i].name));
        }
    }

    void verify_hs_shard(const ShardInfo& lhs, const ShardInfo& rhs) {
        // operator == is already overloaded , so we need to compare each field
        EXPECT_EQ(lhs.id, rhs.id);
        EXPECT_EQ(lhs.placement_group, rhs.placement_group);
        EXPECT_EQ(lhs.state, rhs.state);
        EXPECT_EQ(lhs.lsn, rhs.lsn);
        EXPECT_EQ(lhs.created_time, rhs.created_time);
        EXPECT_EQ(lhs.last_modified_time, rhs.last_modified_time);
        EXPECT_EQ(lhs.available_capacity_bytes, rhs.available_capacity_bytes);
        EXPECT_EQ(lhs.total_capacity_bytes, rhs.total_capacity_bytes);
        EXPECT_EQ(lhs.deleted_capacity_bytes, rhs.deleted_capacity_bytes);
        EXPECT_EQ(lhs.current_leader, rhs.current_leader);
    }

    void run_on_pg_leader(pg_id_t pg_id, auto&& lambda) {
        PGStats pg_stats;
        auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
        RELEASE_ASSERT(res, "can not get pg {} stats", pg_id);
        if (g_helper->my_replica_id() == pg_stats.leader_id) { lambda(); }
        // TODO: add logic for check and retry of leader change if necessary
    }

private:
    bool pg_exist(pg_id_t pg_id) {
        std::vector< pg_id_t > pg_ids;
        _obj_inst->pg_manager()->get_pg_ids(pg_ids);
        return std::find(pg_ids.begin(), pg_ids.end(), pg_id) != pg_ids.end();
    }

    bool shard_exist(shard_id_t id) {
        auto r = _obj_inst->shard_manager()->get_shard(id).get();
        return !!r;
    }

    bool blob_exist(shard_id_t shard_id, blob_id_t blob_id) {
        auto r = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
        return !!r;
    }

    void trigger_cp(bool wait) {
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

    homeobject::Blob build_blob(blob_id_t blob_id) {
        uint32_t alignment = 512;
        // Create non 512 byte aligned address to create copy.
        if (blob_id % 2 == 0) alignment = 256;

        // for different blob_id, we need to use different random engine, so that the rand_blob_size and
        // rand_user_key_size and the content of user_key and blob will all be predictable
        std::mt19937_64 predictable_engine(blob_id);

        std::string user_key;
        user_key.resize(rand_user_key_size(predictable_engine));
        BitsGenerator::gen_blob_bits(user_key.size(), (uint8_t*)user_key.data(), blob_id);
        auto blob_size = rand_blob_size(predictable_engine);
        homeobject::Blob blob{sisl::io_blob_safe(blob_size, alignment), user_key, 42ul};
        BitsGenerator::gen_blob_bits(blob.body, blob_id);
        return blob;
    }

private:
    std::random_device rnd{};
    std::default_random_engine rnd_engine{rnd()};
    std::uniform_int_distribution< uint32_t > rand_blob_size;
    std::uniform_int_distribution< uint32_t > rand_user_key_size;
};
