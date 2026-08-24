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
#include <iomgr/iomgr_config_generated.h>
#include <iomgr/http_server.hpp>
#include <sisl/settings/settings.hpp>

SETTINGS_INIT(iomgrcfg::IomgrSettings, iomgr_config);
#define IM_SETTINGS_FACTORY() SETTINGS_FACTORY(iomgr_config)

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

    HomeObjectFixture() :
            rand_blob_size{1u, 16 * 1024}, rand_user_key_size{1u, HSHomeObject::BlobHeader::max_user_key_length} {}

    void SetUp() override {
        IM_SETTINGS_FACTORY().modifiable_settings([](auto& s) {
            s.io_env.http_port = 5000 + g_helper->replica_num();
            LOGD("setup http port to {}", s.io_env.http_port);
        });

        HSHomeObject::_hs_chunk_size = SISL_OPTIONS["chunk_size"].as< uint64_t >() * Mi;
        _obj_inst = std::dynamic_pointer_cast< HSHomeObject >(g_helper->build_new_homeobject());

        // Used to export metrics, it should be called after init_homeobject
        if (SISL_OPTIONS["enable_http"].as< bool >()) { g_helper->app->start_http_server(); }
        if (!g_helper->is_current_testcase_restarted()) {
            g_helper->bump_sync_point_and_sync();
        } else {
            g_helper->sync();
        }
    }

    void TearDown() override {
        g_helper->sync();
        LOGINFO("Tearing down homeobject replica={}", g_helper->my_replica_id());
        LOGINFO("Metrics={}", sisl::MetricsFarm::getInstance().get_result_in_json().dump(2));
        g_helper->app->stop_http_server();
        _obj_inst.reset();
        g_helper->delete_homeobject();
    }

    void restart(uint32_t shutdown_delay_secs = 0u, uint32_t restart_delay_secs = 0u, uint32_t disk_lost_num = 0u,
                 bool clean_lost_disk = false) {
        g_helper->sync();
        LOGINFO("Restarting homeobject replica={}", g_helper->my_replica_id());
        LOGTRACE("Metrics={}", sisl::MetricsFarm::getInstance().get_result_in_json().dump(2));
        trigger_cp(true);
        _obj_inst.reset();
        _obj_inst = std::dynamic_pointer_cast< HSHomeObject >(
            g_helper->restart(shutdown_delay_secs, restart_delay_secs, disk_lost_num, clean_lost_disk));
        // wait for leader to be elected
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    void stop() {
        LOGINFO("Stopping homeobject replica={}", g_helper->my_replica_id());
        LOGINFO("Metrics={}", sisl::MetricsFarm::getInstance().get_result_in_json().dump(2));
        _obj_inst.reset();
        g_helper->homeobj_.reset();
        sleep(10);
    }

    void start() {
        LOGINFO("Starting homeobject replica={}", g_helper->my_replica_id());
        _obj_inst.reset();
        _obj_inst = std::dynamic_pointer_cast< HSHomeObject >(g_helper->restart());
        // wait for leader to be elected
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    void kill() {
        LOGINFO("SigKilling homeobject replica={}", g_helper->my_replica_id());
        std::raise(SIGKILL);
    }

    /**
     * \brief create pg with a given id.
     *
     * \param pg_id pg id that will be newly created.
     * \param leader_replica_num the replica number that will be the initial leader of repl dev of this pg
     * \param excluding_replicas_in_pg the set of replicas that will be excluded in the initial members of this pg. this
     * means all the started replicas that are not in this set will be the initial members of this pg.
     */
    void create_pg(pg_id_t pg_id, uint8_t leader_replica_num = 0,
                   std::optional< std::unordered_set< uint8_t > > excluding_replicas_in_pg = std::nullopt) {
        std::unordered_set< uint8_t > excluding_pg_replicas;
        if (excluding_replicas_in_pg.has_value()) excluding_pg_replicas = excluding_replicas_in_pg.value();
        if (excluding_pg_replicas.contains(leader_replica_num))
            RELEASE_ASSERT(false, "fail to create pg, leader_replica_num {} is excluded in the pg", leader_replica_num);

        auto my_replica_num = g_helper->replica_num();
        if (excluding_pg_replicas.contains(my_replica_num)) return;

        auto pg_size =
            SISL_OPTIONS["chunks_per_pg"].as< uint64_t >() * SISL_OPTIONS["chunk_size"].as< uint64_t >() * Mi;
        auto name = g_helper->test_name();

        if (leader_replica_num == my_replica_num) {
            auto members = g_helper->members();
            auto info = homeobject::PGInfo(pg_id);
            info.size = pg_size;
            for (const auto& member : members) {
                if (leader_replica_num == member.second) {
                    // by default, leader is the first member
                    info.members.insert(homeobject::PGMember{member.first, name + std::to_string(member.second), 1});
                } else if (!excluding_pg_replicas.contains(member.second)) {
                    info.members.insert(homeobject::PGMember{member.first, name + std::to_string(member.second), 0});
                }
            }
            auto p = _obj_inst->pg_manager()->create_pg(std::move(info)).get();
            ASSERT_TRUE(p);
            LOGINFO("pg={} is created at leader", pg_id);
        } else {
            // follower need to wait for pg creation to complete locally
            while (!pg_exist(pg_id)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            LOGINFO("pg={} is created at follower", pg_id);
        }
    }

    ShardInfo create_shard(pg_id_t pg_id, uint64_t size_bytes, std::string meta) {
        g_helper->sync();
        if (!am_i_in_pg(pg_id)) return {};
        auto tid = generateRandomTraceId();

        // All replicas loop: the current leader runs create_shard and sets uint64_id;
        // if the leader changes before executing, the new leader picks it up next iteration.
        // done_check waits for both the IPC slot to be set and local shard replication,
        // consistent with the put_blob pattern.
        run_on_pg_leader_with_retry(
            pg_id,
            [&] {
                auto id = g_helper->get_uint64_id();
                return id != INVALID_UINT64_ID && shard_exist(id, tid);
            },
            [&]() -> bool {
                auto s = _obj_inst->shard_manager()->create_shard(pg_id, size_bytes, meta, tid).get();
                if (!s) {
                    if (is_not_leader_error(s.error())) return false;
                    RELEASE_ASSERT(false, "failed to create shard");
                }
                g_helper->set_uint64_id(s.value().id);
                return true;
            });

        // get shard_id from IPC (guaranteed set and locally replicated by done_check)
        auto shard_id = g_helper->get_uint64_id();

        // set v_chunk_id to IPC
        run_on_pg_leader_with_retry(
            pg_id, [&] { return g_helper->get_auxiliary_uint64_id() != INVALID_UINT64_ID; },
            [&]() -> bool {
                auto v_chunkID = _obj_inst->get_shard_v_chunk_id(shard_id);
                RELEASE_ASSERT(v_chunkID.has_value(), "failed to get shard v_chunk_id");
                g_helper->set_auxiliary_uint64_id(v_chunkID.value());
                return true;
            });

        // get v_chunk_id from IPC and compare with local
        auto leader_v_chunk_id = g_helper->get_auxiliary_uint64_id();
        auto local_v_chunkID = _obj_inst->get_shard_v_chunk_id(shard_id);
        RELEASE_ASSERT(local_v_chunkID.has_value(), "failed to get shard v_chunk_id");
        RELEASE_ASSERT(leader_v_chunk_id == local_v_chunkID, "v_chunk_id supposed to be identical");

        auto r = _obj_inst->shard_manager()->get_shard(shard_id, tid).get();
        RELEASE_ASSERT(!!r, "failed to get shard {}", shard_id);
        return r.value();
    }

    ShardInfo seal_shard(shard_id_t shard_id) {
        g_helper->sync();
        auto tid = generateRandomTraceId();
        auto r = _obj_inst->shard_manager()->get_shard(shard_id, tid).get();
        if (!r) return {};
        auto pg_id = r.value().placement_group;

        std::optional< ShardInfo > sealed_opt;
        auto is_sealed = [&]() -> bool {
            auto res = _obj_inst->shard_manager()->get_shard(shard_id, tid).get();
            if (res && res.value().state == ShardInfo::State::SEALED) {
                sealed_opt = res.value();
                return true;
            }
            return false;
        };

        run_on_pg_leader_with_retry(pg_id, is_sealed, [&]() -> bool {
            if (is_sealed()) return true; // idempotent: already sealed, sealed_opt captured inside is_sealed
            auto s = _obj_inst->shard_manager()->seal_shard(shard_id, tid).get();
            if (!s) {
                if (is_not_leader_error(s.error())) return false;
                RELEASE_ASSERT(false, "failed to seal shard");
            }
            sealed_opt = s.value(); // leader path: capture directly from seal result, no extra get_shard needed
            return true;
        });

        RELEASE_ASSERT(sealed_opt.has_value(), "shard {} not sealed after run_on_pg_leader_with_retry", shard_id);
        return sealed_opt.value();
    }

    void put_blob(shard_id_t shard_id, Blob&& blob) {
        g_helper->sync();
        auto tid = generateRandomTraceId();
        auto r = _obj_inst->shard_manager()->get_shard(shard_id, tid).get();
        if (!r) return;
        auto pg_id = r.value().placement_group;

        // Clone blob so it remains valid across retries (the move only happens on success).
        auto blob_to_put = std::move(blob);
        // IPC slot carries the assigned blob_id from leader to all replicas.
        // done_check also waits for blob_exist so no separate replication-wait loop is needed.
        run_on_pg_leader_with_retry(
            pg_id,
            [&] {
                auto blob_id = g_helper->get_uint64_id();
                return blob_id != INVALID_UINT64_ID && blob_exist(shard_id, blob_id);
            },
            [&]() -> bool {
                auto b = _obj_inst->blob_manager()->put(shard_id, blob_to_put.clone(), tid).get();
                if (!b) {
                    if (is_not_leader_error(b.error())) return false;
                    RELEASE_ASSERT(false, "failed to put blob");
                }
                g_helper->set_uint64_id(b.value());
                return true;
            });
    }

    /**
     * Writes multiple blobs to specified shards across PGs
     *
     * @param pg_shard_id_vec Map of PG IDs to their shard ID vectors. Blobs are written to each shard in each PG.
     * @param num_blobs_per_shard Number of blobs to write to each shard.
     * @param pg_blob_id In/out map tracking next blob ID for each PG. Updated after writing blobs.
     * @param sync_before_start If true, trigger sync across all replicas at the beginning. (default: true)
     * @param trigger_cp_on_leader If true, triggers checkpoint on leader after each blob write. (default: false)
     *                             Useful for baseline resync tests as log truncation depends on checkpoints.
     *
     * @return Map of shard IDs to their blob IDs and written block counts
     *
     * @note Blob IDs are sequential within each PG, starting from pg_blob_id[pg_id]
     * @note Only writes to PGs where current replica is a member
     * @note TODO: make this run in parallel
     */
    std::map< shard_id_t, std::map< blob_id_t, uint64_t > >
    put_blobs(std::map< pg_id_t, std::vector< shard_id_t > > const& pg_shard_id_vec, uint64_t const num_blobs_per_shard,
              std::map< pg_id_t, blob_id_t >& pg_blob_id, bool sync_before_start = true,
              bool trigger_cp_on_leader = false) {
        if (sync_before_start) { g_helper->sync(); }
        std::map< shard_id_t, std::map< blob_id_t, uint64_t > > shard_blob_ids_map;
        for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
            if (!am_i_in_pg(pg_id)) continue;
            // Build ordered submission list and pre-compute return map (actual_written_blk_count_for_blob is pure).
            std::vector< std::pair< shard_id_t, blob_id_t > > blob_order;
            blob_id_t current_blob_id{pg_blob_id[pg_id]};
            for (const auto& shard_id : shard_vec) {
                for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                    blob_order.emplace_back(shard_id, current_blob_id);
                    auto [it, _] = shard_blob_ids_map.try_emplace(shard_id, std::map< blob_id_t, uint64_t >());
                    it->second[current_blob_id] = actual_written_blk_count_for_blob(current_blob_id);
                    current_blob_id++;
                }
            }
            if (blob_order.empty()) continue;
            auto [last_shard_id, last_blob_id] = blob_order.back();

            // All replicas wait for the last blob; the leader submits all missing blobs in one pass.
            // If leadership changes mid-batch, the new leader resumes from where the old one left off.
            // In replace_member tests an out-member may be removed before the last blob arrives; am_i_in_pg guards
            // that.
            run_on_pg_leader_with_retry(
                pg_id, [&] { return !am_i_in_pg(pg_id) || blob_exist(last_shard_id, last_blob_id); },
                [&]() -> bool {
                    for (auto& [shard_id, blob_id] : blob_order) {
                        if (blob_exist(shard_id, blob_id)) continue; // already committed, skip
                        auto put_blob = build_blob(blob_id);
                        auto tid = generateRandomTraceId();
                        LOGDEBUG("Put blob pg={} shard {} blob {} size {} data {} trace_id={}", pg_id, shard_id,
                                 blob_id, put_blob.body.size(),
                                 hex_bytes(put_blob.body.cbytes(), std::min(10u, put_blob.body.size())), tid);
                        auto b = _obj_inst->blob_manager()->put(shard_id, std::move(put_blob), tid).get();
                        if (!b) {
                            if (is_not_leader_error(b.error())) return false;
                            RELEASE_ASSERT(false, "Failed to put blob pg={} shard {} error={}", pg_id, shard_id,
                                           b.error());
                        }
                        RELEASE_ASSERT(b.value() == blob_id, "predicted blob_id={} actual={}", blob_id, b.value());
                        if (trigger_cp_on_leader) { trigger_cp(true); }
                    }
                    return true;
                });

            // Update pg_blob_id after the batch is confirmed so it always reflects actual committed blobs.
            pg_blob_id[pg_id] = current_blob_id;
            LOGINFO("pg={} shard={} blob={} confirmed locally — all blobs in batch are visible", pg_id, last_shard_id,
                    last_blob_id);
        }

        return shard_blob_ids_map;
    }

    void del_blob(pg_id_t pg_id, shard_id_t shard_id, blob_id_t blob_id) {
        g_helper->sync();
        auto tid = generateRandomTraceId();
        run_on_pg_leader_with_retry(
            pg_id, [&] { return !blob_exist(shard_id, blob_id); },
            [&]() -> bool {
                if (!blob_exist(shard_id, blob_id)) return true; // idempotent: already deleted
                auto g = _obj_inst->blob_manager()->del(shard_id, blob_id, tid).get();
                if (!g) {
                    if (is_not_leader_error(g.error())) return false;
                    RELEASE_ASSERT(false, "failed to del blob");
                }
                LOGINFO("delete blob, pg={} shard {} blob {} trace_id={}", pg_id, shard_id, blob_id, tid);
                return true;
            });
    }

    void del_blobs(pg_id_t pg_id, std::map< shard_id_t, std::set< blob_id_t > > const& shard_blob_ids_map) {
        g_helper->sync();
        if (shard_blob_ids_map.empty()) return;
        auto last_shard_id = shard_blob_ids_map.rbegin()->first;
        auto last_blob_id = *shard_blob_ids_map.rbegin()->second.rbegin();

        // All replicas wait for the last blob to be absent; the leader deletes all surviving blobs in one pass.
        // If leadership changes mid-batch, the new leader resumes by skipping already-deleted blobs.
        // !am_i_in_pg guards the replace_member case: a removed member stops receiving raft updates and
        // would otherwise hang waiting for a deletion it will never see locally.
        run_on_pg_leader_with_retry(
            pg_id, [&] { return !am_i_in_pg(pg_id) || !blob_exist(last_shard_id, last_blob_id); },
            [&]() -> bool {
                for (const auto& [shard_id, blob_ids] : shard_blob_ids_map) {
                    for (const auto& blob_id : blob_ids) {
                        if (!blob_exist(shard_id, blob_id)) continue; // already deleted
                        auto tid = generateRandomTraceId();
                        auto g = _obj_inst->blob_manager()->del(shard_id, blob_id, tid).get();
                        if (!g) {
                            if (is_not_leader_error(g.error())) return false;
                            RELEASE_ASSERT(false, "failed to del blob");
                        }
                        LOGDEBUG("delete blob, pg={} shard {} blob {} trace_id={}", pg_id, shard_id, blob_id, tid);
                    }
                }
                return true;
            });
    }

    void del_all_blobs(std::map< pg_id_t, std::vector< shard_id_t > > const& pg_shard_id_vec,
                       uint64_t const num_blobs_per_shard, std::map< pg_id_t, blob_id_t >& pg_blob_id) {
        g_helper->sync();
        for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
            if (!am_i_in_pg(pg_id)) continue;
            if (shard_vec.empty() || pg_blob_id[pg_id] == 0) continue;

            // Build ordered deletion list matching the put_blobs submission order.
            std::vector< std::pair< shard_id_t, blob_id_t > > blob_order;
            blob_id_t current_blob_id{0};
            for (; current_blob_id < pg_blob_id[pg_id];) {
                for (const auto& shard_id : shard_vec) {
                    for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                        blob_order.emplace_back(shard_id, current_blob_id);
                        current_blob_id++;
                    }
                }
            }

            auto [last_shard_id, last_blob_id] = blob_order.back();

            // All replicas wait for the last blob to be absent; the leader deletes all surviving blobs in one pass.
            run_on_pg_leader_with_retry(
                pg_id, [&] { return !blob_exist(last_shard_id, last_blob_id); },
                [&]() -> bool {
                    for (auto& [shard_id, blob_id] : blob_order) {
                        if (!blob_exist(shard_id, blob_id)) continue; // already deleted
                        auto tid = generateRandomTraceId();
                        auto g = _obj_inst->blob_manager()->del(shard_id, blob_id, tid).get();
                        if (!g) {
                            if (is_not_leader_error(g.error())) return false;
                            RELEASE_ASSERT(false, "failed to del blob");
                        }
                        LOGINFO("delete blob shard {} blob {}", shard_id, blob_id);
                    }
                    return true;
                });
        }
    }

    uint32_t get_valid_blob_count_in_pg(const pg_id_t pg_id) {
        auto hs_pg = _obj_inst->get_hs_pg(pg_id);
        if (!hs_pg) {
            LOGERROR("failed to get hs_pg for pg_id={}", pg_id);
            return 0;
        }

        auto start_key =
            BlobRouteKey{BlobRoute{uint64_t(pg_id) << homeobject::shard_width, std::numeric_limits< uint64_t >::min()}};
        auto end_key = BlobRouteKey{
            BlobRoute{uint64_t(pg_id + 1) << homeobject::shard_width, std::numeric_limits< uint64_t >::min()}};

        homestore::BtreeQueryRequest< BlobRouteKey > query_req{
            homestore::BtreeKeyRange< BlobRouteKey >{std::move(start_key), true /* inclusive */, std::move(end_key),
                                                     false /* inclusive */},
            homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY,
            std::numeric_limits< uint32_t >::max() /* blob count in a shard will not exceed uint32_t_max*/,
            [](homestore::BtreeKey const& key, homestore::BtreeValue const& value) -> bool {
                BlobRouteValue existing_value{value};
                if (existing_value.pbas() == HSHomeObject::tombstone_pbas) { return false; }
                return true;
            }};

        std::vector< std::pair< BlobRouteKey, BlobRouteValue > > valid_blob_indexes;
        hs_pg->index_table_->query(query_req, valid_blob_indexes);
        return valid_blob_indexes.size();
    }

    void verify_shard_meta(std::map< pg_id_t, std::vector< shard_id_t > > const& pg_shard_id_vec) {
        for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
            if (!am_i_in_pg(pg_id)) continue;
            for (const auto& shard_id : shard_vec) {
                auto iter = _obj_inst->_shard_map.find(shard_id);
                ASSERT_TRUE(iter != _obj_inst->_shard_map.end())
                    << "shard not found in local shard_map, shard_id " << shard_id << " replica number "
                    << g_helper->replica_num();
                auto meta_str = "shard meta:" + std::to_string(shard_id);
                uint8_t expected_meta[ShardInfo::meta_length]{};
                memcpy(expected_meta, meta_str.c_str(), meta_str.length());
                expected_meta[meta_str.length()] = '\0';
                auto shard = iter->second->get();
                ASSERT_TRUE(std::memcmp(shard->info.meta, expected_meta, ShardInfo::meta_length) == 0);
            }
        }
    }

    // TODO:make this run in parallel
    void verify_get_blob(std::map< pg_id_t, std::vector< shard_id_t > > const& pg_shard_id_vec,
                         uint64_t const num_blobs_per_shard, bool const use_random_offset = false,
                         bool const wait_when_not_exist = false,
                         std::map< pg_id_t, blob_id_t > pg_start_blob_id = std::map< pg_id_t, blob_id_t >()) {
        uint32_t off = 0, len = 0;

        for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
            if (!am_i_in_pg(pg_id)) continue;
            blob_id_t current_blob_id{0};
            if (pg_start_blob_id.find(pg_id) != pg_start_blob_id.end()) current_blob_id = pg_start_blob_id[pg_id];
            for (const auto& shard_id : shard_vec) {
                for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                    auto tid = generateRandomTraceId();
                    LOGDEBUG("going to verify blob pg={} shard {} blob {} trace_id={}", pg_id, shard_id,
                             current_blob_id, tid);
                    auto blob = build_blob(current_blob_id);
                    len = blob.body.size();

                    bool allow_skip_verify = true;
                    if (use_random_offset) {
                        std::uniform_int_distribution< uint32_t > rand_off_gen{0u, len - 1u};
                        std::uniform_int_distribution< uint32_t > rand_len_gen{1u, len};

                        off = rand_off_gen(rnd_engine);
                        len = rand_len_gen(rnd_engine);
                        if (off + len >= blob.body.size()) { len = blob.body.size() - off; }

                        // randomly set allow_skip_verify to false to do full verification
                        std::uniform_int_distribution< uint32_t > bool_dist(0, 1);
                        allow_skip_verify = (bool)bool_dist(rnd_engine);
                    }

                    auto g = _obj_inst->blob_manager()
                                 ->get(shard_id, current_blob_id, off, len, allow_skip_verify, tid)
                                 .get();
                    while (wait_when_not_exist && g.hasError() && g.error().code == BlobErrorCode::UNKNOWN_BLOB) {
                        LOGDEBUG("blob not exist at the moment, waiting for sync, shard {} blob {}", shard_id,
                                 current_blob_id);
                        wait_for_blob(shard_id, current_blob_id);
                        g = _obj_inst->blob_manager()
                                ->get(shard_id, current_blob_id, off, len, allow_skip_verify, tid)
                                .get();
                    }
                    ASSERT_TRUE(!!g) << "get blob fail, shard_id " << shard_id << " blob_id " << current_blob_id
                                     << " replica number " << g_helper->replica_num();
                    auto result = std::move(g.value());
                    LOGINFO("get blob pg={} shard {} blob {} off {} len {} data {} allow_skip_verify {}", pg_id,
                            shard_id, current_blob_id, off, len, hex_bytes(result.body.cbytes(), std::min(len, 10u)),
                            allow_skip_verify);
                    EXPECT_EQ(result.body.size(), len);
                    EXPECT_EQ(std::memcmp(result.body.bytes(), blob.body.cbytes() + off, result.body.size()), 0);
                    // Only verify user_key and object_off when allow_skip_verify is false
                    if (!allow_skip_verify) {
                        EXPECT_EQ(result.user_key.size(), blob.user_key.size());
                        EXPECT_EQ(blob.user_key, result.user_key);
                        EXPECT_EQ(blob.object_off, result.object_off);
                    }
                    current_blob_id++;
                }
            }
        }
    }

    void verify_shard_blobs(const std::map< shard_id_t, std::set< blob_id_t > >& shard_blobs,
                            bool const use_random_offset = false) {
        uint32_t off = 0, len = 0;
        for (const auto& [shard_id, blob_ids] : shard_blobs) {
            for (const auto& blob_id : blob_ids) {
                auto tid = generateRandomTraceId();
                LOGDEBUG("going to verify blob shard {} blob {} trace_id={}", shard_id, blob_id, tid);
                auto blob = build_blob(blob_id);
                len = blob.body.size();
                if (use_random_offset) {
                    std::uniform_int_distribution< uint32_t > rand_off_gen{0u, len - 1u};
                    std::uniform_int_distribution< uint32_t > rand_len_gen{1u, len};

                    off = rand_off_gen(rnd_engine);
                    len = rand_len_gen(rnd_engine);
                    if ((off + len) >= blob.body.size()) { len = blob.body.size() - off; }
                }

                auto g = _obj_inst->blob_manager()->get(shard_id, blob_id, off, len, tid).get();
                ASSERT_TRUE(!!g) << "get blob fail, shard_id " << shard_id << " blob_id " << blob_id
                                 << " replica number " << g_helper->replica_num();
                auto result = std::move(g.value());
                LOGDEBUG("get shard {} blob {} off {} len {} data {}", shard_id, blob_id, off, len,
                         hex_bytes(result.body.cbytes(), std::min(len, 10u)));

                EXPECT_EQ(result.body.size(), len);
                EXPECT_EQ(std::memcmp(result.body.bytes(), blob.body.cbytes() + off, result.body.size()), 0);
                EXPECT_EQ(result.user_key.size(), blob.user_key.size());
                EXPECT_EQ(blob.user_key, result.user_key);
                EXPECT_EQ(blob.object_off, result.object_off);
            }
        }
    }

    void verify_obj_count(uint32_t num_pgs, uint32_t shards_per_pg, uint32_t blobs_per_shard,
                          bool deleted_all = false) {
        uint32_t exp_active_blobs = deleted_all ? 0 : shards_per_pg * blobs_per_shard;
        uint32_t exp_tombstone_blobs = deleted_all ? shards_per_pg * blobs_per_shard : 0;

        for (uint32_t i = 1; i <= num_pgs; ++i) {
            if (!am_i_in_pg(i)) continue;
            PGStats stats;
            _obj_inst->pg_manager()->get_stats(i, stats);
            LOGW("verify obj count in pg={}", i);
            ASSERT_EQ(stats.num_active_objects, exp_active_blobs)
                << "Active objs stats not correct " << g_helper->replica_num() << "pg_id" << i;
            ASSERT_EQ(stats.num_tombstone_objects, exp_tombstone_blobs) << "Deleted objs stats not correct";
        }
    }

    void verify_pg_destroy(pg_id_t pg_id, const string& index_table_uuid_str,
                           const std::vector< shard_id_t >& shard_id_vec, bool wait_for_destroy = false) {
        // check pg
        if (wait_for_destroy) {
            while (pg_exist(pg_id)) {
                LOGD("pg still exists, wait for gc to destroy");
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
        ASSERT_FALSE(pg_exist(pg_id));
        ASSERT_EQ(_obj_inst->index_table_pg_map_.find(index_table_uuid_str), _obj_inst->index_table_pg_map_.end());
        // check shards
        auto e = _obj_inst->shard_manager()->list_shards(pg_id).get();
        ASSERT_EQ(e.error().getCode(), ShardErrorCode::UNKNOWN_PG);
        for (const auto& shard_id : shard_id_vec) {
            ASSERT_FALSE(shard_exist(shard_id));
        }
        // check chunk_selector
        const auto& chunk_selector = _obj_inst->chunk_selector();
        ASSERT_EQ(chunk_selector->m_per_pg_chunks.find(pg_id), chunk_selector->m_per_pg_chunks.end());
    }

    void destroy_pg(pg_id_t pg_id) { _obj_inst->pg_manager()->destroy_pg(pg_id); }

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
        EXPECT_EQ(lhs->num_expected_members, rhs->num_expected_members);
        EXPECT_EQ(lhs->num_dynamic_members, rhs->num_dynamic_members);
        EXPECT_EQ(lhs->num_chunks, rhs->num_chunks);
        EXPECT_EQ(lhs->chunk_size, rhs->chunk_size);
        EXPECT_EQ(lhs->pg_size, rhs->pg_size);
        EXPECT_EQ(lhs->replica_set_uuid, rhs->replica_set_uuid);
        EXPECT_EQ(lhs->index_table_uuid, rhs->index_table_uuid);
        EXPECT_EQ(lhs->blob_sequence_num, rhs->blob_sequence_num);
        EXPECT_EQ(lhs->active_blob_count, rhs->active_blob_count);
        EXPECT_EQ(lhs->tombstone_blob_count, rhs->tombstone_blob_count);
        EXPECT_EQ(lhs->total_occupied_blk_count, rhs->total_occupied_blk_count);
        EXPECT_EQ(lhs->tombstone_blob_count, rhs->tombstone_blob_count);
        for (uint32_t i = 0; i < lhs->num_dynamic_members; i++) {
            EXPECT_EQ(lhs->get_pg_members()[i].id, rhs->get_pg_members()[i].id);
            EXPECT_EQ(lhs->get_pg_members()[i].priority, rhs->get_pg_members()[i].priority);
            EXPECT_EQ(0, std::strcmp(lhs->get_pg_members()[i].name, rhs->get_pg_members()[i].name));
        }
        for (homestore::chunk_num_t i = 0; i < lhs->num_chunks; ++i) {
            EXPECT_EQ(lhs->get_chunk_ids()[i], rhs->get_chunk_ids()[i]);
        }

        // verify recovered pg_info
        EXPECT_EQ(lhs_pg->pg_info_.id, rhs_pg->pg_info_.id);
        EXPECT_EQ(lhs_pg->pg_info_.replica_set_uuid, rhs_pg->pg_info_.replica_set_uuid);
        EXPECT_EQ(lhs_pg->pg_info_.size, rhs_pg->pg_info_.size);
        EXPECT_EQ(lhs_pg->pg_info_.chunk_size, rhs_pg->pg_info_.chunk_size);
    }

    void verify_hs_shard(const ShardInfo& lhs, const ShardInfo& rhs) {
        // operator == is already overloaded , so we need to compare each field
        EXPECT_EQ(lhs.id, rhs.id);
        EXPECT_EQ(lhs.placement_group, rhs.placement_group);
        EXPECT_EQ(lhs.state, rhs.state);
        EXPECT_EQ(lhs.create_lsn, rhs.create_lsn);
        EXPECT_EQ(lhs.created_time, rhs.created_time);
        EXPECT_EQ(lhs.last_modified_time, rhs.last_modified_time);
        EXPECT_EQ(lhs.available_capacity_bytes, rhs.available_capacity_bytes);
        EXPECT_EQ(lhs.total_capacity_bytes, rhs.total_capacity_bytes);
        EXPECT_EQ(lhs.current_leader, rhs.current_leader);
        EXPECT_TRUE(std::memcmp(lhs.meta, rhs.meta, ShardInfo::meta_length) == 0);
    }

    bool verify_start_replace_member_result(pg_id_t pg_id, std::string& task_id, peer_id_t out_member_id,
                                            peer_id_t in_member_id) {
        auto hs_pg = _obj_inst->get_hs_pg(pg_id);
        RELEASE_ASSERT(hs_pg, "PG not found");
        auto out_member = PGMember(out_member_id, "out_member");
        auto in_member = PGMember(in_member_id, "in_member");
        auto out = hs_pg->pg_info_.members.find(out_member);
        auto in = hs_pg->pg_info_.members.find(in_member);
        RELEASE_ASSERT(hs_pg->pg_info_.members.size() == 4, "Invalid pg member size={}",
                       hs_pg->pg_info_.members.size());
        if (in == hs_pg->pg_info_.members.end()) {
            LOGERROR("in_member not found, in_member={}", boost::uuids::to_string(in_member_id));
            return false;
        }
        if (out == hs_pg->pg_info_.members.end()) {
            LOGERROR("out_member not found, out_member={}", boost::uuids::to_string(out_member_id));
            return false;
        }
        run_on_pg_leader(pg_id, [this, pg_id, &task_id, &out_member, &in_member]() {
            std::vector< PGMember > others;
            for (auto m : g_helper->members_) {
                if (m.first != out_member.id && m.first != in_member.id) { others.emplace_back(PGMember(m.first, "")); }
            }
            auto result = _obj_inst->get_replace_member_status(pg_id, task_id, out_member, in_member, others, 0);
            ASSERT_EQ(result.task_id, task_id);
            ASSERT_EQ(result.status, PGReplaceMemberTaskStatus::IN_PROGRESS);
            ASSERT_EQ(result.members.size(), 4);
            for (auto& p : result.members) {
                if (p.id == out_member.id) { ASSERT_FALSE(p.can_vote); }
                if (p.id == in_member.id) { ASSERT_TRUE(p.can_vote); }
            }
        });
        return true;
    }

    uint32_t get_br_progress(pg_id_t pg_id) {
        auto hs_pg = _obj_inst->get_hs_pg(pg_id);
        RELEASE_ASSERT(hs_pg, "PG not found");
        RELEASE_ASSERT(hs_pg->pg_state_.is_state_set(PGStateMask::BASELINE_RESYNC), "PG state is not in BR state");
        return hs_pg->get_snp_progress();
    }

    bool verify_complete_replace_member_result(pg_id_t pg_id, std::string& task_id, peer_id_t out_member_id,
                                               peer_id_t in_member_id) {
        auto hs_pg = _obj_inst->get_hs_pg(pg_id);
        RELEASE_ASSERT(hs_pg, "PG not found");
        RELEASE_ASSERT(hs_pg->pg_info_.members.size() == 3, "Invalid pg member size");
        auto out_member = PGMember(out_member_id, "out_member");
        auto in_member = PGMember(in_member_id, "in_member");
        auto in = hs_pg->pg_info_.members.find(in_member);
        auto out = hs_pg->pg_info_.members.find(out_member);
        if (in == hs_pg->pg_info_.members.end()) {
            LOGERROR("in_member not found, in_member={}", boost::uuids::to_string(in_member_id));
            return false;
        }
        if (out != hs_pg->pg_info_.members.end()) {
            LOGERROR("Out member still exists in PG");
            return false;
        }

        if (g_helper->my_replica_id() == in_member_id && hs_pg->pg_state_.is_state_set(PGStateMask::BASELINE_RESYNC)) {
            return false;
        }

        run_on_pg_leader(pg_id, [this, pg_id, &task_id, &out_member, &in_member]() {
            std::vector< PGMember > others;
            for (auto m : g_helper->members_) {
                if (m.first != out_member.id && m.first != in_member.id) { others.emplace_back(PGMember(m.first, "")); }
            }
            auto result = _obj_inst->get_replace_member_status(pg_id, task_id, out_member, in_member, others, 0);
            ASSERT_EQ(result.task_id, task_id);
            ASSERT_EQ(result.status, PGReplaceMemberTaskStatus::COMPLETED);
            ASSERT_EQ(result.members.size(), 3);
            for (auto& p : result.members) {
                ASSERT_TRUE(p.can_vote);
            }
        });
        return true;
    }

    bool verify_rollback_replace_member_result(pg_id_t pg_id, std::string& task_id, peer_id_t out_member_id,
                                               peer_id_t in_member_id) {
        auto hs_pg = _obj_inst->get_hs_pg(pg_id);
        RELEASE_ASSERT(hs_pg, "PG not found");
        RELEASE_ASSERT(hs_pg->pg_info_.members.size() == 3, "Invalid pg member size");
        auto out_member = PGMember(out_member_id, "out_member");
        auto in_member = PGMember(in_member_id, "in_member");
        auto in = hs_pg->pg_info_.members.find(in_member);
        auto out = hs_pg->pg_info_.members.find(out_member);
        if (in != hs_pg->pg_info_.members.end()) {
            LOGERROR("in_member still in pg, in_member={}", boost::uuids::to_string(in_member_id));
            return false;
        }
        if (out == hs_pg->pg_info_.members.end()) {
            LOGERROR("Out member not exists in PG");
            return false;
        }

        // verify task
        auto r = _obj_inst->pg_manager()->list_all_replace_member_tasks(0);
        RELEASE_ASSERT(r.hasValue(), "Failed to list_all_replace_member_tasks");
        const auto& tasks = r.value();
        bool found = std::any_of(tasks.cbegin(), tasks.cend(), [&task_id](const homeobject::replace_member_task& task) {
            return task.task_id == task_id;
        });
        if (found) {
            LOGI("Task with ID '{}' was found.", task_id);
            return false;
        }
        LOGI("Task with ID '{}' was not found on this member", task_id);
        return true;
    }

    void run_on_pg_leader(pg_id_t pg_id, auto&& lambda) {
        PGStats pg_stats;
        auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
        if (!res) return;
        if (g_helper->my_replica_id() == pg_stats.leader_id) { lambda(); }
        // TODO: add logic for check and retry of leader change if necessary
    }

    // Submit a scrub task and wait for it; retry if the task was cancelled mid-flight (e.g. due
    // to a leader switch).  Returns nullptr on non-leader replicas.  RELEASE_ASSERTs if the
    // retry deadline is exceeded while this replica is still the leader.
    std::shared_ptr< ScrubManager::ShallowScrubReport > submit_scrub_with_retry(pg_id_t pg_id, bool is_deep,
                                                                                uint32_t timeout_secs = 60) {
        auto scrub_mgr = _obj_inst->scrub_manager();
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_secs);
        while (std::chrono::steady_clock::now() < deadline) {
            PGStats pg_stats;
            if (!_obj_inst->pg_manager()->get_stats(pg_id, pg_stats)) return nullptr;
            if (g_helper->my_replica_id() != pg_stats.leader_id) return nullptr;

            auto report = scrub_mgr->submit_scrub_task(pg_id, is_deep, SCRUB_TRIGGER_TYPE::MANUALLY).get();
            if (report) return report;

            // null means the task was cancelled (leader switch); re-check leadership and retry.
            LOGWARN("scrub task cancelled for pg={} (leader switch?), retrying…", pg_id);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        RELEASE_ASSERT(false, "submit_scrub_with_retry: timeout after {}s for pg={}", timeout_secs, pg_id);
        return nullptr;
    }

    void run_on_pg_follower(pg_id_t pg_id, auto&& lambda) {
        PGStats pg_stats;
        auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
        if (!res) return;
        if (g_helper->my_replica_id() != pg_stats.leader_id) { lambda(); }
        // TODO: add logic for check and retry of leader change if necessary
    }

    // done_check: () -> bool — called by all replicas; returns true when the operation is complete.
    // leader_op:  () -> bool — called only when this replica is the current leader;
    //                          true = succeeded, false = lost leadership mid-op (will retry).
    // Solves the deadlock where a leader switch between sync() and run_on_pg_leader() leaves
    // nobody executing the lambda: the new leader picks it up in the next loop iteration.
    void run_on_pg_leader_with_retry(pg_id_t pg_id, auto&& done_check, auto&& leader_op, uint32_t timeout_secs = 60) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_secs);
        while (std::chrono::steady_clock::now() < deadline) {
            if (done_check()) return;
            PGStats pg_stats;
            auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
            if (!res) {
                LOGWARN("run_on_pg_leader_with_retry: get_stats failed on pg={}, retrying...", pg_id);
            } else if (g_helper->my_replica_id() == pg_stats.leader_id) {
                if (leader_op()) return;
                // leader_op returned false: lost leadership during execution, retry
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        PGStats pg_stats{};
        const auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
        const auto leader_id = res ? pg_stats.leader_id : uuids::nil_uuid();
        RELEASE_ASSERT(
            false, "run_on_pg_leader_with_retry: timeout after {}s on pg={} current_leader={} am_leader={} stats_ok={}",
            timeout_secs, pg_id, leader_id, g_helper->my_replica_id() == leader_id, res);
    }

    static bool is_not_leader_error(const ShardError& err) {
        auto c = err.getCode();
        return c == ShardErrorCode::NOT_LEADER || c == ShardErrorCode::RETRY_REQUEST ||
            c == ShardErrorCode::PG_NOT_READY;
    }

    static bool is_not_leader_error(const BlobError& err) {
        auto c = err.getCode();
        return c == BlobErrorCode::NOT_LEADER || c == BlobErrorCode::RETRY_REQUEST;
    }

    // Yields Raft leadership to the desired replica and waits for the change to take effect.
    // Call before sync() in tests that need a stable, predictable leader.
    void ensure_leader(pg_id_t pg_id, uint8_t desired_replica = 0, uint32_t timeout_secs = 30) {
        if (!am_i_in_pg(pg_id)) return;
        auto desired_id = g_helper->replica_id(desired_replica);
        if (desired_id.is_nil()) return;

        // Verify the desired replica is actually a member of this PG before attempting transfer.
        {
            PGStats pg_stats{};
            if (!_obj_inst->pg_manager()->get_stats(pg_id, pg_stats)) {
                LOGWARN("ensure_leader: get_stats failed for pg={}, skipping member validation", pg_id);
            } else {
                const bool is_member = std::any_of(pg_stats.members.begin(), pg_stats.members.end(),
                                                   [&](const auto& m) { return m.id == desired_id; });
                RELEASE_ASSERT(is_member, "ensure_leader: replica {} is not a member of pg={}", desired_id, pg_id);
            }
        }

        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_secs);
        while (true) {
            RELEASE_ASSERT(std::chrono::steady_clock::now() < deadline,
                           "ensure_leader: timeout after {}s, pg={} desired={}", timeout_secs, pg_id, desired_id);

            PGStats pg_stats{};
            const auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
            if (!res || pg_stats.leader_id.is_nil()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }
            if (pg_stats.leader_id == desired_id) return;

            if (g_helper->my_replica_id() == pg_stats.leader_id) {
                auto hs_pg = _obj_inst->get_hs_pg(pg_id);
                if (hs_pg) { hs_pg->yield_leadership_to_follower(desired_id); }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    peer_id_t get_leader_id(pg_id_t pg_id) {
        if (!am_i_in_pg(pg_id)) return uuids::nil_uuid();
        while (true) {
            PGStats pg_stats;
            auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
            if (!res || pg_stats.leader_id.is_nil()) {
                LOGINFO("fail to get leader, retry later");
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }
            return pg_stats.leader_id;
        }
    }

    bool wait_for_leader_change(pg_id_t pg_id, peer_id_t old_leader) {
        if (old_leader.is_nil()) return false;
        while (true) {
            auto leader = get_leader_id(pg_id);
            if (old_leader != leader) { return true; }
            LOGDEBUG("leader not change, leader_id={}", leader);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    void run_if_in_pg(pg_id_t pg_id, auto&& lambda) {
        if (am_i_in_pg(pg_id)) lambda();
    }

    bool am_i_in_pg(pg_id_t pg_id) {
        PGStats pg_stats;
        auto res = _obj_inst->pg_manager()->get_stats(pg_id, pg_stats);
        if (!res) return false;
        for (const auto& member : pg_stats.members) {
            if (member.id == g_helper->my_replica_id()) return true;
        }
        return false;
    }

    // wait for the last blob to be created locally, which means all the blob before this blob are created
    void wait_for_blob(shard_id_t shard_id, blob_id_t blob_id) {
        while (true) {
            if (blob_exist(shard_id, blob_id)) {
                LOGINFO("shard {} blob {} is created locally, which means all the blob before {} are created", shard_id,
                        blob_id, blob_id);
                return;
            }
            std::this_thread::sleep_for(1s);
        }
    }

    peer_id_t get_group_id(pg_id_t pg_id) const {
        auto pg = _obj_inst->get_hs_pg(pg_id);
        if (!pg) {
            LOGW("pg not found, pg_id={}", pg_id);
            return uuids::nil_uuid();
        }
        auto repl_dev = pg->repl_dev_;
        if (!repl_dev) {
            LOGW("repl_dev is null, pg_id={}", pg_id);
            return uuids::nil_uuid();
        }
        return repl_dev->group_id();
    }

private:
    bool pg_exist(pg_id_t pg_id) {
        std::vector< pg_id_t > pg_ids;
        _obj_inst->pg_manager()->get_pg_ids(pg_ids);
        return std::find(pg_ids.begin(), pg_ids.end(), pg_id) != pg_ids.end();
    }

    bool shard_exist(shard_id_t id, trace_id_t tid = 0) {
        auto r = _obj_inst->shard_manager()->get_shard(id, tid).get();
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

    uint64_t actual_written_blk_count_for_blob(blob_id_t blob_id) {
        using homeobject::io_align;
        auto blob = build_blob(blob_id);
        auto blob_size = blob.body.size();

        uint64_t actual_written_size{uint32_cast(sisl::round_up(sizeof(HSHomeObject::BlobHeader), io_align))};

        if (((r_cast< uintptr_t >(blob.body.cbytes()) % io_align) != 0) || ((blob_size % io_align) != 0)) {
            blob_size = sisl::round_up(blob_size, io_align);
        }

        actual_written_size += blob_size;

        auto pad_len = sisl::round_up(actual_written_size, HSHomeObject::_data_block_size) - actual_written_size;
        if (pad_len) { actual_written_size += pad_len; }

        return actual_written_size / HSHomeObject::_data_block_size;
    }

#ifdef _PRERELEASE
    void set_basic_flip(const std::string flip_name, int count = 1, uint32_t percent = 100) {
        flip::FlipCondition null_cond;
        flip::FlipFrequency freq;
        freq.set_count(count);
        freq.set_percent(percent);
        m_fc.inject_noreturn_flip(flip_name, {null_cond}, freq);
        LOGINFO("Flip {} set", flip_name);
    }

    template < typename T >
    void set_retval_flip(const std::string flip_name, const T retval, uint32_t count = 1, uint32_t percent = 100,
                         flip::FlipCondition cond = flip::FlipCondition()) {
        flip::FlipFrequency freq;
        freq.set_count(count);
        freq.set_percent(percent);
        ASSERT_TRUE(m_fc.inject_retval_flip(flip_name, {cond}, freq, retval));
        LOGINFO("Flip {} with returned value set, value={}", flip_name, retval);
    }

    void set_delay_flip(const std::string flip_name, uint64_t delay_usec, uint32_t count = 1, uint32_t percent = 100) {
        flip::FlipCondition null_cond;
        flip::FlipFrequency freq;
        freq.set_count(count);
        freq.set_percent(percent);
        m_fc.inject_delay_flip(flip_name, {null_cond}, freq, delay_usec);
        LOGINFO("Flip {} set", flip_name);
    }

    void set_callback_flip(const std::string flip_name, std::function< void() > callback, uint32_t count = 1,
                           uint32_t percent = 100) {
        flip::FlipCondition null_cond;
        flip::FlipFrequency freq;
        freq.set_count(count);
        freq.set_percent(percent);
        m_fc.inject_callback_flip(flip_name, {null_cond}, freq, callback);
        LOGINFO("Flip {} with callback set", flip_name);
    }

    template < typename T >
    void set_callback_retval_flip(const std::string flip_name, std::function< T() > callback, uint32_t count = 1,
                                  uint32_t percent = 100) {
        flip::FlipCondition null_cond;
        flip::FlipFrequency freq;
        freq.set_count(count);
        freq.set_percent(percent);
        ASSERT_TRUE(m_fc.inject_callback_retval_flip(flip_name, {null_cond}, freq, callback));
        LOGINFO("Flip {} with callback retval set", flip_name);
    }

    void remove_flip(const std::string flip_name) {
        m_fc.remove_flip(flip_name);
        LOGINFO("Flip {} removed", flip_name);
    }
#endif

    void RestartFollowerDuringBaselineResyncUsingSigKill(uint64_t flip_delay, uint64_t restart_interval,
                                                         string restart_phase);
    void RestartLeaderDuringBaselineResyncUsingSigKill(uint64_t flip_delay, uint64_t restart_interval,
                                                       string restart_phase);
    void ReplaceMember(bool withGC);

    void EmergentGC(bool with_crash_recovery);

private:
    std::random_device rnd{};
    std::default_random_engine rnd_engine{rnd()};
    std::uniform_int_distribution< uint32_t > rand_blob_size;
    std::uniform_int_distribution< uint32_t > rand_user_key_size;
#ifdef _PRERELEASE
    flip::FlipClient m_fc{iomgr_flip::instance()};
#endif
};
