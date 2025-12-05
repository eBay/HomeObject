/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
/*
 * Homeobject Replication testing binaries shared common definitions, apis and data structures
 */
#include "homeobj_fixture.hpp"
#include "homeobj_fixture_http.hpp"

#define BEFORE_FIRST_SHARD_DONE "BEFORE_FIRST_SHARD_DONE"
#define RECEIVING_SNAPSHOT "RECEIVING_SNAPSHOT"
#define APPLYING_SNAPSHOT "APPLYING_SNAPSHOT"
#define AFTER_BASELINE_RESYNC "AFTER_BASELINE_RESYNC"
#define TRUNCATING_LOGS "TRUNCATING_LOGS"

// Restart follower before first shard is complete to test null ctx_ handling
TEST_F(HomeObjectFixture, RestartFollowerBeforeFirstShardDone) {
    RestartFollowerDuringBaselineResyncUsingSigKill(5000, 1000, BEFORE_FIRST_SHARD_DONE);
}

// Restart during baseline resync
TEST_F(HomeObjectFixture, RestartFollowerDuringBaselineResyncWithoutTimeout) {
    RestartFollowerDuringBaselineResyncUsingSigKill(10000, 1000, RECEIVING_SNAPSHOT);
}

// Restart follower during baseline resync and timeout. Also test resumption and conflict with incoming traffic.
// Take default value as example, num_shards_per_pg=4, num_blobs_per_shard=5.
// 1. Put 1st round blobs in the pg: shard1(blob 0-4), shard2(blob 5-9), shard3(blob 10-14), shard4(blob 15-19).
// 2. Then replace a member, the new member will perform baseline resync.
// 3.a During the baseline resync(BR), restart it when blob id is 11(in shard3) to simulate a follower crash.
// 3.b Then put 2nd round blobs to each shard, shard1(blob 0-4, 20), shard2(blob 5-9, 21), shard3(blob 10-14, 22),
// shard4(blob 15-19, 23).
// 4. After restart, new member recovers from the context(skip shard1(blob 0-4), shard2(blob 5-9)) and resume to receive
// shard3(blob 10-14, 22), shard4(blob 15-19, 23).
// 5. After the baseline resync is completed, start incremental append-log requests: logs for blob 20-21 will fetch
// data, and logs for blob 22-23 will skip. Note: pay attention to num_reserved_log_items and snapshot_freq_distance, if
// a new snapshot triggered when handling the 2nd round put blobs, logs will be truncated and a new BR will be triggered
// in incremental resync stage.
TEST_F(HomeObjectFixture, RestartFollowerDuringBaselineResyncAndTimeout) {
    RestartFollowerDuringBaselineResyncUsingSigKill(10000, 10000, RECEIVING_SNAPSHOT);
}

// Restart follower when applying snapshot and timeout
TEST_F(HomeObjectFixture, RestartFollowerWhenApplyingSnapshot) {
    RestartFollowerDuringBaselineResyncUsingSigKill(10000, 10000, APPLYING_SNAPSHOT);
}

// Restart follower during baseline resync and timeout
TEST_F(HomeObjectFixture, RestartFollowerAfterBaselineResync) {
    RestartFollowerDuringBaselineResyncUsingSigKill(10000, 1000, AFTER_BASELINE_RESYNC);
}

// Restart follower when truncating logs
// TEST_F(HomeObjectFixture, RestartFollowerWhenTruncatingLogs) {
//     RestartFollowerDuringBaselineResyncUsingSigKill(10000, 1000, TRUNCATING_LOGS);
// }

// Test case to restart new member during baseline resync, it will start 4 process to simulate the 4 replicas, let's say
// P0, P1, P2 and P3. P0, P1, P2 are the original members of the pg, P3 is the spare replica. After the
// start_replace_member happens, P3 will join the pg, and then kill itself(sigkill) to simulate the restart during
// baseline resync. As P0 is the original process who spawn the other 3 processes, so P0 will also help to spawn a new
// process to simulate the new member restart.
void HomeObjectFixture::RestartFollowerDuringBaselineResyncUsingSigKill(uint64_t flip_delay, uint64_t restart_interval,
                                                                        string restart_phase) {
    LOGINFO("HomeObject replica={} setup completed", g_helper->replica_num());
    auto spare_num_replicas = SISL_OPTIONS["spare_replicas"].as< uint8_t >();
    ASSERT_TRUE(spare_num_replicas > 0) << "we need spare replicas for homestore backend dynamic tests";

    auto is_restart = g_helper->is_current_testcase_restarted();
    auto num_replicas = SISL_OPTIONS["replicas"].as< uint8_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >();
    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    pg_id_t pg_id{1};
    auto out_member_id = g_helper->replica_id(num_replicas - 1);
    auto in_member_id = g_helper->replica_id(num_replicas); /*spare replica*/

    // ======== Stage 1: Create a pg without spare replicas and put blobs ========
    std::unordered_set< uint8_t > excluding_replicas_in_pg;
    for (size_t i = num_replicas; i < num_replicas + spare_num_replicas; i++)
        excluding_replicas_in_pg.insert(i);

    if (!is_restart) { create_pg(pg_id, 0 /* pg_leader */, excluding_replicas_in_pg); }

    // we can not share all the shard_id and blob_id among all the replicas including the spare ones, so we need to
    // derive them by calculating.
    // since shard_id = pg_id + shard_sequence_num, so we can derive shard_ids for all the shards in this pg, and these
    // derived info is used by all replicas(including the newly added member) to verify the blobs.
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    pg_blob_id[pg_id] = 0;
    for (shard_id_t shard_id = 1; shard_id <= num_shards_per_pg; shard_id++) {
        auto derived_shard_id = make_new_shard_id(pg_id, shard_id);
        pg_shard_id_vec[pg_id].emplace_back(derived_shard_id);
    }
    auto last_shard = pg_shard_id_vec[pg_id].back();
    // put one more blob in every shard to test incremental resync.
    auto last_blob = num_blobs_per_shard * num_shards_per_pg + num_shards_per_pg - 1;

    auto kill_until_shard = pg_shard_id_vec[pg_id].back();
    auto kill_until_blob = num_blobs_per_shard * num_shards_per_pg - 1;
#ifdef _PRERELEASE
    if (!is_restart && in_member_id == g_helper->my_replica_id()) {
        if (restart_phase == BEFORE_FIRST_SHARD_DONE) {
            LOGINFO("Test case: restart follower before first shard done={}", restart_phase);
            flip::FlipCondition cond;
            // Delay happens before writing blob 2, so we kill after blob 1 is written
            m_fc.create_condition("blob_id", flip::Operator::EQUAL, static_cast< long >(2), &cond);
            set_retval_flip("simulate_write_snapshot_save_blob_delay", static_cast< long >(flip_delay) /*ms*/, 1, 100,
                            cond);
            // Wait for blob 1, then kill before the first shard is complete (first shard has blobs 0 to
            // num_blobs_per_shard-1)
            kill_until_shard = pg_shard_id_vec[pg_id].front();
            kill_until_blob = 1;
        } else if (restart_phase == RECEIVING_SNAPSHOT) {
            LOGINFO("Test case: restart follower when receiving snapshot={}", restart_phase);
            flip::FlipCondition cond;
            // will only delay the snapshot with blob id 11 during which restart will happen
            m_fc.create_condition("blob_id", flip::Operator::EQUAL, static_cast< long >(11), &cond);
            set_retval_flip("simulate_write_snapshot_save_blob_delay", static_cast< long >(flip_delay) /*ms*/, 1, 100,
                            cond);
            // kill after the last blob in the first shard is replicated
            kill_until_shard = pg_shard_id_vec[pg_id].front();
            kill_until_blob = num_blobs_per_shard - 1;
        } else if (restart_phase == APPLYING_SNAPSHOT) {
            LOGINFO("Test case: restart follower when applying snapshot={}", restart_phase);
            set_retval_flip("simulate_apply_snapshot_delay", static_cast< long >(flip_delay) /*ms*/, 1, 100);
        } else if (restart_phase == TRUNCATING_LOGS) {
            flip::FlipCondition cond;
            // TODO This flip should be added in homestore
            m_fc.create_condition("compact_lsn", flip::Operator::EQUAL, static_cast< long >(26), &cond);
            LOGINFO("Test case: restart follower when truncating logs={}", restart_phase);
            set_retval_flip("simulate_truncate_log_delay", static_cast< long >(flip_delay) /*ms*/, 1, 100, cond);
        } else {
            LOGWARN("restart after baseline resync={}", restart_phase);
        }
    }
#endif
    std::string task_id = "task_id";
    if (!is_restart) {
        for (uint64_t j = 0; j < num_shards_per_pg; j++)
            create_shard(pg_id, 64 * Mi, "shard meta:" + std::to_string(pg_shard_id_vec[pg_id][j]));

        // put and verify blobs in the pg, excluding the spare replicas
        put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id, true, true);

        verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
        verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);

        // all the replicas , including the spare ones, sync at this point
        g_helper->sync();

        // ======== Stage 2: replace a member ========
        LOGINFO("start replace member, pg={}, task_id={}", pg_id, task_id);
        run_on_pg_leader(pg_id, [&]() {
            auto r = _obj_inst->pg_manager()
                         ->replace_member(pg_id, task_id, out_member_id, PGMember{in_member_id, "new_member", 0})
                         .get();
            ASSERT_TRUE(r);
        });

        // ======== Stage 3: the new member will kill itself to simulate restart, then P0 will help start it ========
        if (in_member_id == g_helper->my_replica_id()) {
            while (!am_i_in_pg(pg_id)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                LOGINFO("new member is waiting to become a member of pg={}", pg_id);
            }
            LOGDEBUG("wait for the data[shard={}, blob={}] replicated to the new member", kill_until_shard,
                     kill_until_blob);
            wait_for_blob(kill_until_shard, kill_until_blob);
            LOGINFO("about to kill new member");
            sleep(3);
            // SyncPoint 1(new member): kill itself.
            g_helper->sync();
            // verify the intermediate result of replace_member
            // if (restart_phase == RECEIVING_SNAPSHOT) {
            //     ASSERT_TRUE(verify_start_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
            //     auto progress = get_br_progress(pg_id);
            //     LOGDEBUG("baseline resync progress={}", progress);
            //     ASSERT_GT(progress, 0);
            // }
            kill();
        }

        // SyncPoint 1(others): wait for the new member stop, then P0 will help start it.
        LOGINFO("waiting for new member stop")
        g_helper->sync();
        // if (restart_phase == RECEIVING_SNAPSHOT) {
        //     ASSERT_TRUE(verify_start_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
        // }
        if (g_helper->replica_num() == 0) {
            // wait for kill
            std::this_thread::sleep_for(std::chrono::milliseconds(restart_interval));
            LOGINFO("going to restart new member")
            g_helper->spawn_homeobject_process(num_replicas, true);
        }

        // SyncPoint 2: put more blobs when the new member is restarted.
        // g_helper->sync() will be called in new process setup and pub_blobs implicitly.
        LOGINFO("going to put more blobs")
        pg_blob_id[pg_id] = num_blobs_per_shard * num_shards_per_pg;
        g_helper->sync();
        if (out_member_id != g_helper->my_replica_id()) {
            put_blobs(pg_shard_id_vec, 1, pg_blob_id, false);
            wait_for_blob(last_shard, last_blob);
        }

        // SyncPoint 3: wait for new member to verify all blobs.
        g_helper->sync();
    } else {
        // new member restart
        while (!am_i_in_pg(pg_id)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            LOGINFO("new member is waiting to become a member of pg={}", pg_id);
        }
        run_if_in_pg(pg_id, [&]() {
            wait_for_blob(last_shard, last_blob);
            verify_shard_meta(pg_shard_id_vec);
            // 1st round blobs
            verify_get_blob(pg_shard_id_vec, num_blobs_per_shard, false, true);
            // 2nd round blobs
            pg_blob_id[pg_id] = num_blobs_per_shard * num_shards_per_pg;
            verify_get_blob(pg_shard_id_vec, 1, false, true, pg_blob_id);
            verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard + 1, false);
        });
        // SyncPoint 3(new member): replication done, notify others.
        g_helper->sync();
    }
    if (out_member_id == g_helper->my_replica_id()) {
        // TODO : revisit the assert here after we have the ability to force destroy pg on out member(there is a corner
        // case that the pg
        //  LOGINFO("Destroying PG on removed member");
        //  while (am_i_in_pg(pg_id)) {
        //      std::this_thread::sleep_for(std::chrono::milliseconds(500));
        //      LOGINFO("Old member is waiting to leave pg={}", pg_id);
        //  }
        g_helper->sync();
    } else {
        g_helper->sync();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ASSERT_TRUE(verify_complete_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
    }

    g_helper->sync();
}

TEST_F(HomeObjectFixture, RestartFollowerDuringBaselineResyncUsingGracefulShutdown) {
    LOGINFO("HomeObject replica={} setup completed", g_helper->replica_num());
    auto spare_num_replicas = SISL_OPTIONS["spare_replicas"].as< uint8_t >();
    ASSERT_TRUE(spare_num_replicas > 0) << "we need spare replicas for homestore backend dynamic tests";

    auto num_replicas = SISL_OPTIONS["replicas"].as< uint8_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >();
    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    pg_id_t pg_id{1};
    auto out_member_id = g_helper->replica_id(num_replicas - 1);
    auto in_member_id = g_helper->replica_id(num_replicas); /*spare replica*/

    // ======== Stage 1: Create a pg without spare replicas and put blobs ========
    std::unordered_set< uint8_t > excluding_replicas_in_pg;
    for (size_t i = num_replicas; i < num_replicas + spare_num_replicas; i++)
        excluding_replicas_in_pg.insert(i);

    create_pg(pg_id, 0 /* pg_leader */, excluding_replicas_in_pg);

    // we can not share all the shard_id and blob_id among all the replicas including the spare ones, so we need to
    // derive them by calculating.
    // since shard_id = pg_id + shard_sequence_num, so we can derive shard_ids for all the shards in this pg, and these
    // derived info is used by all replicas(including the newly added member) to verify the blobs.
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    pg_blob_id[pg_id] = 0;
    for (shard_id_t shard_id = 1; shard_id <= num_shards_per_pg; shard_id++) {
        auto derived_shard_id = make_new_shard_id(pg_id, shard_id);
        pg_shard_id_vec[pg_id].emplace_back(derived_shard_id);
    }
    auto last_shard = pg_shard_id_vec[pg_id].back();
    // put one more blob in every shard to test incremental resync.
    auto last_blob = num_blobs_per_shard * num_shards_per_pg - 1;

    auto kill_until_shard = pg_shard_id_vec[pg_id].back();
    auto kill_until_blob = num_blobs_per_shard * num_shards_per_pg - 1;
#ifdef _PRERELEASE
    flip::FlipCondition cond;
    // will only delay the snapshot with blob id 11 during which restart will happen
    m_fc.create_condition("blob_id", flip::Operator::EQUAL, static_cast< long >(7), &cond);
    set_retval_flip("simulate_write_snapshot_save_blob_delay", static_cast< long >(10000) /*ms*/, 1, 100, cond);
    // kill after the last blob in the first shard is replicated
    kill_until_shard = pg_shard_id_vec[pg_id].front();
    kill_until_blob = num_blobs_per_shard - 1;
#endif

    for (uint64_t j = 0; j < num_shards_per_pg; j++)
        create_shard(pg_id, 64 * Mi, "shard meta:" + std::to_string(pg_shard_id_vec[pg_id][j]));

    // put and verify blobs in the pg, excluding the spare replicas
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id, true, true);

    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
    verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);

    // SyncPoint1 all the replicas , including the spare ones, sync at this point
    g_helper->sync();

    // ======== Stage 2: replace a member ========
    std::string task_id = "task_id";
    run_on_pg_leader(pg_id, [&]() {
        auto r = _obj_inst->pg_manager()
                     ->replace_member(pg_id, task_id, out_member_id, PGMember{in_member_id, "new_member", 0})
                     .get();
        ASSERT_TRUE(r);
    });

    // ======== Stage 3: the new member will kill itself to simulate restart, then P0 will help start it ========
    if (in_member_id == g_helper->my_replica_id()) {
        while (!am_i_in_pg(pg_id)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            LOGINFO("new member is waiting to become a member of pg={}", pg_id);
        }
        LOGDEBUG("wait for the data[shard={}, blob={}] replicated to the new member", kill_until_shard,
                 kill_until_blob);
        wait_for_blob(kill_until_shard, kill_until_blob);
        // SyncPoint2
        g_helper->sync();
        LOGINFO("about to restart new member")
        restart();
        LOGINFO("restart, wait for data replication")
        if (in_member_id == g_helper->my_replica_id()) {
            wait_for_blob(last_shard, last_blob);
            verify_shard_meta(pg_shard_id_vec);
            verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
            verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);
        }
    } else {
        // SyncPoint2, verify intermediate status during replacement
        g_helper->sync();
        // ASSERT_TRUE(verify_start_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
    }
    // SyncPoint3, after new member restart
    g_helper->sync();
    LOGINFO("All data replicated to the new member, wait for old member to leave pg={}", pg_id);
    if (out_member_id == g_helper->my_replica_id()) {
        // TODO : revisit the assert here after we have the ability to force destroy pg on out member(there is a corner
        // case that the pg
        //  LOGINFO("Destroying PG on removed member");
        //  while (am_i_in_pg(pg_id)) {
        //      std::this_thread::sleep_for(std::chrono::milliseconds(500));
        //      LOGINFO("Old member is waiting to leave pg={}", pg_id);
        //  }
        //  SyncPoint4, sync for verify_complete_replace_member_result
        g_helper->sync();
    } else {
        // SyncPoint4, sync for verify_complete_replace_member_result
        g_helper->sync();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ASSERT_TRUE(verify_complete_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
    }
    g_helper->sync();
}

TEST_F(HomeObjectFixture, ReplaceMember) { ReplaceMember(false); }

TEST_F(HomeObjectFixture, FetchDataWithOriginatorGC) { ReplaceMember(true); }

void HomeObjectFixture::ReplaceMember(bool withGC) {
    LOGINFO("HomeObject replica={} setup completed", g_helper->replica_num());
    auto spare_num_replicas = SISL_OPTIONS["spare_replicas"].as< uint8_t >();
    ASSERT_TRUE(spare_num_replicas > 0) << "we need spare replicas for homestore backend dynamic tests";

    // step 1:  Create a pg without spare replicas.
    auto num_replicas = SISL_OPTIONS["replicas"].as< uint8_t >();
    std::unordered_set< uint8_t > excluding_replicas_in_pg;
    for (size_t i = num_replicas; i < num_replicas + spare_num_replicas; i++)
        excluding_replicas_in_pg.insert(i);

    pg_id_t pg_id{1};
    create_pg(pg_id, 0 /* pg_leader */, excluding_replicas_in_pg);

    // step 2: create shard and put a blob in the pg
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >();
    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;

    // we can not share all the shard_id and blob_id among all the replicas including the spare ones, so we need to
    // derive them by calculating.
    // since shard_id = pg_id + shard_sequence_num, so we can derive shard_ids for all the shards in this pg, and these
    // derived info is used by all replicas(including the newly added member) to verify the blobs.
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    for (shard_id_t shard_id = 1; shard_id <= num_shards_per_pg; shard_id++) {
        auto derived_shard_id = make_new_shard_id(pg_id, shard_id);
        pg_shard_id_vec[pg_id].emplace_back(derived_shard_id);
    }

    // last shard is empty shard
    for (uint64_t j = 0; j < num_shards_per_pg + 1; j++) {
        auto derived_shard_id = make_new_shard_id(pg_id, j + 1);
        create_shard(pg_id, 64 * Mi, "shard meta:" + std::to_string(derived_shard_id));
    }

    // TODO:: if we add delete blobs case in baseline resync, we need also derive the last blob_id in this pg for spare
    // replicas
    pg_blob_id[pg_id] = 0;

    // put and verify blobs in the pg, excluding the spare replicas
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id, true, true);

    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
    verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);

    if (withGC) {
        run_if_in_pg(pg_id, [&]() {
            auto gc_mgr = _obj_inst->gc_manager();
            std::vector< folly::SemiFuture< bool > > futs;
            for (const auto& [pg_id, shards] : pg_shard_id_vec) {
                for (const auto& shard_id : shards) {
                    auto chunk_id_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
                    RELEASE_ASSERT(chunk_id_opt.has_value(), "failed to get chunk_id for shard {}", shard_id);
                    futs.emplace_back(gc_mgr->submit_gc_task(task_priority::emergent, chunk_id_opt.value()));
                }
            }
            // wait for all egc completed
            folly::collectAllUnsafe(futs)
                .thenValue([](auto&& results) {
                    for (auto const& ok : results) {
                        ASSERT_TRUE(ok.hasValue());
                        // all egc task should be completed
                        ASSERT_TRUE(ok.value());
                    }
                })
                .get();

            futs.clear();
        });
    }

    // all the replicas , including the spare ones, sync at this point
    g_helper->sync();

    // step 3: replace a member
    auto out_member_id = g_helper->replica_id(num_replicas - 1);
    auto in_member_id = g_helper->replica_id(num_replicas); /*spare replica*/

    // get out_member's index_table_uuid with pg_id
    string index_table_uuid_str;
    if (out_member_id == g_helper->my_replica_id()) {
        auto hs_pg = _obj_inst->get_hs_pg(pg_id);
        RELEASE_ASSERT(hs_pg, "PG not found");
        index_table_uuid_str = uuids::to_string(hs_pg->pg_sb_->index_table_uuid);
    }

#ifdef _PRERELEASE
    // Set flips (name, count, percentage) to simulate different error scenarios
    set_basic_flip("pg_blob_iterator_create_snapshot_data_error", 1);     // simulate read pg snapshot data error
    set_basic_flip("pg_blob_iterator_generate_shard_blob_list_error", 1); // simulate generate shard blob list error
    set_basic_flip("pg_blob_iterator_load_blob_data_error", 1, 10);       // simulate load blob data error

    set_basic_flip("state_machine_write_corrupted_data", 3, 25);       // simulate random data corruption
    set_basic_flip("snapshot_receiver_pg_error", 1);                   // simulate pg creation error
    set_basic_flip("snapshot_receiver_shard_write_data_error", 2, 33); // simulate shard write data error
    set_basic_flip("snapshot_receiver_blob_write_data_error", 4, 15);  // simulate blob write data error
    set_basic_flip("snapshot_receiver_blk_allocation_error", 4, 15);   // simulate blob allocation error
#endif

    std::string task_id = "task_id";
    LOGINFO("start replace member, pg={}, task_id={}", pg_id, task_id);
    run_on_pg_leader(pg_id, [&]() {
        auto r = _obj_inst->pg_manager()
                     ->replace_member(pg_id, task_id, out_member_id, PGMember{in_member_id, "new_member", 0})
                     .get();
        ASSERT_TRUE(r);
    });

    // the new member should wait until it joins the pg and all the blobs are replicated to it
    if (in_member_id == g_helper->my_replica_id()) {
        while (!am_i_in_pg(pg_id)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            LOGINFO("new member is waiting to become a member of pg={}", pg_id);
        }

        wait_for_blob(pg_shard_id_vec[pg_id].back() /*the last shard id in this pg*/,
                      num_shards_per_pg * num_blobs_per_shard - 1 /*the last blob id in this pg*/);

        sleep(5); // wait for incremental append-log requests to complete
    }

    // step 4: after completing replace member, verify the blob on all members of this pg including the newly added
    // spare replica,
    run_if_in_pg(pg_id, [&]() {
        verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
        verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);
    });

    g_helper->sync();
    // Because we don't know when hs triggers complete_replace_member, so it's hard to find an accurate time slot to
    // verify verify_start_replace_member_result(pg_id, out_member_id, in_member_id); step 5: Verify no pg related data
    // in out_member
    if (out_member_id == g_helper->my_replica_id()) {
        while (am_i_in_pg(pg_id)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            LOGINFO("old member is waiting to leave pg={}", pg_id);
        }
        verify_pg_destroy(pg_id, index_table_uuid_str, pg_shard_id_vec[pg_id], true);
        // since this case out_member don't have any pg, so we can check each chunk.
        for (const auto& [_, chunk] : _obj_inst->chunk_selector()->m_chunks) {
            // TODO: revisit the assert here after we implement gc recovery
            ASSERT_TRUE(chunk->m_state == ChunkState::AVAILABLE || chunk->m_state == ChunkState::GC);
            if (!withGC) { ASSERT_EQ(chunk->available_blks(), chunk->get_total_blks()); }
        }
        LOGINFO("check no pg related data in out member successfully");
        g_helper->sync();
    } else {
        g_helper->sync();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ASSERT_TRUE(verify_complete_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
    }

    // Step 6: restart, verify the blobs again on all members, including the new spare replica, and out_member
    restart();
    run_if_in_pg(pg_id, [&]() {
        verify_shard_meta(pg_shard_id_vec);
        verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
        verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);
        LOGINFO("After restart, check pg related data in pg members successfully");
    });

    if (out_member_id == g_helper->my_replica_id()) {
        verify_pg_destroy(pg_id, index_table_uuid_str, pg_shard_id_vec[pg_id]);
        // since this case out_member don't have any pg, so we can check each chunk.
        for (const auto& [_, chunk] : _obj_inst->chunk_selector()->m_chunks) {
            // TODO: revisit the assert here after we implement gc recovery
            ASSERT_TRUE(chunk->m_state == ChunkState::AVAILABLE || chunk->m_state == ChunkState::GC);
            if (!withGC) { ASSERT_EQ(chunk->available_blks(), chunk->get_total_blks()); }
        }
        LOGINFO("After restart, check no pg related data in out member successfully");
    } else {
        ASSERT_TRUE(verify_complete_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
    }
}

TEST_F(HomeObjectFixture, RestartLeaderDuringBaselineResync) {
    RestartLeaderDuringBaselineResyncUsingSigKill(10000, 1000, RECEIVING_SNAPSHOT);
}

TEST_F(HomeObjectFixture, RestartLeaderWhenApplySnapshot) {
    RestartLeaderDuringBaselineResyncUsingSigKill(10000, 1000, APPLYING_SNAPSHOT);
}

TEST_F(HomeObjectFixture, RestartLeaderAfterBaselineResync) {
    RestartLeaderDuringBaselineResyncUsingSigKill(10000, 1000, AFTER_BASELINE_RESYNC);
}

// Need to restart the leader for process sync
void HomeObjectFixture::RestartLeaderDuringBaselineResyncUsingSigKill(uint64_t flip_delay, uint64_t restart_interval,
                                                                      string restart_phase) {
    LOGINFO("HomeObject replica={} setup completed", g_helper->replica_num());
    auto spare_num_replicas = SISL_OPTIONS["spare_replicas"].as< uint8_t >();
    ASSERT_TRUE(spare_num_replicas > 0) << "we need spare replicas for homestore backend dynamic tests";

    auto is_restart = g_helper->is_current_testcase_restarted();
    auto num_replicas = SISL_OPTIONS["replicas"].as< uint8_t >();
    pg_id_t pg_id{1};

    std::unordered_set< uint8_t > excluding_replicas_in_pg;
    for (size_t i = num_replicas; i < num_replicas + spare_num_replicas; i++)
        excluding_replicas_in_pg.insert(i);

    auto initial_leader_replica_num = 1;
    if (!is_restart) { create_pg(pg_id, initial_leader_replica_num /* pg_leader */, excluding_replicas_in_pg); }
    peer_id_t initial_leader_replica_id = get_leader_id(pg_id);

    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >();
    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;

    // we can not share all the shard_id and blob_id among all the replicas including the spare ones, so we need to
    // derive them by calculating.
    // since shard_id = pg_id + shard_sequence_num, so we can derive shard_ids for all the shards in this pg, and these
    // derived info is used by all replicas(including the newly added member) to verify the blobs.
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    pg_blob_id[pg_id] = 0;
    for (shard_id_t shard_id = 1; shard_id <= num_shards_per_pg; shard_id++) {
        auto derived_shard_id = make_new_shard_id(pg_id, shard_id);
        pg_shard_id_vec[pg_id].emplace_back(derived_shard_id);
    }

    auto last_shard = pg_shard_id_vec[pg_id].back();
    // put one more blob in every shard to test incremental resync.
    auto last_blob = num_blobs_per_shard * num_shards_per_pg + num_shards_per_pg - 1;

    auto out_member_id = g_helper->replica_id(num_replicas - 1);
    auto in_member_id = g_helper->replica_id(num_replicas); /*spare replica*/

    std::string index_table_uuid_str;
    if (out_member_id == g_helper->my_replica_id_) {
        auto hs_pg = _obj_inst->get_hs_pg(pg_id);
        index_table_uuid_str = uuids::to_string(hs_pg->pg_sb_->index_table_uuid);
    }
    std::string task_id = "task_id";
    if (!is_restart) {
        auto kill_until_shard = pg_shard_id_vec[pg_id].back();
        auto kill_until_blob = num_blobs_per_shard * num_shards_per_pg - 1;
        if (restart_phase == RECEIVING_SNAPSHOT) {
            // kill after the last blob in the first shard is replicated
            kill_until_shard = pg_shard_id_vec[pg_id].front();
            kill_until_blob = num_blobs_per_shard - 1;
        }
#ifdef _PRERELEASE
        if (initial_leader_replica_num == g_helper->replica_num()) {
            if (restart_phase == RECEIVING_SNAPSHOT) {
                LOGINFO("restart when receiving snapshot={}, kill_until_shard={}, kill_until_blob={}", restart_phase,
                        kill_until_shard, kill_until_blob);
                flip::FlipCondition cond;
                // will only delay the snapshot with blob id 7 during which restart will happen
                m_fc.create_condition("blob_id", flip::Operator::EQUAL, static_cast< long >(7), &cond);
                set_retval_flip("simulate_read_snapshot_load_blob_delay", static_cast< long >(flip_delay) /*ms*/, 1,
                                100, cond);
            } else if (restart_phase == APPLYING_SNAPSHOT) {
                LOGINFO("restart when applying snapshot={}", restart_phase);
                set_retval_flip("simulate_apply_snapshot_delay", static_cast< long >(flip_delay) /*ms*/, 1, 100);
            } else {
                LOGWARN("restart after baseline resync={}", restart_phase);
            }
        }
#endif

        // ========Stage 1:  Create a pg without spare replicas and put blobs========

        for (uint64_t j = 0; j < num_shards_per_pg; j++)
            create_shard(pg_id, 64 * Mi, "shard meta:" + std::to_string(pg_shard_id_vec[pg_id][j]));

        // put and verify blobs in the pg, excluding the spare replicas
        put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id, true, true);

        verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
        verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);

        // all the replicas , including the spare ones, sync at this point
        g_helper->sync();

        // ========Stage 2: replace a member========
        run_on_pg_leader(pg_id, [&]() {
            auto r = _obj_inst->pg_manager()
                         ->replace_member(pg_id, task_id, out_member_id, PGMember{in_member_id, "new_member", 0})
                         .get();
            ASSERT_TRUE(r);
        });
        initial_leader_replica_id = get_leader_id(pg_id);
        // ========Stage 3: kill leader ========
        if (in_member_id == g_helper->my_replica_id()) {
            while (!am_i_in_pg(pg_id)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                LOGINFO("new member is waiting to become a member of pg={}", pg_id);
            }
            LOGDEBUG("wait for the data[shard={}, blob={}] replicated to the new member", kill_until_shard,
                     kill_until_blob);
            wait_for_blob(kill_until_shard, kill_until_blob);
        } else if (initial_leader_replica_num == g_helper->replica_num()) {
            // SyncPoint 1(leader)
            g_helper->sync();
            LOGINFO("going to kill leader");
            // if (restart_phase == RECEIVING_SNAPSHOT) {
            //     ASSERT_TRUE(verify_start_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
            // }
            kill();
        }
        // SyncPoint 1: tell leader to kill
        g_helper->sync();
        // if (restart_phase == RECEIVING_SNAPSHOT) {
        //     ASSERT_TRUE(verify_start_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
        //     if (in_member_id == g_helper->my_replica_id()) {
        //         auto progress = get_br_progress(pg_id);
        //         LOGDEBUG("baseline resync progress={}", progress);
        //         ASSERT_GT(progress, 0);
        //     }
        // }
        // out member helps to spawn a new process to simulate the leader restart
        if (out_member_id == g_helper->my_replica_id()) {
            std::thread spawn_thread([restart_interval, initial_leader_replica_num]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(restart_interval));
                LOGINFO("going to restart replica {}", initial_leader_replica_num)
                g_helper->spawn_homeobject_process(initial_leader_replica_num, true);
            });
            spawn_thread.detach();
        }
        LOGINFO("wait for leader restart");
        // SyncPoint 2: other: wait leader restart
        g_helper->sync();
    } else { // is_restart == true, path for restarted leader
        // SyncPoint 2: Leader: restarted
        // The sync() was called in Setup().
        LOGINFO("leader restarted");
    }
    if (out_member_id != g_helper->my_replica_id()) {
        // make sure there is a leader
        get_leader_id(pg_id);
    }

    if (initial_leader_replica_num == g_helper->replica_num()) {
        LOGINFO("I was the previous leader, now my_id={}, leader_id={}", g_helper->my_replica_id(),
                get_leader_id(pg_id));
    }

    LOGINFO("going to put more blobs")
    pg_blob_id[pg_id] = num_blobs_per_shard * num_shards_per_pg;
    g_helper->sync(); // make sure new member is ready to receive blobs
    if (out_member_id != g_helper->my_replica_id()) {
        put_blobs(pg_shard_id_vec, 1, pg_blob_id, false);
        wait_for_blob(last_shard, last_blob);
    }

    if (in_member_id == g_helper->my_replica_id()) {
        run_if_in_pg(pg_id, [&]() {
            LOGDEBUG("verify blobs");
            // 1st round blobs
            verify_get_blob(pg_shard_id_vec, num_blobs_per_shard, false, true);
            // // 2nd round blobs
            pg_blob_id[pg_id] = num_blobs_per_shard * num_shards_per_pg;
            verify_shard_meta(pg_shard_id_vec);
            verify_get_blob(pg_shard_id_vec, 1, false, true, pg_blob_id);
            verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard + 1, false);
        });
    }
    LOGINFO("wait for all blobs replicated to the new member")
    // SyncPoint 3: waiting for all the blobs replicated to the new member
    g_helper->sync();

    // ========Stage 4: Complete replace member, remove old member ========
    if (out_member_id == g_helper->my_replica_id()) {
        // TODO : revisit the assert here after we have the ability to force destroy pg on out member(there is a corner
        // case that the pg can not be cleaned up)
        //  while (am_i_in_pg(pg_id)) {
        //      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        //      LOGINFO("old member is waiting to leave pg={}", pg_id);
        //  }
        //  verify_pg_destroy(pg_id, index_table_uuid_str, pg_shard_id_vec[pg_id], true);
        //  // since this case out_member don't have any pg, so we can check each chunk.
        //  for (const auto& [_, chunk] : _obj_inst->chunk_selector()->m_chunks) {
        //      ASSERT_TRUE(chunk->m_state == ChunkState::AVAILABLE || chunk->m_state == ChunkState::GC);
        //      ASSERT_EQ(chunk->available_blks(), chunk->get_total_blks());
        //  }
        //  LOGINFO("check no pg related data in out member successfully");
        g_helper->sync();
    } else {
        g_helper->sync();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ASSERT_TRUE(verify_complete_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
    }
    g_helper->sync();
}

// In this test, the new member will be stuck at snapshot, at that moment we rollback the
// operation(remove new member, flip learner and clean the task).
TEST_F(HomeObjectFixture, RollbackReplaceMember) {
    LOGINFO("HomeObject replica={} setup completed", g_helper->replica_num());
    auto spare_num_replicas = SISL_OPTIONS["spare_replicas"].as< uint8_t >();
    ASSERT_TRUE(spare_num_replicas > 0) << "we need spare replicas for homestore backend dynamic tests";

    auto num_replicas = SISL_OPTIONS["replicas"].as< uint8_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >();
    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    pg_id_t pg_id{1};
    auto out_member_id = g_helper->replica_id(num_replicas - 1);
    auto in_member_id = g_helper->replica_id(num_replicas); /*spare replica*/
    auto http_enabled = SISL_OPTIONS["enable_http"].as< bool >();

    // ======== Stage 1: Create a pg without spare replicas and put blobs ========
    std::unordered_set< uint8_t > excluding_replicas_in_pg;
    for (size_t i = num_replicas; i < num_replicas + spare_num_replicas; i++)
        excluding_replicas_in_pg.insert(i);

    create_pg(pg_id, 0 /* pg_leader */, excluding_replicas_in_pg);

    // we can not share all the shard_id and blob_id among all the replicas including the spare ones, so we need to
    // derive them by calculating.
    // since shard_id = pg_id + shard_sequence_num, so we can derive shard_ids for all the shards in this pg, and these
    // derived info is used by all replicas(including the newly added member) to verify the blobs.
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    pg_blob_id[pg_id] = 0;
    for (shard_id_t shard_id = 1; shard_id <= num_shards_per_pg; shard_id++) {
        auto derived_shard_id = make_new_shard_id(pg_id, shard_id);
        pg_shard_id_vec[pg_id].emplace_back(derived_shard_id);
    }

    auto kill_until_shard = pg_shard_id_vec[pg_id].back();
    auto kill_until_blob = num_blobs_per_shard * num_shards_per_pg - 1;
#ifdef _PRERELEASE
    flip::FlipCondition cond;
    // will only delay the snapshot with blob id 7 during which shutdown will happen
    m_fc.create_condition("blob_id", flip::Operator::EQUAL, static_cast< long >(7), &cond);
    set_retval_flip("simulate_write_snapshot_save_blob_delay", static_cast< long >(10000) /*ms*/, 1, 100, cond);
    // kill after the last blob in the first shard is replicated
    kill_until_shard = pg_shard_id_vec[pg_id].front();
    kill_until_blob = num_blobs_per_shard - 1;
#endif

    for (uint64_t j = 0; j < num_shards_per_pg; j++)
        create_shard(pg_id, 64 * Mi);

    // put and verify blobs in the pg, excluding the spare replicas
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id, true, true);

    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
    verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);

    // SyncPoint1 all the replicas , including the spare ones, sync at this point
    g_helper->sync();

    // ======== Stage 2: replace a member ========
    std::string task_id = "task_id";
    run_on_pg_leader(pg_id, [&]() {
        auto r = _obj_inst->pg_manager()
                     ->replace_member(pg_id, task_id, out_member_id, PGMember{in_member_id, "new_member", 0})
                     .get();
        ASSERT_TRUE(r);
    });

    // ======== Stage 3: rollback the replace member during snapshot ========
    if (in_member_id == g_helper->my_replica_id()) {
        while (!am_i_in_pg(pg_id)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            LOGINFO("new member is waiting to become a member of pg={}", pg_id);
        }
        LOGDEBUG("wait for the data[shard={}, blob={}] replicated to the new member", kill_until_shard,
                 kill_until_blob);
        wait_for_blob(kill_until_shard, kill_until_blob);
        // SyncPoint2
        g_helper->sync();
        // SyncPoint3
        g_helper->sync();
        // SyncPoint4
        g_helper->sync();
        // SyncPoint5
        g_helper->sync();
        // SyncPoint6
        g_helper->sync();
    } else {
        // SyncPoint2, verify intermediate status during replacement
        g_helper->sync();
        ASSERT_TRUE(verify_start_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
        // SyncPoint3 remove member
        g_helper->sync();
        LOGINFO("about to remove new member")
        run_on_pg_leader(pg_id, [&]() {
            bool retry = true;
            while (retry) {
                if (http_enabled) {
                    HttpHelper http_helper("127.0.0.1", 5000 + g_helper->replica_num());
                    nlohmann::json j;
                    j["pg_id"] = std::to_string(pg_id);
                    j["member_id"] = boost::uuids::to_string(in_member_id);
                    j["commit_quorum"] = "0";
                    auto r = http_helper.del("/api/v1/member", j.dump());
                    if (r.code() == Pistache::Http::Code::Ok) {
                        retry = false;
                    } else if (r.code() == Pistache::Http::Code::Service_Unavailable) {
                        LOGINFO("remove_member get RETRY_REQUEST error, will retry");
                        std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    } else {
                        ASSERT_FALSE(true);
                    }
                } else {
                    auto r = _obj_inst->pg_manager()->remove_member(pg_id, in_member_id, 0, 0).get();
                    // new member can not respond to remove_member request because it is stuck at snapshot, so we may
                    // get RETRY_REQUEST error here, but the remove_member takes effective after force removal timeout.
                    if (!r.hasError()) {
                        retry = false;
                    } else if (r.error() == PGError::RETRY_REQUEST) {
                        LOGINFO("remove_member get RETRY_REQUEST error, will retry");
                        std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    } else {
                        ASSERT_FALSE(true);
                    }
                }
            }
        });
        // SyncPoint4 flip learner
        g_helper->sync();
        LOGINFO("about to flip learner")
        run_on_pg_leader(pg_id, [&]() {
            if (http_enabled) {
                HttpHelper http_helper("127.0.0.1", 5000 + g_helper->replica_num());
                nlohmann::json j;
                j["pg_id"] = std::to_string(pg_id);
                j["member_id"] = boost::uuids::to_string(out_member_id);
                j["learner"] = "false";
                j["commit_quorum"] = "0";
                auto r = http_helper.post("/api/v1/flip_learner", j.dump());
                ASSERT_EQ(r.code(), Pistache::Http::Code::Ok);
            } else {
                auto r = _obj_inst->pg_manager()->flip_learner_flag(pg_id, out_member_id, false, 0, 0).get();
                ASSERT_FALSE(r.hasError());
            }
        });
        // SyncPoint5 clean task
        g_helper->sync();
        LOGINFO("about to clean task")
        run_on_pg_leader(pg_id, [&]() {
            if (!http_enabled) {
                auto r = _obj_inst->pg_manager()->clean_replace_member_task(pg_id, task_id, 0, 0).get();
                ASSERT_FALSE(r.hasError());
            } else {
                HttpHelper http_helper("127.0.0.1", 5000 + g_helper->replica_num());
                nlohmann::json j;
                j["pg_id"] = std::to_string(pg_id);
                j["task_id"] = task_id;
                j["commit_quorum"] = "0";
                auto r = http_helper.del("/api/v1/pg_replacemember_task", j.dump());
                ASSERT_EQ(r.code(), Pistache::Http::Code::Ok);
            }
        });
        // wait for replication settled
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        // SyncPoint6 verify
        g_helper->sync();
        LOGINFO("verify rollback result")
        ASSERT_TRUE(verify_rollback_replace_member_result(pg_id, task_id, out_member_id, in_member_id));
    }
    // SyncPoint7
    g_helper->sync();
    LOGINFO("After rollback, wait for new member to leave pg={}", pg_id);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // Since in member is stuck in snapshot previouly, so it may not response to remove_member request, lead to being
    // force removed after timeout. In that case, after it recovers, it can not discover itself being removed because
    // is_catching_up flag is set, so we need to manually remove it.
    uuid_t group_id = get_group_id(pg_id);
    if (in_member_id == g_helper->my_replica_id()) {
        while (am_i_in_pg(pg_id)) {
            LOGINFO("New member is going to exit pg={}", pg_id);
            if (http_enabled) {
                HttpHelper http_helper("127.0.0.1", 5000 + g_helper->replica_num());
                auto resource = "/api/v1/pg?group_id=" + boost::uuids::to_string(group_id) +
                    "&replica_id=" + boost::uuids::to_string(g_helper->my_replica_id_);
                auto r = http_helper.del(resource, "");
                ASSERT_EQ(r.code(), Pistache::Http::Code::Ok);
            } else {
                _obj_inst->pg_manager()->exit_pg(group_id, g_helper->my_replica_id(), 0);
            }
            LOGINFO("New member is waiting to leave pg={}", pg_id);
        }
        // Test idempotence of exit_pg
        LOGINFO("Try to call exit_pg again pg={}", pg_id);
        auto ret = _obj_inst->pg_manager()->exit_pg(group_id, g_helper->my_replica_id(), 0);
        ASSERT_FALSE(ret.hasError());
    }
    g_helper->sync();
}

SISL_OPTION_GROUP(
    test_homeobject_repl_common,
    (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"),
    (dev_size_mb, "", "dev_size_mb", "size of each device in MB", ::cxxopts::value< uint64_t >()->default_value("2048"),
     "number"),
    (chunks_per_pg, "", "chunks_per_pg", "how many chunks a PG has", ::cxxopts::value< uint64_t >()->default_value("5"),
     "number"),
    (chunk_size, "", "chunk_size", "size of chunk in MB", ::cxxopts::value< uint64_t >()->default_value("20"),
     "number"),
    (num_threads, "", "num_threads", "number of threads", ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
    (num_devs, "", "num_devs", "number of devices to create", ::cxxopts::value< uint32_t >()->default_value("3"),
     "number"),
    (use_file, "", "use_file", "use file instead of real drive", ::cxxopts::value< bool >()->default_value("false"),
     "true or false"),
    (init_device, "", "init_device", "init real device", ::cxxopts::value< bool >()->default_value("false"),
     "true or false"),
    (replicas, "", "replicas", "Total number of replicas", ::cxxopts::value< uint8_t >()->default_value("3"), "number"),
    (spare_replicas, "", "spare_replicas", "Additional number of spare replicas not part of repldev",
     ::cxxopts::value< uint8_t >()->default_value("1"), "number"),
    (base_port, "", "base_port", "Port number of first replica", ::cxxopts::value< uint16_t >()->default_value("4000"),
     "number"),
    (replica_num, "", "replica_num", "Internal replica num (used to lauch multi process) - don't override",
     ::cxxopts::value< uint8_t >()->default_value("0"), "number"),
    (replica_dev_list, "", "replica_dev_list", "Device list for all replicas",
     ::cxxopts::value< std::vector< std::string > >(), "path [...]"),
    (qdepth, "", "qdepth", "Max outstanding operations", ::cxxopts::value< uint32_t >()->default_value("8"), "number"),
    (num_pgs, "", "num_pgs", "number of pgs", ::cxxopts::value< uint64_t >()->default_value("2"), "number"),
    (num_shards, "", "num_shards", "number of shards", ::cxxopts::value< uint64_t >()->default_value("4"), "number"),
    (num_blobs, "", "num_blobs", "number of blobs", ::cxxopts::value< uint64_t >()->default_value("20"), "number"),
    (is_restart, "", "is_restart",
     "(internal) the process is restart or the first start, only used for the first testcase",
     ::cxxopts::value< bool >()->default_value("false"), "true or false"),
    (enable_http, "", "enable_http", "enable http server or not", ::cxxopts::value< bool >()->default_value("false"),
     "true or false"));

SISL_LOGGING_INIT(homeobject)
#define test_options logging, config, homeobject, test_homeobject_repl_common
SISL_OPTIONS_ENABLE(test_options)

std::unique_ptr< test_common::HSReplTestHelper > g_helper;

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    char** orig_argv = argv;
    std::vector< std::string > args;
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, test_options);

    g_helper = std::make_unique< test_common::HSReplTestHelper >("test_homeobject_dynamic", args, orig_argv);
    // We spawn spare replica's, which is used for testing baseline resync
    // TODO: handle overflow for the sum of two uint8_t
    auto total_replicas = SISL_OPTIONS["replicas"].as< uint8_t >() + SISL_OPTIONS["spare_replicas"].as< uint8_t >();
    g_helper->setup(total_replicas);
    auto ret = RUN_ALL_TESTS();
    g_helper->teardown();
    return ret;
}
