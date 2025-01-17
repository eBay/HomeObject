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

TEST_F(HomeObjectFixture, ReplaceMember) {
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

    for (uint64_t j = 0; j < num_shards_per_pg; j++)
        create_shard(pg_id, 64 * Mi);

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

    // TODO:: if we add delete blobs case in baseline resync, we need also derive the last blob_id in this pg for spare
    // replicas
    pg_blob_id[pg_id] = 0;

    // put and verify blobs in the pg, excluding the spare replicas
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
    verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);

    // all the replicas , including the spare ones, sync at this point
    g_helper->sync();

    // step 3: replace a member
    auto out_member_id = g_helper->replica_id(num_replicas - 1);
    auto in_member_id = g_helper->replica_id(num_replicas); /*spare replica*/

    // get out_member's index_table_uuid with pg_id
    string index_table_uuid_str;
    if (out_member_id == g_helper->my_replica_id()) {
        auto iter = _obj_inst->_pg_map.find(pg_id);
        RELEASE_ASSERT(iter != _obj_inst->_pg_map.end(), "PG not found");
        auto hs_pg = static_cast< homeobject::HSHomeObject::HS_PG* >(iter->second.get());
        index_table_uuid_str = boost::uuids::to_string(hs_pg->pg_sb_->index_table_uuid);
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
#endif

    run_on_pg_leader(pg_id, [&]() {
        auto r = _obj_inst->pg_manager()
                     ->replace_member(pg_id, out_member_id, PGMember{in_member_id, "new_member", 0})
                     .get();
        ASSERT_TRUE(r);
    });

    // the new member should wait until it joins the pg and all the blobs are replicated to it
    if (in_member_id == g_helper->my_replica_id()) {
        while (!am_i_in_pg(pg_id)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            LOGINFO("new member is waiting to become a member of pg {}", pg_id);
        }

        wait_for_all(pg_shard_id_vec[pg_id].back() /*the last shard id in this pg*/,
                     num_shards_per_pg * num_blobs_per_shard - 1 /*the last blob id in this pg*/);

        sleep(5); // wait for incremental append-log requests to complete
    }

    // step 4: after completing replace member, verify the blob on all the members of this pg including the newly added
    // spare replica,
    run_if_in_pg(pg_id, [&]() {
        verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
        verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);
    });

    // step 5: Verify no pg related data in out_member
    if (out_member_id == g_helper->my_replica_id()) {
        while (am_i_in_pg(pg_id)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            LOGINFO("old member is waiting to leave pg {}", pg_id);
        }

        verify_pg_destroy(pg_id, index_table_uuid_str, pg_shard_id_vec[pg_id]);
        // since this case out_member don't have any pg, so we can check each chunk.
        for (const auto& [_, chunk] : _obj_inst->chunk_selector()->m_chunks) {
            ASSERT_EQ(chunk->m_state, ChunkState::AVAILABLE);
            ASSERT_EQ(chunk->available_blks(), chunk->get_total_blks());
        }
        LOGINFO("check no pg related data in out member successfully");
    }

    // Step 6: restart, verify the blobs again on all members, including the new spare replica, and out_member
    restart();
    run_if_in_pg(pg_id, [&]() {
        verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
        verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);
        LOGINFO("After restart, check pg related data in pg members successfully");
    });

    if (out_member_id == g_helper->my_replica_id()) {
        verify_pg_destroy(pg_id, index_table_uuid_str, pg_shard_id_vec[pg_id]);
        // since this case out_member don't have any pg, so we can check each chunk.
        for (const auto& [_, chunk] : _obj_inst->chunk_selector()->m_chunks) {
            ASSERT_EQ(chunk->m_state, ChunkState::AVAILABLE);
            ASSERT_EQ(chunk->available_blks(), chunk->get_total_blks());
        }
        LOGINFO("After restart, check no pg related data in out member successfully");
    }
}

//Restart during baseline resync and timeout
TEST_F(HomeObjectFixture, RestartFollowerDuringBaselineResyncAndTimeout) {
    RestartFollowerDuringBaselineResyncUsingSigKill(30000, 10000);
}

// Test case to restart new member during baseline resync, it will start 4 process to simulate the 4 replicas, let's say P0, P1, P2 and P3.
// P0, P1, P2 are the original members of the pg, P3 is the spare replica.
// After the replace_member happens, P3 will join the pg, and then kill itself(sigkill) to simulate the restart during baseline resync.
// As P0 is the original process who spawn the other 3 processes, so P0 will also help to spawn a new process to simulate the new member restart.
void HomeObjectFixture::RestartFollowerDuringBaselineResyncUsingSigKill(uint64_t flip_delay, uint64_t restart_interval) {
   LOGINFO("HomeObject replica={} setup completed", g_helper->replica_num());
    auto spare_num_replicas = SISL_OPTIONS["spare_replicas"].as< uint8_t >();
    ASSERT_TRUE(spare_num_replicas > 0) << "we need spare replicas for homestore backend dynamic tests";

   auto is_restart = SISL_OPTIONS["is_restart"].as< bool >();
   auto num_replicas = SISL_OPTIONS["replicas"].as< uint8_t >();
    pg_id_t pg_id{1};
    #ifdef _PRERELEASE
        //simulate delay in snapshot read data
        flip::FlipCondition cond1;
        blob_id_t blob_id = 7;
        m_fc.create_condition("blob id", flip::Operator::DONT_CARE, blob_id, &cond1);
        //This latency simulation is used to workaround the shutdown concurrency issue
        set_retval_flip("read_snapshot_load_blob_latency", flip_delay /*ms*/, 1, 100, cond1);
        //simulate delay in snapshot write data
        //set_retval_flip("write_snapshot_save_blob_latency", 30 * 1000 /*ms*/, 1, 100, cond);

    #endif
    // ====================Stage 1:  Create a pg without spare replicas and put blobs.====================

    std::unordered_set< uint8_t > excluding_replicas_in_pg;
    for (size_t i = num_replicas; i < num_replicas + spare_num_replicas; i++)
        excluding_replicas_in_pg.insert(i);

    create_pg(pg_id, 0 /* pg_leader */, excluding_replicas_in_pg);

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

    if(!is_restart) {
        for (uint64_t j = 0; j < num_shards_per_pg; j++)
            create_shard(pg_id, 64 * Mi);

        // put and verify blobs in the pg, excluding the spare replicas
        put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

        verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
        verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);

        // all the replicas , including the spare ones, sync at this point
        g_helper->sync();

        // ====================Stage 2: replace a member====================
        auto out_member_id = g_helper->replica_id(num_replicas - 1);
        auto in_member_id = g_helper->replica_id(num_replicas); /*spare replica*/

        run_on_pg_leader(pg_id, [&]() {
            auto r = _obj_inst->pg_manager()
                         ->replace_member(pg_id, out_member_id, PGMember{in_member_id, "new_member", 0})
                         .get();
            ASSERT_TRUE(r);
        });

        // ====================Stage 3: the new member will kill itself to simulate restart, then P0 will help start it ====================
        if (in_member_id == g_helper->my_replica_id()) {
            while (!am_i_in_pg(pg_id)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                LOGINFO("new member is waiting to become a member of pg {}", pg_id);
            }
            wait_for_all(pg_shard_id_vec[pg_id].front() /*the first shard id in this pg*/,
                         num_blobs_per_shard - 1 /*the last blob id in this shard*/);
            LOGINFO("the data in the first shard has been replicated to the new member");
            LOGINFO("about to kill new member")
            // SyncPoint 1(new member): kill itself.
            g_helper->sync();
            kill();
        }

        // SyncPoint 1(others): wait for the new member stop, then P0 will help start it.
        LOGINFO("waiting for new member stop")
        g_helper->sync();
       if (g_helper->replica_num() == 0) {
           //wait for kill
           std::this_thread::sleep_for(std::chrono::milliseconds(restart_interval));
           LOGINFO("going to restart new member")
           g_helper->spawn_homeobject_process(num_replicas, true);
       }
        // SyncPoint 2(others): wait for restart completed, new member will call g_helper->sync() at setup func to end up this stage implicitly.
        LOGINFO("waiting for new member start up")
        g_helper->sync();
        // SyncPoint 3: waiting for all the blobs are replicated to the new member
        g_helper->sync();
    } else {
        // new member restart
        while (!am_i_in_pg(pg_id)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            LOGINFO("new member is waiting to become a member of pg {}", pg_id);
        }
        wait_for_all(pg_shard_id_vec[pg_id].back() /*the first shard id in this pg*/,
                     num_shards_per_pg * num_blobs_per_shard - 1 /*the last blob id in this shard*/);
        run_if_in_pg(pg_id, [&]() {
            verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
            verify_obj_count(1, num_shards_per_pg, num_blobs_per_shard, false);
        });
        // SyncPoint 3(new member): replication done, notify others.
        g_helper->sync();
    }
}

SISL_OPTION_GROUP(
    test_homeobject_repl_common,
    (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"),
    (dev_size_mb, "", "dev_size_mb", "size of each device in MB", ::cxxopts::value< uint64_t >()->default_value("2048"),
     "number"),
    (pg_size, "", "pg_size", "default size of pg in MB", ::cxxopts::value< uint64_t >()->default_value("100"),
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
    (is_restart, "", "is_restart", "the process is restart or the first start", ::cxxopts::value< bool >()->
        default_value("false"), "true or false"));

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