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
    verify_obj_count(1, num_blobs_per_shard, num_shards_per_pg, false);

    // all the replicas , including the spare ones, sync at this point
    g_helper->sync();

    // step 3: replace a member
    auto out_member_id = g_helper->replica_id(num_replicas - 1);
    auto in_member_id = g_helper->replica_id(num_replicas); /*spare replica*/

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
    }

    // step 4: after completing replace member, verify the blob on all the members of this pg including the newly added
    // spare replica,
    run_if_in_pg(pg_id, [&]() {
        verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);
        verify_obj_count(1, num_blobs_per_shard, num_shards_per_pg, false);
    });
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
    (num_blobs, "", "num_blobs", "number of blobs", ::cxxopts::value< uint64_t >()->default_value("20"), "number"));

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