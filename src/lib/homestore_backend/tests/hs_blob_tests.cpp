#include "homeobj_fixture.hpp"

#include "lib/homestore_backend/index_kv.hpp"
#include <homestore/replication_service.hpp>

TEST_F(HomeObjectFixture, BasicEquivalence) {
    auto shard_mgr = _obj_inst->shard_manager();
    auto pg_mgr = _obj_inst->pg_manager();
    auto blob_mgr = _obj_inst->blob_manager();
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(shard_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(pg_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(blob_mgr.get()));
}
TEST_F(HomeObjectFixture, BasicPutGetDelBlobWithRestart) {
    // test recovery with pristine state firstly
    restart();

    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;

    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;

    // pg -> next blob_id in this pg
    std::map< pg_id_t, blob_id_t > pg_blob_id;

    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        pg_blob_id[i] = 0;
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = create_shard(i, 64 * Mi);
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    // Restart homeobject
    restart();

    // Verify all get blobs after restart
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    //  Put blob after restart to test the persistence of blob sequence number
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs with random offset and length.
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard, true /* use_random_offset */);

    // Verify the stats after put blobs after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, false /* deleted */);

    // Delete all blobs
    del_all_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);

    // Delete again should have no errors.
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        // for each pg, blob_id start for 0
        blob_id_t blob_id{0};

        run_on_pg_leader(pg_id, [&]() {
            for (const auto& shard_id : shard_vec) {
                for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                    auto tid = generateRandomTraceId();
                    auto g = _obj_inst->blob_manager()->del(shard_id, blob_id, tid).get();
                    ASSERT_TRUE(g);
                    LOGINFO("delete blob shard {} blob {}, trace_id={}", shard_id, blob_id, tid);
                    blob_id++;
                }
            }
        });
    }
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);

    // After delete all blobs, get should fail
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        blob_id_t blob_id{0};
        for (; blob_id != pg_blob_id[pg_id];) {
            for (const auto& shard_id : shard_vec) {
                for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                    auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
                    ASSERT_TRUE(!g);
                    blob_id++;
                }
            }
        }
    }

    // all the deleted blobs should be tombstone in index table
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        auto hs_pg = _obj_inst->get_hs_pg(pg_id);
        ASSERT_TRUE(hs_pg != nullptr);
        auto index_table = hs_pg->index_table_;
        blob_id_t blob_id{0};
        for (; blob_id != pg_blob_id[pg_id];) {
            for (const auto& shard_id : shard_vec) {
                auto g = _obj_inst->get_blob_from_index_table(index_table, shard_id, blob_id);
                ASSERT_FALSE(!!g);
                EXPECT_EQ(BlobErrorCode::UNKNOWN_BLOB, g.error().getCode());
                blob_id++;
            }
        }
    }

    // Restart homeobject
    restart();

    // After restart, for all deleted blobs, get should fail
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        blob_id_t blob_id{0};
        for (; blob_id != pg_blob_id[pg_id];) {
            for (const auto& shard_id : shard_vec) {
                for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                    auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
                    ASSERT_TRUE(!g);
                    blob_id++;
                }
            }
        }
    }

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);
}

TEST_F(HomeObjectFixture, BasicPutGetDelBlobOnExistPGWithDiskLost) {
    // test recovery with pristine state firstly
    restart();

    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;

    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;

    // pg -> next blob_id in this pg
    std::map< pg_id_t, blob_id_t > pg_blob_id;

    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        pg_blob_id[i] = 0;
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = create_shard(i, 64 * Mi);
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    /*
     * restart with one disk lost, exist pg should support put, get, delete.
     */
    restart(0, 0, 1);
    // current num_pgs = 2, pg{2} is diskdown
    LOGI("restart with one disk lost");
    std::set< pg_id_t > lost_pg{2};
    std::map< pg_id_t, std::vector< shard_id_t > > exist_pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > exist_pg_blob_id;

    for (const auto& pair : pg_shard_id_vec) {
        if (!lost_pg.contains(pair.first)) {
            exist_pg_shard_id_vec.emplace(pair);
            exist_pg_blob_id[pair.first] = pg_blob_id[pair.first];
        }
    }
    const uint64_t exist_num_pgs = num_pgs - 1;
    // Verify all get blobs after restart
    verify_get_blob(exist_pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats after restart
    verify_obj_count(exist_num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    //  Put blob after restart to test the persistence of blob sequence number
    put_blobs(exist_pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs with random offset and length.
    verify_get_blob(exist_pg_shard_id_vec, num_blobs_per_shard, true /* use_random_offset */);

    // Verify the stats after put blobs after restart
    verify_obj_count(exist_num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, false /* deleted */);

    // Delete all blobs
    del_all_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify the stats after restart
    verify_obj_count(exist_num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);
}

TEST_F(HomeObjectFixture, BasicPutGetDelBlobWithDiskBack) {
    // test recovery with pristine state firstly
    restart();

    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;

    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;

    // pg -> next blob_id in this pg
    std::map< pg_id_t, blob_id_t > pg_blob_id;

    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        pg_blob_id[i] = 0;
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = create_shard(i, 64 * Mi);
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    // Restart with disk lost
    restart(0, 0, 1);
    // Restart with disk back
    restart();

    // Verify all get blobs after restart
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    //  Put blob after restart to test the persistence of blob sequence number
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs with random offset and length.
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard, true /* use_random_offset */);

    // Verify the stats after put blobs after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, false /* deleted */);

    // Delete all blobs
    del_all_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);

    // Delete again should have no errors.
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        // for each pg, blob_id start for 0
        blob_id_t blob_id{0};

        run_on_pg_leader(pg_id, [&]() {
            for (const auto& shard_id : shard_vec) {
                for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                    auto tid = generateRandomTraceId();
                    auto g = _obj_inst->blob_manager()->del(shard_id, blob_id, tid).get();
                    ASSERT_TRUE(g);
                    LOGINFO("delete blob shard {} blob {}, trace_id={}", shard_id, blob_id, tid);
                    blob_id++;
                }
            }
        });
    }
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);

    // After delete all blobs, get should fail
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        blob_id_t blob_id{0};
        for (; blob_id != pg_blob_id[pg_id];) {
            for (const auto& shard_id : shard_vec) {
                for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                    auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
                    ASSERT_TRUE(!g);
                    blob_id++;
                }
            }
        }
    }

    // all the deleted blobs should be tombstone in index table
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        auto hs_pg = _obj_inst->get_hs_pg(pg_id);
        ASSERT_TRUE(hs_pg != nullptr);
        auto index_table = hs_pg->index_table_;
        blob_id_t blob_id{0};
        for (; blob_id != pg_blob_id[pg_id];) {
            for (const auto& shard_id : shard_vec) {
                auto g = _obj_inst->get_blob_from_index_table(index_table, shard_id, blob_id);
                ASSERT_FALSE(!!g);
                EXPECT_EQ(BlobErrorCode::UNKNOWN_BLOB, g.error().getCode());
                blob_id++;
            }
        }
    }

    // Restart with disk lost
    restart(0, 0, 1);
    // Restart with disk back
    restart();

    // After restart, for all deleted blobs, get should fail
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        blob_id_t blob_id{0};
        for (; blob_id != pg_blob_id[pg_id];) {
            for (const auto& shard_id : shard_vec) {
                for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                    auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
                    ASSERT_TRUE(!g);
                    blob_id++;
                }
            }
        }
    }

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);
}

TEST_F(HomeObjectFixture, BasicPutGetDelOnAllPGWithDiskLost) {
    // test recovery with pristine state firstly
    restart();

    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;

    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;

    // pg -> next blob_id in this pg
    std::map< pg_id_t, blob_id_t > pg_blob_id;

    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        pg_blob_id[i] = 0;
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = create_shard(i, 64 * Mi);
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    // degrade_pg_id = 2, leader's replica_num=0, chose the replica_num=2 membet lost one disk.
    pg_id_t degrade_pg_id = 2;
    const uint8_t lost_disk_replica_num = 2;

    g_helper->sync();
    if (g_helper->replica_num() == lost_disk_replica_num) {
        // reset with one member lost one disk
        restart(0, 0, 1);

        // send read request to disk lost member
        blob_id_t current_blob_id{0};
        uint32_t off = 0, len = 0;
        for (const auto& shard_id : pg_shard_id_vec[degrade_pg_id]) {
            for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                auto tid = generateRandomTraceId();
                LOGI("going to verify blob pg={} shard {} blob {} trace_id={}", degrade_pg_id, shard_id,
                     current_blob_id, tid);
                auto blob = build_blob(current_blob_id);
                len = blob.body.size();
                auto g = _obj_inst->blob_manager()->get(shard_id, current_blob_id, off, len, tid).get();
                ASSERT_TRUE(g.hasError())
                    << "degraded pg on error member should return get blob fail, shard_id " << shard_id << " blob_id "
                    << current_blob_id << " replica number " << g_helper->replica_num();
                current_blob_id++;
            }
        }

        // send put/del blob request to disk lost member
        auto put_shard_id = pg_shard_id_vec[degrade_pg_id].back();
        auto put_blob = build_blob(current_blob_id);
        auto tid = generateRandomTraceId();

        LOGDEBUG("Put blob pg={} shard {} blob {} size {} data {} trace_id={} on disk lost member", degrade_pg_id,
                 put_shard_id, current_blob_id, put_blob.body.size(),
                 hex_bytes(put_blob.body.cbytes(), std::min(10u, put_blob.body.size())), tid);

        auto b = _obj_inst->blob_manager()->put(put_shard_id, std::move(put_blob), tid).get();
        ASSERT_TRUE(b.hasError()) << "degraded pg on error member should return put blob fail, shard_id "
                                  << put_shard_id << " blob_id " << current_blob_id << " replica number "
                                  << g_helper->replica_num();

        blob_id_t delete_blob_id{0};
        auto delete_shard_id = pg_shard_id_vec[degrade_pg_id].back();
        auto delete_blob = build_blob(delete_blob_id);
        tid = generateRandomTraceId();
        LOGDEBUG("Delete blob pg={} shard {} blob {} size {} data {} trace_id={} on disk lost member", degrade_pg_id,
                 delete_shard_id, delete_blob_id, delete_blob.body.size(),
                 hex_bytes(delete_blob.body.cbytes(), std::min(10u, delete_blob.body.size())), tid);

        auto d = _obj_inst->blob_manager()->del(delete_shard_id, delete_blob_id, tid).get();
        ASSERT_TRUE(d.hasError()) << "degraded pg on error member should return delete blob fail, shard_id "
                                  << delete_shard_id << " blob_id " << delete_blob_id << " replica number "
                                  << g_helper->replica_num();
    } else {
        restart();
        sleep(10);
    }
    g_helper->sync();
}

TEST_F(HomeObjectFixture, DeleteNonExistBlob) {
    g_helper->sync();
    // create a pg with one shard
    const auto pg_id = 1;
    create_pg(pg_id);
    const auto shard_id = create_shard(1, 64 * Mi).id;
    verify_obj_count(1, 1, 0, false /* deleted */);

    // do not put any blob to exercise deleting a non-exist blob, everything should goes well
    del_blob(1, shard_id, 1);
}

#ifdef _PRERELEASE
TEST_F(HomeObjectFixture, BasicPutGetBlobWithPushDataDisabled) {
    // disable leader push data. As a result, followers have to fetch data to exercise the fetch_data implementation of
    // statemachine
    set_basic_flip("disable_leader_push_data", std::numeric_limits< int >::max());

    // test recovery with pristine state firstly
    restart();

    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;

    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;

    // pg -> next blob_id in this pg
    std::map< pg_id_t, blob_id_t > pg_blob_id;

    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        pg_blob_id[i] = 0;
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = create_shard(i, 64 * Mi);
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    remove_flip("disable_leader_push_data");
}

// TODO:: add a test for no_space_left without flip.

#endif

// TODO:: add more test cases to verify the push data disabled scenario after we have gc
