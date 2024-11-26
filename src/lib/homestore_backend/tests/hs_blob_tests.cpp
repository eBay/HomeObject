#include "homeobj_fixture.hpp"

#include "lib/homestore_backend/index_kv.hpp"
#include "generated/resync_blob_data_generated.h"

TEST_F(HomeObjectFixture, BasicEquivalence) {
    auto shard_mgr = _obj_inst->shard_manager();
    auto pg_mgr = _obj_inst->pg_manager();
    auto blob_mgr = _obj_inst->blob_manager();
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(shard_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(pg_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(blob_mgr.get()));
}

TEST_F(HomeObjectFixture, BasicPutGetDelBlobWRestart) {
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
            LOGINFO("pg {} shard {}", i, shard.id);
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

    //  Put blob after restart to test the persistance of blob sequence number
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
                    auto g = _obj_inst->blob_manager()->del(shard_id, blob_id).get();
                    ASSERT_TRUE(g);
                    LOGINFO("delete blob shard {} blob {}", shard_id, blob_id);
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
        shared< BlobIndexTable > index_table;
        {
            std::shared_lock lock_guard(_obj_inst->_pg_lock);
            auto iter = _obj_inst->_pg_map.find(pg_id);
            ASSERT_TRUE(iter != _obj_inst->_pg_map.end());
            index_table = static_cast< HSHomeObject::HS_PG* >(iter->second.get())->index_table_;
        }
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

TEST_F(HomeObjectFixture, PGBlobIterator) {
    // uint64_t num_shards_per_pg = 3;
    // uint64_t num_blobs_per_shard = 5;
    // std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    // // pg -> next blob_id in this pg
    // std::map< pg_id_t, blob_id_t > pg_blob_id;
    //
    // pg_id_t pg_id{1};
    // create_pg(pg_id);
    // for (uint64_t i = 0; i < num_shards_per_pg; i++) {
    //     auto shard = create_shard(1, 64 * Mi);
    //     pg_shard_id_vec[1].emplace_back(shard.id);
    //     pg_blob_id[i] = 0;
    //     LOGINFO("pg {} shard {}", pg_id, shard.id);
    // }
    //
    // // Put blob for all shards in all pg's.
    // put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);
    //
    // PG* pg1;
    // {
    //     auto lg = std::shared_lock(_obj_inst->_pg_lock);
    //     auto iter = _obj_inst->_pg_map.find(pg_id);
    //     ASSERT_TRUE(iter != _obj_inst->_pg_map.end());
    //     pg1 = iter->second.get();
    // }
    //
    // auto pg1_iter =
    //     std::make_shared< homeobject::HSHomeObject::PGBlobIterator >(*_obj_inst, pg1->pg_info_.replica_set_uuid);
    // ASSERT_EQ(pg1_iter->end_of_scan(), false);
    //
    // // Verify PG shard meta data.
    // sisl::io_blob_safe meta_blob;
    // pg1_iter->create_pg_shard_snapshot_data(meta_blob);
    // ASSERT_TRUE(meta_blob.size() > 0);
    //
    // auto pg_req = GetSizePrefixedResyncPGShardInfo(meta_blob.bytes());
    // ASSERT_EQ(pg_req->pg()->pg_id(), pg1->pg_info_.id);
    // auto u1 = pg_req->pg()->replica_set_uuid();
    // auto u2 = pg1->pg_info_.replica_set_uuid;
    // ASSERT_EQ(std::string(u1->begin(), u1->end()), std::string(u2.begin(), u2.end()));
    //
    // // Verify get blobs for pg.
    // uint64_t max_num_blobs_in_batch = 3, max_batch_size_bytes = 128 * Mi;
    // std::vector< HSHomeObject::BlobInfoData > blob_data_vec;
    // while (!pg1_iter->end_of_scan()) {
    //     std::vector< HSHomeObject::BlobInfoData > vec;
    //     bool end_of_shard;
    //     auto result = pg1_iter->get_next_blobs(max_num_blobs_in_batch, max_batch_size_bytes, vec, end_of_shard);
    //     ASSERT_EQ(result, 0);
    //     for (auto& v : vec) {
    //         blob_data_vec.push_back(std::move(v));
    //     }
    // }
    //
    // ASSERT_EQ(blob_data_vec.size(), num_shards_per_pg * num_blobs_per_shard);
    // for (auto& b : blob_data_vec) {
    //     auto g = _obj_inst->blob_manager()->get(b.shard_id, b.blob_id, 0, 0).get();
    //     ASSERT_TRUE(!!g);
    //     auto result = std::move(g.value());
    //     LOGINFO("Get blob pg {} shard {} blob {} len {} data {}", 1, b.shard_id, b.blob_id, b.blob.body.size(),
    //             hex_bytes(result.body.cbytes(), 5));
    //     EXPECT_EQ(result.body.size(), b.blob.body.size());
    //     EXPECT_EQ(std::memcmp(result.body.bytes(), b.blob.body.cbytes(), result.body.size()), 0);
    //     EXPECT_EQ(result.user_key.size(), b.blob.user_key.size());
    //     EXPECT_EQ(result.user_key, b.blob.user_key);
    //     EXPECT_EQ(result.object_off, b.blob.object_off);
    // }
}

TEST_F(HomeObjectFixture, SnapshotReceiveHandler) {}
