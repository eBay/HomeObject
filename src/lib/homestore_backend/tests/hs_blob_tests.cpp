#include "homeobj_fixture.hpp"

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

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    // for (uint64_t i = 1; i <= num_pgs; i++) {
    //     r_cast< HSHomeObject* >(_obj_inst.get())->print_btree_index(i);
    // }

    LOGINFO("Flushing CP.");
    trigger_cp(true /* wait */);

    // Restart homeobject
    restart();

    // Verify all get blobs after restart
    verify_get_blob(blob_map);

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    // Put blob after restart to test the persistance of blob sequence number
    put_blob(blob_map, pg_shard_id_vec, num_blobs_per_shard, max_blob_size);

    // Verify all get blobs with random offset and length.
    verify_get_blob(blob_map, true /* use_random_offset */);

    // Verify the stats after put blobs after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, false /* deleted */);

    // Delete all blobs
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->del(shard_id, blob_id).get();
        ASSERT_TRUE(g);
        LOGINFO("delete blob shard {} blob {}", shard_id, blob_id);
    }

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);

    // Delete again should have no errors.
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->del(shard_id, blob_id).get();
        ASSERT_TRUE(g);
        LOGINFO("delete blob shard {} blob {}", shard_id, blob_id);
    }

    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);

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

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);
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
