#include "homeobj_fixture.hpp"

TEST_F(HomeObjectFixture, CreateMultiShards) {
    pg_id_t pg_id{1};
    create_pg(pg_id);
    auto _shard_1 = create_shard(pg_id, 64 * Mi, "shard meta");
    auto _shard_2 = create_shard(pg_id, 64 * Mi, "shard meta");

    auto chunk_num_1 = _obj_inst->get_shard_p_chunk_id(_shard_1.id);
    ASSERT_TRUE(chunk_num_1.has_value());

    auto chunk_num_2 = _obj_inst->get_shard_p_chunk_id(_shard_2.id);
    ASSERT_TRUE(chunk_num_2.has_value());

    // check if both chunk is on the same pg and pdev;
    auto chunks = _obj_inst->chunk_selector()->m_chunks;
    ASSERT_TRUE(chunks.find(chunk_num_1.value()) != chunks.end());
    ASSERT_TRUE(chunks.find(chunk_num_2.value()) != chunks.end());
    auto chunk_1 = chunks[chunk_num_1.value()];
    auto chunk_2 = chunks[chunk_num_2.value()];
    ASSERT_TRUE(chunk_1->m_pg_id.has_value());
    ASSERT_TRUE(chunk_2->m_pg_id.has_value());
    ASSERT_TRUE(chunk_1->m_pg_id.value() == chunk_2->m_pg_id.value());
    ASSERT_TRUE(chunk_1->get_pdev_id() == chunk_2->get_pdev_id());
}

TEST_F(HomeObjectFixture, CreateMultiShardsOnMultiPG) {
    std::vector< homeobject::pg_id_t > pgs;

    for (pg_id_t pg{1}; pg < 4; pg++) {
        create_pg(pg);
        pgs.push_back(pg);
    }

    for (const auto pg : pgs) {
        auto shard_info = create_shard(pg, Mi, "shard meta");
        auto p_chunk_ID1 = _obj_inst->get_shard_p_chunk_id(shard_info.id);
        auto v_chunk_ID1 = _obj_inst->get_shard_v_chunk_id(shard_info.id);
        ASSERT_TRUE(p_chunk_ID1.has_value());
        ASSERT_TRUE(v_chunk_ID1.has_value());

        // create another shard again.
        shard_info = create_shard(pg, Mi, "shard meta");
        auto p_chunk_ID2 = _obj_inst->get_shard_p_chunk_id(shard_info.id);
        auto v_chunk_ID2 = _obj_inst->get_shard_v_chunk_id(shard_info.id);
        ASSERT_TRUE(p_chunk_ID2.has_value());
        ASSERT_TRUE(v_chunk_ID2.has_value());

        // v_chunk_id should not same
        ASSERT_NE(v_chunk_ID1.value(), v_chunk_ID2.value());
        // check if both chunk is on the same pg and pdev;
        auto chunks = _obj_inst->chunk_selector()->m_chunks;
        ASSERT_TRUE(chunks.find(p_chunk_ID1.value()) != chunks.end());
        ASSERT_TRUE(chunks.find(p_chunk_ID2.value()) != chunks.end());
        auto chunk_1 = chunks[p_chunk_ID1.value()];
        auto chunk_2 = chunks[p_chunk_ID2.value()];
        ASSERT_TRUE(chunk_1->m_pg_id.has_value());
        ASSERT_TRUE(chunk_2->m_pg_id.has_value());
        ASSERT_EQ(chunk_1->m_pg_id.value(), chunk_2->m_pg_id.value());
        ASSERT_EQ(chunk_1->get_pdev_id(), chunk_2->get_pdev_id());
    }
}

TEST_F(HomeObjectFixture, SealShard) {
    pg_id_t pg_id{1};
    create_pg(pg_id);
    auto shard_info = create_shard(pg_id, 64 * Mi, "shard meta");
    ASSERT_EQ(ShardInfo::State::OPEN, shard_info.state);

    // seal the shard
    shard_info = seal_shard(shard_info.id);
    ASSERT_EQ(ShardInfo::State::SEALED, shard_info.state);

    // seal the shard again
    shard_info = seal_shard(shard_info.id);
    ASSERT_EQ(ShardInfo::State::SEALED, shard_info.state);

    // create shard until no space left, we have 5 chunks in one pg.
    for (auto i = 0; i < 5; i++) {
        shard_info = create_shard(pg_id, 64 * Mi, "shard meta");
        ASSERT_EQ(ShardInfo::State::OPEN, shard_info.state);
    }

    {
        g_helper->sync();

        // expect to create shard failed
        run_on_pg_leader(pg_id, [&]() {
            auto s = _obj_inst->shard_manager()->create_shard(pg_id, 64 * Mi, "shard meta").get();
            ASSERT_TRUE(s.hasError());
            ASSERT_EQ(ShardErrorCode::NO_SPACE_LEFT, s.error().getCode());
        });

        auto start_time = std::chrono::steady_clock::now();
        bool res = true;

        while (g_helper->get_uint64_id() == INVALID_UINT64_ID) {
            auto current_time = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast< std::chrono::seconds >(current_time - start_time).count();
            if (duration >= 20) {
                LOGINFO("Failed to create shard at pg={}", pg_id);
                res = false;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        ASSERT_FALSE(res);
    }

    shard_info = seal_shard(shard_info.id);
    ASSERT_EQ(ShardInfo::State::SEALED, shard_info.state);

    shard_info = create_shard(pg_id, 64 * Mi, "shard meta");
    ASSERT_EQ(ShardInfo::State::OPEN, shard_info.state);

    shard_info = seal_shard(shard_info.id);
    ASSERT_EQ(ShardInfo::State::SEALED, shard_info.state);
}

TEST_F(HomeObjectFixture, ShardManagerRecovery) {
    pg_id_t pg_id{1};
    create_pg(pg_id);

    // create one shard;
    auto shard_info = create_shard(pg_id, Mi, "shard meta");;
    auto shard_id = shard_info.id;
    EXPECT_EQ(ShardInfo::State::OPEN, shard_info.state);
    EXPECT_EQ(Mi, shard_info.total_capacity_bytes);
    EXPECT_EQ(Mi, shard_info.available_capacity_bytes);
    EXPECT_EQ(pg_id, shard_info.placement_group);

    // restart homeobject and check if pg/shard info will be recovered.
    restart();

    // check PG after recovery.
    EXPECT_TRUE(_obj_inst->_pg_map.size() == 1);
    auto pg_result = _obj_inst->get_hs_pg(pg_id);
    EXPECT_TRUE(pg_result != nullptr);
    EXPECT_EQ(1, pg_result->shards_.size());
    // verify the sequence number is correct after recovery.
    EXPECT_EQ(1, pg_result->shard_sequence_num_);
    // check recovered shard state.
    auto hs_shard = d_cast< homeobject::HSHomeObject::HS_Shard* >(pg_result->shards_.front().get());
    auto& recovered_shard_info = hs_shard->info;
    verify_hs_shard(recovered_shard_info, shard_info);

    // seal the shard when shard is recovery
    shard_info = seal_shard(shard_id);
    EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);

    // restart again to verify the shards has expected states.
    restart();

    auto s = _obj_inst->shard_manager()->get_shard(shard_id).get();
    ASSERT_TRUE(!!s);

    EXPECT_EQ(ShardInfo::State::SEALED, s.value().state);
    pg_result = _obj_inst->get_hs_pg(pg_id);
    EXPECT_TRUE(pg_result != nullptr);
    // verify the sequence number is correct after recovery.

    EXPECT_EQ(1, pg_result->shard_sequence_num_);

    // re-create new shards on this pg works too even homeobject is restarted twice.
    auto new_shard_info = create_shard(pg_id, Mi, "shard meta");;
    EXPECT_NE(shard_id, new_shard_info.id);

    EXPECT_EQ(ShardInfo::State::OPEN, new_shard_info.state);
    EXPECT_EQ(2, pg_result->shard_sequence_num_);
}

TEST_F(HomeObjectFixture, SealedShardRecovery) {
    // test recovery with pristine state firstly
    restart();

    pg_id_t pg_id{1};
    create_pg(pg_id);

    // create one shard and seal it.
    auto shard_info = create_shard(pg_id, Mi, "shard meta");;
    auto shard_id = shard_info.id;
    shard_info = seal_shard(shard_id);
    EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);

    // check the shard info from ShardManager to make sure on_commit() is successfully.
    auto pg_result = _obj_inst->get_hs_pg(pg_id);
    EXPECT_TRUE(pg_result != nullptr);
    EXPECT_EQ(1, pg_result->shards_.size());
    auto hs_shard = d_cast< homeobject::HSHomeObject::HS_Shard* >(pg_result->shards_.front().get());
    EXPECT_EQ(ShardInfo::State::SEALED, hs_shard->info.state);

    // release the homeobject and homestore will be shutdown automatically.
    LOGI("restart home_object");
    restart();

    // re-create the homeobject and pg infos and shard infos will be recover automatically.
    EXPECT_TRUE(_obj_inst->_pg_map.size() == 1);
    // check shard internal state;
    pg_result = _obj_inst->get_hs_pg(pg_id);
    EXPECT_TRUE(pg_result != nullptr);
    EXPECT_EQ(1, pg_result->shards_.size());
    hs_shard = d_cast< homeobject::HSHomeObject::HS_Shard* >(pg_result->shards_.front().get());
    auto& recovered_shard_info = hs_shard->info;
    verify_hs_shard(recovered_shard_info, shard_info);
}

TEST_F(HomeObjectFixture, SealShardWithRestart) {
    // Create a pg, shard, put blob should succeed, seal and put blob again should fail.
    // Recover and put blob again should fail.
    pg_id_t pg_id{1};
    create_pg(pg_id);

    auto shard_info = create_shard(pg_id, 64 * Mi, "shard meta");
    auto shard_id = shard_info.id;
    auto s = _obj_inst->shard_manager()->get_shard(shard_id).get();
    ASSERT_TRUE(!!s);

    LOGINFO("Got shard {}", shard_id);
    shard_info = s.value();
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::OPEN);
    put_blob(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul});

    shard_info = seal_shard(shard_id);
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::SEALED);
    LOGINFO("Sealed shard {}", shard_id);

    run_on_pg_leader(pg_id, [this, shard_id]() {
        auto b = _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
        ASSERT_TRUE(!b);
        ASSERT_EQ(b.error().getCode(), BlobErrorCode::SEALED_SHARD);
        LOGINFO("Put blob {}", b.error());
    });

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

    run_on_pg_leader(pg_id, [this, shard_id]() {
        auto b = _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
        ASSERT_TRUE(!b);
        ASSERT_EQ(b.error().getCode(), BlobErrorCode::SEALED_SHARD);
        LOGINFO("Put blob {}", b.error());
    });
}

TEST_F(HomeObjectFixture, CreateShardOnDiskLostMemeber) {
    pg_id_t pg_id{1};
    pg_id_t degrade_pg_id{2};
    create_pg(pg_id);
    create_pg(degrade_pg_id);
    std::map< pg_id_t, shard_id_t > pg_shard_id_map;
    for (int i = 1; i <= 2; i++) {
        auto shard_info = create_shard(i, 64 * Mi, "shard meta");
        ASSERT_EQ(ShardInfo::State::OPEN, shard_info.state);
        pg_shard_id_map[i] = shard_info.id;
        LOGINFO("pg={} shard {}", i, shard_info.id);
    }
    /*
     * restart with one member lost one disk, create or seal shard request should be refused on disk lost member
     */
    g_helper->sync();
    const uint8_t lost_disk_replica_num = 2;
    if (g_helper->replica_num() == lost_disk_replica_num) {
        // reset with one member lost one disk
        restart(0, 0, 1);

        auto tid = generateRandomTraceId();
        auto s = _obj_inst->shard_manager()->seal_shard(pg_shard_id_map[degrade_pg_id], tid).get();
        ASSERT_TRUE(s.hasError()) << "degraded pg on error member should return seal shard fail, pg_id "
                                  << degrade_pg_id << "shard_id " << pg_shard_id_map[degrade_pg_id]
                                  << " replica number " << g_helper->replica_num();

        tid = generateRandomTraceId();
        s = _obj_inst->shard_manager()->create_shard(degrade_pg_id, 64 * Mi, "shard meta", tid).get();
        ASSERT_TRUE(s.hasError()) << "degraded pg on error member should return create shard fail, pg_id "
                                  << degrade_pg_id << " replica number " << g_helper->replica_num();
    } else {
        restart();
        sleep(10);
    }
    g_helper->sync();
}

TEST_F(HomeObjectFixture, ShardVersionMigrationRecovery) {
    // Test migration with mixed versions (v1 and v2) to simulate partial migration or interrupted upgrade
    // Also tests the actual bug scenario where v1 shards had incorrect type field
    pg_id_t pg_id{1};
    create_pg(pg_id);

    // Create multiple shards with current version
    LOGINFO("Creating 5 test shards...");
    std::vector< ShardInfo > shard_infos;
    std::vector< shard_id_t > shard_ids;
    for (int i = 0; i < 5; ++i) {
        auto shard_info = create_shard(pg_id, Mi, fmt::format("test_migration_{}", i));
        shard_infos.push_back(shard_info);
        shard_ids.push_back(shard_info.id);
        EXPECT_EQ(ShardInfo::State::OPEN, shard_info.state);
    }

    // Get the PG and verify all shards are at current version
    auto pg_result = _obj_inst->get_hs_pg(pg_id);
    EXPECT_TRUE(pg_result != nullptr);
    EXPECT_EQ(5, pg_result->shards_.size());

    // Save original data for all shards - use map keyed by shard_id since recovery order is not guaranteed
    struct ShardOriginalData {
        ShardInfo info;
        homestore::chunk_num_t p_chunk_id;
        homestore::chunk_num_t v_chunk_id;
        bool should_be_v1; // Track which shards should be created as v1
    };
    std::unordered_map< shard_id_t, ShardOriginalData > original_data_map;

    // Mark which shards should be v1 vs v2 (using alternating pattern for variety)
    // shard_ids[0], [2], [4] -> v1 (needs migration)
    // shard_ids[1], [3] -> v2 (already migrated)
    std::set v1_shard_ids = {shard_ids[0], shard_ids[2], shard_ids[4]};

    for (auto& shard : pg_result->shards_) {
        auto hs_shard = d_cast< HSHomeObject::HS_Shard* >(shard.get());
        EXPECT_EQ(0x02, hs_shard->sb_->version);
        EXPECT_EQ(0x02, hs_shard->sb_->sb_version);

        auto shard_id = hs_shard->sb_->info.id;
        original_data_map[shard_id] = {hs_shard->sb_->info, hs_shard->sb_->p_chunk_id, hs_shard->sb_->v_chunk_id,
                                       v1_shard_ids.contains(shard_id)};

        // Destroy all current superblks
        hs_shard->sb_.destroy();
    }

    // Simulate partial migration: create shards with mixed versions
    LOGINFO("Creating mixed version shards (3 v1 shards, 2 v2 shards)...");

    for (const auto& [shard_id, orig_data] : original_data_map) {
        if (orig_data.should_be_v1) {
            // Create v1 shard (needs migration)
            homestore::superblk< HSHomeObject::v1_shard_info_superblk > old_sb("ShardManager");
            old_sb.create(sizeof(HSHomeObject::v1_shard_info_superblk));
            old_sb->magic = HSHomeObject::DataHeader::data_header_magic;
            old_sb->version = 0x01; // Old version
            // Simulate the actual bug: first v1 shard has incorrect type field set to BLOB_INFO
            if (shard_id == shard_ids[0]) {
                old_sb->type = HSHomeObject::DataHeader::data_type_t::BLOB_INFO; // Bug scenario
                LOGINFO("Created v1 shard {} with BLOB_INFO type (bug scenario)", shard_id);
            } else {
                LOGINFO("Created v1 shard {}", shard_id);
            }
            // Convert v2 ShardInfo to v1_ShardInfo (v1 doesn't have meta field)
            old_sb->info.id = orig_data.info.id;
            old_sb->info.placement_group = orig_data.info.placement_group;
            old_sb->info.state = orig_data.info.state;
            old_sb->info.lsn = orig_data.info.lsn;
            old_sb->info.created_time = orig_data.info.created_time;
            old_sb->info.last_modified_time = orig_data.info.last_modified_time;
            old_sb->info.available_capacity_bytes = orig_data.info.available_capacity_bytes;
            old_sb->info.total_capacity_bytes = orig_data.info.total_capacity_bytes;
            old_sb->info.current_leader = orig_data.info.current_leader;
            // Note: v1 doesn't have meta field, so we don't copy it
            old_sb->p_chunk_id = orig_data.p_chunk_id;
            old_sb->v_chunk_id = orig_data.v_chunk_id;
            old_sb.write();
        } else {
            // Create v2 shard (already migrated)
            homestore::superblk< HSHomeObject::shard_info_superblk > new_sb("ShardManager");
            new_sb.create(sizeof(HSHomeObject::shard_info_superblk));
            new_sb->magic = HSHomeObject::DataHeader::data_header_magic;
            new_sb->version = 0x02; // New version
            new_sb->type = HSHomeObject::DataHeader::data_type_t::SHARD_INFO;
            new_sb->sb_version = 0x02;
            new_sb->info = orig_data.info;
            new_sb->p_chunk_id = orig_data.p_chunk_id;
            new_sb->v_chunk_id = orig_data.v_chunk_id;
            new_sb.write();
            LOGINFO("Created v2 shard {}", shard_id);
        }
    }

    auto old_size = sizeof(HSHomeObject::v1_shard_info_superblk);
    auto new_size = sizeof(HSHomeObject::shard_info_superblk);
    LOGINFO("Setup complete - old size={}, new size={}", old_size, new_size);

    // Restart homeobject - this should migrate only v1 shards
    LOGINFO("Restarting homeobject to trigger migration...");
    restart();

    // Verify all shards are now at v2
    pg_result = _obj_inst->get_hs_pg(pg_id);
    EXPECT_TRUE(pg_result != nullptr);
    EXPECT_EQ(5, pg_result->shards_.size());

    LOGINFO("Verifying all shards after migration...");
    for (auto& shard : pg_result->shards_) {
        auto hs_shard = d_cast< HSHomeObject::HS_Shard* >(shard.get());
        auto shard_id = hs_shard->sb_->info.id;

        // Look up the original data for this shard
        auto it = original_data_map.find(shard_id);
        ASSERT_NE(it, original_data_map.end()) << "Shard " << shard_id << " not found in original data";
        const auto& orig_data = it->second;

        // All shards should now be at v2
        EXPECT_EQ(0x02, hs_shard->sb_->version) << "Shard " << shard_id << " version should be 0x02";
        EXPECT_EQ(0x02, hs_shard->sb_->sb_version) << "Shard " << shard_id << " sb_version should be 0x02";

        // Verify all shard data was preserved
        EXPECT_EQ(HSHomeObject::DataHeader::data_header_magic, hs_shard->sb_->magic);
        EXPECT_EQ(HSHomeObject::DataHeader::data_type_t::SHARD_INFO, hs_shard->sb_->type);
        EXPECT_EQ(orig_data.info.id, hs_shard->sb_->info.id);
        EXPECT_EQ(orig_data.info.placement_group, hs_shard->sb_->info.placement_group);
        EXPECT_EQ(orig_data.info.state, hs_shard->sb_->info.state);
        EXPECT_EQ(orig_data.p_chunk_id, hs_shard->sb_->p_chunk_id);
        EXPECT_EQ(orig_data.v_chunk_id, hs_shard->sb_->v_chunk_id);

        LOGINFO("Shard {} verified successfully (was {})", shard_id, orig_data.should_be_v1 ? "v1" : "v2");
    }

    // Verify all shards are still functional
    LOGINFO("Verifying shard functionality...");
    for (size_t i = 0; i < shard_ids.size(); ++i) {
        auto s = _obj_inst->shard_manager()->get_shard(shard_ids[i]).get();
        ASSERT_TRUE(!!s) << "Shard " << i << " should be accessible";
        EXPECT_EQ(shard_ids[i], s.value().id);
        EXPECT_EQ(ShardInfo::State::OPEN, s.value().state);
    }

    // Seal some v1 shards to verify they still work after migration
    LOGINFO("Sealing two v1 shards to verify post-migration functionality...");
    auto sealed_shard_0 = seal_shard(shard_ids[0]);
    EXPECT_EQ(ShardInfo::State::SEALED, sealed_shard_0.state);

    auto sealed_shard_2 = seal_shard(shard_ids[2]);
    EXPECT_EQ(ShardInfo::State::SEALED, sealed_shard_2.state);

    // Track which shards were sealed for later verification
    std::set sealed_shard_ids = {shard_ids[0], shard_ids[2]};

    LOGINFO("Mixed version migration test completed - 3 shards migrated from v1, 2 shards already at v2");

    // Restart again to verify the migration was persisted to disk
    LOGINFO("Second restart to verify persistence...");
    restart();

    // Verify all migrated versions persist after restart
    pg_result = _obj_inst->get_hs_pg(pg_id);
    EXPECT_TRUE(pg_result != nullptr);
    EXPECT_EQ(5, pg_result->shards_.size());

    LOGINFO("Verifying all shards after second restart...");
    for (auto& shard : pg_result->shards_) {
        auto hs_shard = d_cast< HSHomeObject::HS_Shard* >(shard.get());
        auto shard_id = hs_shard->sb_->info.id;

        EXPECT_EQ(0x02, hs_shard->sb_->version) << "Shard " << shard_id << " should still be v2 after restart";
        EXPECT_EQ(0x02, hs_shard->sb_->sb_version) << "Shard " << shard_id << " sb_version should still be 0x02";

        // Verify sealed shards remain sealed
        if (sealed_shard_ids.contains(shard_id)) {
            EXPECT_EQ(ShardInfo::State::SEALED, hs_shard->sb_->info.state)
                << "Shard " << shard_id << " should remain sealed";
        } else {
            EXPECT_EQ(ShardInfo::State::OPEN, hs_shard->sb_->info.state)
                << "Shard " << shard_id << " should remain open";
        }
    }

    LOGINFO("Verified migration persisted to disk - all {} shards remain at v2 after second restart",
            pg_result->shards_.size());
}
