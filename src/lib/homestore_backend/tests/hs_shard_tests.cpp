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
