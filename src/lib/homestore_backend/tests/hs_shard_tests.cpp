#include "homeobj_fixture.hpp"

TEST_F(HomeObjectFixture, CreateMultiShards) {
    pg_id_t pg_id{1};
    create_pg(pg_id);
    auto _shard_1 = create_shard(pg_id, 64 * Mi);
    auto _shard_2 = create_shard(pg_id, 64 * Mi);

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
        auto shard_info = create_shard(pg, Mi);
        auto chunk_num_1 = _obj_inst->get_shard_p_chunk_id(shard_info.id);
        ASSERT_TRUE(chunk_num_1.has_value());

        // create another shard again.
        shard_info = create_shard(pg, Mi);
        auto chunk_num_2 = _obj_inst->get_shard_p_chunk_id(shard_info.id);
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
}

TEST_F(HomeObjectFixture, SealShard) {
    pg_id_t pg_id{1};
    create_pg(pg_id);
    auto shard_info = create_shard(pg_id, 64 * Mi);
    ASSERT_EQ(ShardInfo::State::OPEN, shard_info.state);

    // seal the shard
    shard_info = seal_shard(shard_info.id);
    ASSERT_EQ(ShardInfo::State::SEALED, shard_info.state);
}

TEST_F(HomeObjectFixture, ShardManagerRecovery) {
    pg_id_t pg_id{1};
    create_pg(pg_id);

    // create one shard;
    auto shard_info = create_shard(pg_id, Mi);
    auto shard_id = shard_info.id;
    EXPECT_EQ(ShardInfo::State::OPEN, shard_info.state);
    EXPECT_EQ(Mi, shard_info.total_capacity_bytes);
    EXPECT_EQ(Mi, shard_info.available_capacity_bytes);
    EXPECT_EQ(0ul, shard_info.deleted_capacity_bytes);
    EXPECT_EQ(pg_id, shard_info.placement_group);

    // restart homeobject and check if pg/shard info will be recovered.
    restart();

    // check PG after recovery.
    EXPECT_TRUE(_obj_inst->_pg_map.size() == 1);
    auto pg_iter = _obj_inst->_pg_map.find(pg_id);
    EXPECT_TRUE(pg_iter != _obj_inst->_pg_map.end());
    auto& pg_result = pg_iter->second;
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
    pg_iter = _obj_inst->_pg_map.find(pg_id);
    // verify the sequence number is correct after recovery.

    EXPECT_EQ(1, pg_iter->second->shard_sequence_num_);

    // re-create new shards on this pg works too even homeobject is restarted twice.
    auto new_shard_info = create_shard(pg_id, Mi);
    EXPECT_NE(shard_id, new_shard_info.id);

    EXPECT_EQ(ShardInfo::State::OPEN, new_shard_info.state);
    EXPECT_EQ(2, pg_iter->second->shard_sequence_num_);
}

TEST_F(HomeObjectFixture, SealedShardRecovery) {
    pg_id_t pg_id{1};
    create_pg(pg_id);

    // create one shard and seal it.
    auto shard_info = create_shard(pg_id, Mi);
    auto shard_id = shard_info.id;
    shard_info = seal_shard(shard_id);
    EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);

    // check the shard info from ShardManager to make sure on_commit() is successfully.
    auto pg_iter = _obj_inst->_pg_map.find(pg_id);
    EXPECT_TRUE(pg_iter != _obj_inst->_pg_map.end());
    auto& pg_result = pg_iter->second;
    EXPECT_EQ(1, pg_result->shards_.size());
    auto hs_shard = d_cast< homeobject::HSHomeObject::HS_Shard* >(pg_result->shards_.front().get());
    EXPECT_EQ(ShardInfo::State::SEALED, hs_shard->info.state);

    // release the homeobject and homestore will be shutdown automatically.
    LOGI("restart home_object");
    restart();

    // re-create the homeobject and pg infos and shard infos will be recover automatically.
    EXPECT_TRUE(_obj_inst->_pg_map.size() == 1);
    // check shard internal state;
    pg_iter = _obj_inst->_pg_map.find(pg_id);
    EXPECT_TRUE(pg_iter != _obj_inst->_pg_map.end());
    EXPECT_EQ(1, pg_iter->second->shards_.size());
    hs_shard = d_cast< homeobject::HSHomeObject::HS_Shard* >(pg_iter->second->shards_.front().get());
    auto& recovered_shard_info = hs_shard->info;
    verify_hs_shard(recovered_shard_info, shard_info);
}

TEST_F(HomeObjectFixture, SealShardWithRestart) {
    // Create a pg, shard, put blob should succeed, seal and put blob again should fail.
    // Recover and put blob again should fail.
    pg_id_t pg_id{1};
    create_pg(pg_id);

    auto shard_info = create_shard(pg_id, 64 * Mi);
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
