#include "homeobj_fixture.hpp"

TEST_F(HomeObjectFixture, BasicGC) {
    const auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    const auto num_shards_per_chunk = SISL_OPTIONS["num_shards"].as< uint64_t >();
    const auto num_blobs_per_shard = 2 * SISL_OPTIONS["num_blobs"].as< uint64_t >();

    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    std::map< pg_id_t, HSHomeObject::HS_PG* > HS_PG_map;
    std::map< pg_id_t, uint64_t > pg_chunk_nums;
    std::map< shard_id_t, std::map< blob_id_t, uint64_t > > shard_blob_ids_map;
    auto chunk_selector = _obj_inst->chunk_selector();

    // create pgs
    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        // do not use HS_PG_map[i] to change anything, const cast just for compiling
        HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
        pg_blob_id[i] = 0;
        pg_chunk_nums[i] = chunk_selector->get_pg_chunks(i)->size();
    }

    // create multiple shards for each chunk
    for (uint64_t i = 0; i < num_shards_per_chunk; i++) {
        std::map< pg_id_t, std::vector< shard_id_t > > pg_open_shard_id_vec;

        // create a shard for each chunk
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            for (uint64_t j = 0; j < chunk_num; j++) {
                auto shard_seq = i * chunk_num + j + 1;
                auto derived_shard_id = make_new_shard_id(pg_id, shard_seq); // shard id start from 1
                auto shard = create_shard(pg_id, 64 * Mi, "shard meta:" + std::to_string(derived_shard_id));
                LOGINFO("create shard pg={} shard {} in chunk {}", pg_id, shard.id, j);
                ASSERT_EQ(derived_shard_id, shard.id);
                pg_open_shard_id_vec[pg_id].emplace_back(shard.id);
                pg_shard_id_vec[pg_id].emplace_back(shard.id);
            }
        }

        // Put blob for all shards in all pg's.
        auto new_shard_blob_ids_map = put_blobs(pg_open_shard_id_vec, num_blobs_per_shard, pg_blob_id);
        for (const auto& [shard_id, blob_to_blk_count] : new_shard_blob_ids_map) {
            shard_blob_ids_map[shard_id].insert(blob_to_blk_count.begin(), blob_to_blk_count.end());
        }

        // seal all shards and check
        for (const auto& [pg_id, shard_vec] : pg_open_shard_id_vec) {
            auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (const auto& shard_id : shard_vec) {
                // seal the shards so that they can be selected for gc
                auto shard_info = seal_shard(shard_id);
                EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);

                auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
                ASSERT_TRUE(chunk_opt.has_value());
                auto chunk_id = chunk_opt.value();

                auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
                ASSERT_TRUE(EXVchunk != nullptr);
                ASSERT_EQ(EXVchunk->m_state, ChunkState::AVAILABLE);
                ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
                auto vchunk_id = EXVchunk->m_v_chunk_id.value();
                ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

                ASSERT_TRUE(EXVchunk->m_pg_id.has_value());
                ASSERT_EQ(EXVchunk->m_pg_id.value(), pg_id);
            }
        }

        for (const auto& [pg_id, hs_pg] : HS_PG_map) {
            uint64_t total_blob_occupied_blk_count{0};
            const auto& shard_vec = pg_shard_id_vec[pg_id];
            for (const auto& shard_id : shard_vec) {
                // TODO: GC will not persist shard header/footer futher,
                // temporarily comment blk count check.
                // total_blob_occupied_blk_count += 2; /*header and footer*/
                for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                    total_blob_occupied_blk_count += blk_count;
                }
            }
            // check pg durable entities
            // ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);

            // check pg index table, the valid blob index count should be equal to the blob count
            ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id]);
        }
    }

    // delete half of the blobs per shard.
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map_for_deletion;
        for (const auto& shard_id : shard_vec) {
            shard_blob_ids_map_for_deletion[shard_id];
            auto& blob_to_blk_count = shard_blob_ids_map[shard_id];
            for (uint64_t i = 0; i < num_blobs_per_shard / 2; i++) {
                ASSERT_FALSE(blob_to_blk_count.empty());
                auto it = blob_to_blk_count.begin();
                auto blob_id = it->first;
                shard_blob_ids_map_for_deletion[shard_id].insert(blob_id);
                blob_to_blk_count.erase(it);
            }
        }
        del_blobs(pg_id, shard_blob_ids_map_for_deletion);
    }

    // wait until all the deleted blobs are reclaimed
    bool all_deleted_blobs_have_been_gc{true};
    while (true) {
        // we need to recalculate this everytime, since gc might update a pchunk of the vchunk for a pg
        std::map< homestore::chunk_num_t, uint64_t > chunk_used_blk_count;

        for (const auto& [shard_id, blob_to_blk_count] : shard_blob_ids_map) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();
            // now, the chunk state is not determined, maybe GC(being gc) or AVAILABLE(complete gc), skip checking it.
            uint32_t used_blks{2}; /* header and footer */

            for (const auto& [_, blk_count] : blob_to_blk_count) {
                used_blks += blk_count;
            }
            chunk_used_blk_count[chunk_id] += used_blks;
        }

        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (uint64_t i{0}; i < chunk_num; i++) {
                auto chunk_id = pg_chunks->at(i);
                auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
                const auto available_blk = EXVchunk->available_blks();
                const auto total_blks = EXVchunk->get_total_blks();
                if (total_blks - available_blk != chunk_used_blk_count[chunk_id]) {
                    LOGINFO("pg_id={}, chunk_id={}, available_blk={}, total_blk={}, use_blk={}, waiting for gc", pg_id,
                            chunk_id, available_blk, total_blks, chunk_used_blk_count[chunk_id]);

                    if (0 == EXVchunk->get_defrag_nblks()) {
                        // some unexpect async write or free happens, increase defrag num to trigger gc again.
                        homestore::data_service().async_free_blk(homestore::MultiBlkId(0, 1, chunk_id));
                    }

                    all_deleted_blobs_have_been_gc = false;
                    break;
                }
            }
            if (!all_deleted_blobs_have_been_gc) break;
        }
        if (all_deleted_blobs_have_been_gc) break;
        all_deleted_blobs_have_been_gc = true;
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    // verify blob data after gc
    std::map< shard_id_t, std::set< blob_id_t > > remaining_shard_blobs;
    for (const auto& [shard_id, blob_to_blk_count] : shard_blob_ids_map) {
        for (const auto& [blob_id, _] : blob_to_blk_count) {
            remaining_shard_blobs[shard_id].insert(blob_id);
        }
    }
    verify_shard_blobs(remaining_shard_blobs);
    verify_shard_meta(pg_shard_id_vec);
    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        // after half blobs have been deleted, the tombstone indexes(half of the total blobs) have been removed by gc
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id] / 2);
        auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();

            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            ASSERT_TRUE(EXVchunk != nullptr);
            ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
            auto vchunk_id = EXVchunk->m_v_chunk_id.value();

            // after gc , pg_chunks should changes, the vchunk shoud change to a new pchunk. however, we can not make
            // sure the pchunk is changed since it is probably that the shard is copied from chunk_1 to chunk_2 and then
            // from chunk_2 to chunk_1, since gc might be scheduled several times when we delete blobs

            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

            ASSERT_TRUE(EXVchunk->m_pg_id.has_value());
            ASSERT_EQ(EXVchunk->m_pg_id.value(), pg_id);
        }
    }

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        uint64_t total_blob_occupied_blk_count{0};
        const auto& shard_vec = pg_shard_id_vec[pg_id];
        for (const auto& shard_id : shard_vec) {
            // total_blob_occupied_blk_count += 2; /*header and footer*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }

        // ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, total_blob_occupied_blk_count);
        // ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
    }

    restart();

    HS_PG_map.clear();

    for (uint64_t i = 1; i <= num_pgs; i++) {
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
    }

    chunk_selector = _obj_inst->chunk_selector();

    verify_shard_blobs(remaining_shard_blobs);

    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        // after half blobs have been deleted, the tombstone indexes(half of the total blobs) have been removed by gc
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id] / 2);
        auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();

            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            ASSERT_TRUE(EXVchunk != nullptr);
            ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
            auto vchunk_id = EXVchunk->m_v_chunk_id.value();

            // after restart , the pchunk of a vchunk shoud not change
            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);
        }
    }

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        uint64_t total_blob_occupied_blk_count{0};
        const auto& shard_vec = pg_shard_id_vec[pg_id];
        for (const auto& shard_id : shard_vec) {
            // total_blob_occupied_blk_count += 2; /*header and footer*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }
        // ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, total_blob_occupied_blk_count);
        // ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
    }

    // delete remaining blobs
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map_for_deletion;
        for (const auto& shard_id : shard_vec) {
            shard_blob_ids_map_for_deletion[shard_id] = remaining_shard_blobs[shard_id];
        }
        del_blobs(pg_id, shard_blob_ids_map_for_deletion);
    }

    // wait until all the deleted blobs are reclaimed
    while (true) {
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (uint64_t i{0}; i < chunk_num; i++) {
                auto chunk_id = pg_chunks->at(i);
                auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
                const auto available_blk = EXVchunk->available_blks();
                const auto total_blks = EXVchunk->get_total_blks();
                if (total_blks != available_blk) {

                    if (0 == EXVchunk->get_defrag_nblks()) {
                        // some unexpect async write or free happens, increase defrag num to trigger gc again.
                        homestore::data_service().async_free_blk(homestore::MultiBlkId(0, 1, chunk_id));
                    }

                    LOGINFO("pg_id={}, chunk_id={}, available_blk={}, total_blk={}, not empty, waiting for gc", pg_id,
                            chunk_id, available_blk, total_blks);
                    all_deleted_blobs_have_been_gc = false;
                    break;
                }
            }
            if (!all_deleted_blobs_have_been_gc) break;
        }
        if (all_deleted_blobs_have_been_gc) break;
        all_deleted_blobs_have_been_gc = true;
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    for (const auto& [shard_id, _] : shard_blob_ids_map) {
        auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
        ASSERT_TRUE(chunk_opt.has_value());
        auto chunk_id = chunk_opt.value();
        auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);

        // the shard is empty
        ASSERT_EQ(0, EXVchunk->get_used_blks());
    }

    // after all blobs have been deleted,
    // 1 the pg index table should be empty
    // 2 check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), 0);
        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, 0);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, 0);
    }

    // if all blobs of shard are deleted , the shard should be deleted.
    // TODO:add more check after we have delete shard implementation
}

TEST_F(HomeObjectFixture, HandlingNoSpaceLeft) {
    const auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    const auto num_shards_per_chunk = SISL_OPTIONS["num_shards"].as< uint64_t >();
    const auto num_blobs_per_shard = 2 * SISL_OPTIONS["num_blobs"].as< uint64_t >();

    std::map< pg_id_t, std::vector< shard_id_t > > total_pg_open_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    std::map< pg_id_t, uint64_t > pg_chunk_nums;
    std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map;
    auto chunk_selector = _obj_inst->chunk_selector();

    // create pgs
    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        pg_blob_id[i] = 0;
        pg_chunk_nums[i] = chunk_selector->get_pg_chunks(i)->size();
    }

    // create multiple shards for each chunk , we seal all shards except the last one
    for (uint64_t i = 0; i < num_shards_per_chunk; i++) {
        std::map< pg_id_t, std::vector< shard_id_t > > pg_open_shard_id_vec;

        // create a shard for each chunk
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            for (uint64_t j = 0; j < chunk_num; j++) {
                auto shard = create_shard(pg_id, 64 * Mi, "shard meta");
                pg_open_shard_id_vec[pg_id].emplace_back(shard.id);
            }
        }

        // Put blob for all shards in all pg's.
        auto new_shard_blob_ids_map = put_blobs(pg_open_shard_id_vec, num_blobs_per_shard, pg_blob_id);

        for (const auto& [shard_id, blob_to_blk_count] : new_shard_blob_ids_map) {
            for (const auto& [blob_id, _] : blob_to_blk_count)
                shard_blob_ids_map[shard_id].insert(blob_id);
        }

        // seal all shards except the last one and check
        for (const auto& [pg_id, shard_vec] : pg_open_shard_id_vec) {
            auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (const auto& shard_id : shard_vec) {
                auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
                ASSERT_TRUE(chunk_opt.has_value());
                auto chunk_id = chunk_opt.value();

                auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
                ASSERT_TRUE(EXVchunk != nullptr);
                if (i < num_shards_per_chunk - 1) {
                    // seal the shards so that they can be selected for gc
                    auto shard_info = seal_shard(shard_id);
                    EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);
                    // if not the last shard, the chunk should be available
                    ASSERT_EQ(EXVchunk->m_state, ChunkState::AVAILABLE);
                } else {
                    total_pg_open_shard_id_vec[pg_id].push_back(shard_id);
                    // if the last shard, the chunk should be inuse
                    ASSERT_EQ(EXVchunk->m_state, ChunkState::INUSE);
                }
            }
        }
    }

    // now we set all the last offset of the blk allocator of all the chunks to the end to simulater no_space_left in
    // all the followers.
    for (uint64_t i = 1; i <= num_pgs; i++) {
        run_on_pg_follower(i, [&]() {
            auto& data_service = homestore::data_service();
            auto blk_size = data_service.get_blk_size();
            auto pg_chunks = _obj_inst->chunk_selector()->get_pg_chunks(i);

            for (const auto& chunk : *(pg_chunks)) {
                auto vchunk = chunk_selector->get_extend_vchunk(chunk);
                ASSERT_TRUE(vchunk);
                auto available_blk_num = vchunk->available_blks();

                homestore::MultiBlkId all_remaining_blk;
                homestore::blk_alloc_hints hints;
                hints.chunk_id_hint = chunk;

                // allocate all the remaining blocks, so that there is no space left on this chunk
                const auto status = data_service.alloc_blks(available_blk_num * blk_size, hints, all_remaining_blk);

                LOGINFO("Set chunk {} to no_space_left, total_blks={}, available_blks={}, used_blks={}", chunk,
                        vchunk->get_total_blks(), vchunk->available_blks(), vchunk->get_used_blks());
                ASSERT_TRUE(status == homestore::BlkAllocStatus::SUCCESS);
                ASSERT_TRUE(vchunk->available_blks() == 0);
            }
        });
    }

    // now, trigger no space left in all chunks and all the put_blob should succeed
    auto new_shard_blob_ids_map = put_blobs(total_pg_open_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    for (const auto& [shard_id, blob_to_blk_count] : new_shard_blob_ids_map) {
        for (const auto& [blob_id, _] : blob_to_blk_count)
            shard_blob_ids_map[shard_id].insert(blob_id);
    }

    verify_shard_blobs(shard_blob_ids_map);
}

TEST_F(HomeObjectFixture, BasicEGC) { EmergentGC(false); }

TEST_F(HomeObjectFixture, EGCWithCrashRecovery) { EmergentGC(true); }

void HomeObjectFixture::EmergentGC(bool with_crash_recovery) {
    const auto num_shards_per_chunk = SISL_OPTIONS["num_shards"].as< uint64_t >();
    const auto num_blobs_per_shard = 2 * SISL_OPTIONS["num_blobs"].as< uint64_t >();
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    std::map< pg_id_t, HSHomeObject::HS_PG* > HS_PG_map;
    std::map< pg_id_t, uint64_t > pg_chunk_nums;
    std::map< shard_id_t, std::map< blob_id_t, uint64_t > > shard_blob_ids_map;
    auto chunk_selector = _obj_inst->chunk_selector();
    const auto num_pgs = chunk_selector->get_pdev_chunks().size();

    for (uint16_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        // do not use HS_PG_map[i] to change anything, const cast just for compiling
        HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
        pg_blob_id[i] = 0;
        pg_chunk_nums[i] = chunk_selector->get_pg_chunks(i)->size();
    }

    // create multiple shards for each chunk , we seal all shards except the last one
    for (uint64_t i = 0; i < num_shards_per_chunk; i++) {
        std::map< pg_id_t, std::vector< shard_id_t > > pg_open_shard_id_vec;

        // create a shard for each chunk
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            for (uint64_t j = 0; j < chunk_num; j++) {
                auto shard = create_shard(pg_id, 64 * Mi, "shard meta");
                pg_open_shard_id_vec[pg_id].emplace_back(shard.id);
                pg_shard_id_vec[pg_id].emplace_back(shard.id);
            }
        }

        // Put blob for all shards in all pg's.
        auto new_shard_blob_ids_map = put_blobs(pg_open_shard_id_vec, num_blobs_per_shard, pg_blob_id);

        for (const auto& [shard_id, blob_to_blk_count] : new_shard_blob_ids_map) {
            shard_blob_ids_map[shard_id].insert(blob_to_blk_count.begin(), blob_to_blk_count.end());
        }

        // seal all shards except the last one and check
        for (const auto& [pg_id, shard_vec] : pg_open_shard_id_vec) {
            auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (const auto& shard_id : shard_vec) {
                auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
                ASSERT_TRUE(chunk_opt.has_value());
                auto chunk_id = chunk_opt.value();

                auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
                ASSERT_TRUE(EXVchunk != nullptr);
                if (i < num_shards_per_chunk - 1) {
                    // seal the shards so that they can be selected for gc
                    auto shard_info = seal_shard(shard_id);
                    EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);
                    // if not the last shard, the chunk should be available
                    ASSERT_EQ(EXVchunk->m_state, ChunkState::AVAILABLE);
                } else {
                    // if the last shard, the chunk should be inuse
                    ASSERT_EQ(EXVchunk->m_state, ChunkState::INUSE);
                }
                ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
                auto vchunk_id = EXVchunk->m_v_chunk_id.value();
                ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

                ASSERT_TRUE(EXVchunk->m_pg_id.has_value());
                ASSERT_EQ(EXVchunk->m_pg_id.value(), pg_id);
            }
        }
    }

    // delete half of the blobs per shard.
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map_for_deletion;
        for (const auto& shard_id : shard_vec) {
            shard_blob_ids_map_for_deletion[shard_id];
            auto& blob_to_blk_count = shard_blob_ids_map[shard_id];
            for (uint64_t i = 0; i < num_blobs_per_shard / 2; i++) {
                ASSERT_FALSE(blob_to_blk_count.empty());
                auto it = blob_to_blk_count.begin();
                auto blob_id = it->first;
                shard_blob_ids_map_for_deletion[shard_id].insert(blob_id);
                blob_to_blk_count.erase(it);
            }
        }
        del_blobs(pg_id, shard_blob_ids_map_for_deletion);
    }

    // do not seal the last shard and trigger gc mannually to simulate emergent gc
    auto gc_mgr = _obj_inst->gc_manager();
    std::vector< folly::SemiFuture< bool > > futs;

    if (with_crash_recovery) {
        const auto egc_thread_count_per_pdev = HS_BACKEND_DYNAMIC_CONFIG(reserved_chunk_num_per_pdev_for_egc);
#ifdef _PRERELEASE
        // for each emergent gc thread, we simutlate a crash.
        set_basic_flip("simulate_gc_crash_recovery", egc_thread_count_per_pdev * num_pgs);
#endif
        // trigger egc. since we have enabled the above flip, the gc task will return without removing gc_task_meta_blk,
        // so that they will be replayed when recovery
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (uint64_t i{0}; i < egc_thread_count_per_pdev; i++) {
                auto chunk_id = pg_chunks->at(i);
                futs.emplace_back(gc_mgr->submit_gc_task(task_priority::emergent, chunk_id));
            }
        }
    } else {
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (uint64_t i{0}; i < chunk_num; i++) {
                auto chunk_id = pg_chunks->at(i);
                futs.emplace_back(gc_mgr->submit_gc_task(task_priority::emergent, chunk_id));
            }
        }
    }

    // wait for all egc completed
    folly::collectAllUnsafe(futs)
        .thenValue([](auto&& results) {
            for (auto const& ok : results) {
                ASSERT_TRUE(ok.hasValue());
                // all egc task should be completed.
                ASSERT_TRUE(ok.value());
            }
        })
        .get();

    futs.clear();

    if (with_crash_recovery) {
        // this will recover gc task
        gc_mgr.reset();
        restart();

        gc_mgr = _obj_inst->gc_manager();
        chunk_selector = _obj_inst->chunk_selector();

        HS_PG_map.clear();
        for (uint64_t i = 1; i <= num_pgs; i++) {
            auto hs_pg = _obj_inst->get_hs_pg(i);
            ASSERT_TRUE(hs_pg != nullptr);
            HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
        }

        // then we gc all the chunks again
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (uint64_t i{0}; i < chunk_num; i++) {
                auto chunk_id = pg_chunks->at(i);
                futs.emplace_back(gc_mgr->submit_gc_task(task_priority::emergent, chunk_id));
            }
        }

        // wait for all egc completed
        folly::collectAllUnsafe(futs)
            .thenValue([](auto&& results) {
                for (auto const& ok : results) {
                    ASSERT_TRUE(ok.hasValue());
                    // all egc task should be completed.
                    ASSERT_TRUE(ok.value());
                }
            })
            .get();

        futs.clear();
    }

    // verify blob data after gc
    std::map< shard_id_t, std::set< blob_id_t > > remaining_shard_blobs;
    for (const auto& [shard_id, blob_to_blk_count] : shard_blob_ids_map) {
        for (const auto& [blob_id, _] : blob_to_blk_count) {
            remaining_shard_blobs[shard_id].insert(blob_id);
        }
    }
    verify_shard_blobs(remaining_shard_blobs);

    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        // after half blobs have been deleted, the tombstone indexes(half of the total blobs) have been removed by gc
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id] / 2);
        auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();

            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            ASSERT_TRUE(EXVchunk != nullptr);

            // emergent gc, then chunk is still in use
            ASSERT_EQ(EXVchunk->m_state, ChunkState::INUSE)
                << "fail chunk_id=" << chunk_id << ", shard_id=" << shard_id;
            ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
            auto vchunk_id = EXVchunk->m_v_chunk_id.value();

            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

            ASSERT_TRUE(EXVchunk->m_pg_id.has_value());
            ASSERT_EQ(EXVchunk->m_pg_id.value(), pg_id);
        }
    }

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        uint64_t total_blob_occupied_blk_count{0};
        const auto& shard_vec = pg_shard_id_vec[pg_id];
        for (const auto& shard_id : shard_vec) {
            total_blob_occupied_blk_count += 2; /*header and footer*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }
        // for each chunk, we have an open shard, which has only header.
        total_blob_occupied_blk_count -= pg_chunk_nums[pg_id];

        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, total_blob_occupied_blk_count);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
    }

    gc_mgr.reset();
    restart();

    HS_PG_map.clear();

    for (uint64_t i = 1; i <= num_pgs; i++) {
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
    }

    chunk_selector = _obj_inst->chunk_selector();
    gc_mgr = _obj_inst->gc_manager();

    verify_shard_blobs(remaining_shard_blobs);

    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        // after half blobs have been deleted, the tombstone indexes(half of the total blobs) have been removed by gc
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id] / 2);
        auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();

            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            ASSERT_TRUE(EXVchunk != nullptr);
            // emergent gc, then chunk is still in use
            ASSERT_EQ(EXVchunk->m_state, ChunkState::INUSE);
            ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
            auto vchunk_id = EXVchunk->m_v_chunk_id.value();

            // after gc , pg_chunks should changes, the vchunk shoud change to a new pchunk.
            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

            ASSERT_TRUE(EXVchunk->m_pg_id.has_value());
            ASSERT_EQ(EXVchunk->m_pg_id.value(), pg_id);
        }
    }

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        uint64_t total_blob_occupied_blk_count{0};
        const auto& shard_vec = pg_shard_id_vec[pg_id];
        for (const auto& shard_id : shard_vec) {
            total_blob_occupied_blk_count += 2; /*header and footer*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }
        // for each chunk, we have an open shard, which has only header.
        total_blob_occupied_blk_count -= pg_chunk_nums[pg_id];

        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, total_blob_occupied_blk_count);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
    }

    // delete remaining blks
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map_for_deletion;
        for (const auto& shard_id : shard_vec) {
            shard_blob_ids_map_for_deletion[shard_id] = remaining_shard_blobs[shard_id];
        }
        del_blobs(pg_id, shard_blob_ids_map_for_deletion);
    }

    // trigger egc for all chunks
    for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
        const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (uint64_t i{0}; i < chunk_num; i++) {
            auto chunk_id = pg_chunks->at(i);
            futs.emplace_back(gc_mgr->submit_gc_task(task_priority::emergent, chunk_id));
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

    // for each chunk in this pg, there is only one shard header
    for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
        const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (uint64_t i{0}; i < chunk_num; i++) {
            auto chunk_id = pg_chunks->at(i);
            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);

            // the open shard is not sealed, so there is only the shard header for each shard
            ASSERT_EQ(EXVchunk->get_used_blks(), 1);
        }
    }

    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        auto& hs_pg = HS_PG_map[pg_id];
        // check pg durable entities. only shard header left, and every chunk has a open shard, so
        // total_occupied_blk_count is equal to the num of chunks in this pg since each chunk has a shard header.
        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, pg_chunk_nums[pg_id]);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, pg_chunk_nums[pg_id]);

        // after all blobs have been deleted, the pg index table should be empty
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), 0);

        auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();

            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            ASSERT_TRUE(EXVchunk != nullptr);
            ASSERT_EQ(EXVchunk->m_state, ChunkState::INUSE);
            ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
            auto vchunk_id = EXVchunk->m_v_chunk_id.value();

            // after gc , pg_chunks should changes, the vchunk shoud change to a new pchunk.
            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);
        }
    }

#ifdef _PRERELEASE
    remove_flip("simulate_gc_crash_recovery");
#endif

    // TODO:: add more check after we have delete shard implementation
}

TEST_F(HomeObjectFixture, GCTaskPbaChunkCheck) {
    const pg_id_t pg_id = 1;
    const auto num_blobs = 10;

    create_pg(pg_id);

    std::map< pg_id_t, blob_id_t > pg_blob_id;
    pg_blob_id[pg_id] = 0;

    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    auto shard = create_shard(pg_id, 64 * Mi, "shard meta");
    pg_shard_id_vec[pg_id].push_back(shard.id);
    put_blobs(pg_shard_id_vec, num_blobs, pg_blob_id);

    auto chunk_selector = _obj_inst->chunk_selector();
    auto hs_pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(hs_pg != nullptr);
    auto gc_mgr = _obj_inst->gc_manager();

    auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard.id);
    EXPECT_TRUE(chunk_opt.has_value());

    auto get_current_chunk = [&]() -> chunk_id_t {
        auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard.id);
        EXPECT_TRUE(chunk_opt.has_value());
        return chunk_opt.value();
    };

    chunk_id_t cur_chunk = get_current_chunk();

    // Case 1: gc succeeds when all blob pbas match move_from_chunk.
    ASSERT_TRUE(gc_mgr->submit_gc_task(task_priority::emergent, cur_chunk).get())
        << "emergent gc should succeed when all blob pbas match move_from_chunk";

    cur_chunk = get_current_chunk();

    // Case 2: gc fails when a blob's pba chunk_id does not match move_from_chunk.
    // Blob 0's correct pba (captured in existing_value) is needed for Case 3.
    BlobRouteValue existing_value;

    BlobRouteKey index_key{BlobRoute{shard.id, 0 /* blob_id */}};
    BlobRouteValue wrong_value{homestore::MultiBlkId{0, 1, std::numeric_limits< chunk_id_t >::max()}};
    homestore::BtreeSinglePutRequest inject_req{&index_key, &wrong_value, homestore::btree_put_type::UPDATE,
                                                &existing_value};
    ASSERT_EQ(hs_pg->index_table_->put(inject_req), homestore::btree_status_t::success)
        << "failed to inject wrong pba into pg index table";

    ASSERT_FALSE(gc_mgr->submit_gc_task(task_priority::emergent, cur_chunk).get())
        << "emergent gc should fail when a blob's pba chunk_id does not match move_from_chunk";

    // Case 3: gc succeeds again after restoring the correct pba.
    homestore::BtreeSinglePutRequest restore_req{&index_key, &existing_value, homestore::btree_put_type::UPDATE,
                                                 nullptr};
    ASSERT_EQ(hs_pg->index_table_->put(restore_req), homestore::btree_status_t::success)
        << "failed to restore correct pba into pg index table";

    ASSERT_TRUE(gc_mgr->submit_gc_task(task_priority::emergent, cur_chunk).get())
        << "emergent gc should succeed after restoring correct blob pba";

    seal_shard(shard.id);

    // deleted blob so that the gc task will be really scheduled for normal gc.
    del_blob(pg_id, shard.id, 1);

    // the same check for normal gc task.
    ASSERT_TRUE(gc_mgr->submit_gc_task(task_priority::normal, cur_chunk).get())
        << "normal gc should succeed when all blob pbas match move_from_chunk";
    cur_chunk = get_current_chunk();

    ASSERT_EQ(hs_pg->index_table_->put(inject_req), homestore::btree_status_t::success)
        << "failed to inject wrong pba into pg index table";

    del_blob(pg_id, shard.id, 2);

    ASSERT_FALSE(gc_mgr->submit_gc_task(task_priority::normal, cur_chunk).get())
        << "normal gc should fail when a blob's pba chunk_id does not match move_from_chunk";

    homestore::BtreeSinglePutRequest new_restore_req{&index_key, &existing_value, homestore::btree_put_type::UPDATE,
                                                     nullptr};
    ASSERT_EQ(hs_pg->index_table_->put(new_restore_req), homestore::btree_status_t::success)
        << "failed to restore correct pba into pg index table";

    ASSERT_TRUE(gc_mgr->submit_gc_task(task_priority::normal, cur_chunk).get())
        << "normal gc should succeed after restoring correct blob pba";
}

// ===================================================================================================
// StalePChunkRouteAfterGC: CREATE_SHARD stale pchunk race (GC / shard-blob route
// inconsistency).
//
// EXACT SCENARIO (chunks_per_pg == 1 forces shard1 and shard2 onto the SAME vchunk N):
//
//   ① CREATE_SHARD2 log is already present in the log store on the laggy follower.
//      (The leader issued seal shard1 then create shard 2; raft appended both logs
//       before commit_index advanced, so the follower sees CREATE_SHARD2 log before SEAL_SHARD1
//       has committed.)
//   ② SEAL_SHARD1 on_commit runs to completion:
//        release_chunk(vchunk_N)  →  vchunk_N becomes AVAILABLE, pchunk is still A.
//   ③ [gate fires] GC runs a normal relocation of pchunk A → B.
//        vchunk_N live pchunk becomes B.  pchunk_A is now an orphaned reserved chunk.
//   ④ CREATE_SHARD2 on_commit resumes:
//        alloc_blks(application_hint = vchunk_N)
//        → must resolve the LIVE pchunk B, NOT the stale A.
//
// On UNFIXED code the alloc in step ④ would see vchunk AVAILABLE and
// grab the current pchunk at that instant; if pchunk_B had not been resolved yet it would get A.
// With the current alloc_blks call happening AFTER the gate resumes (post-GC), B is the live
// pchunk and the test verifies p_chunk(shard2) == live_pchunk(vchunk_N).
//
// MUST be run with --chunks_per_pg=1 so the successor shard is forced to reuse the predecessor vchunk.
//
// ===================================================================================================
#ifdef _PRERELEASE
TEST_F(HomeObjectFixture, StalePChunkRouteAfterGC) {
    const pg_id_t pg_id = 1;
    const auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >();

    ASSERT_EQ(SISL_OPTIONS["chunks_per_pg"].as< uint64_t >(), 1u)
        << "This reproduction must be run with --chunks_per_pg=1 to force vchunk reuse by the successor shard";

    create_pg(pg_id);
    auto chunk_selector = _obj_inst->chunk_selector();

    if (!am_i_in_pg(pg_id)) {
        // not a member, just keep the sync barriers aligned and leave.
        g_helper->sync(); // arm barrier
        g_helper->sync(); // end barrier
        return;
    }

    // The laggy follower is the non-leader replica number 2 (leader defaults to replica 0).
    const bool i_am_leader = (g_helper->my_replica_id() == get_leader_id(pg_id));
    const bool i_am_repro_follower = (!i_am_leader) && (g_helper->replica_num() == 2);

    std::mutex repro1_mtx;
    std::condition_variable repro1_cv;
    std::atomic< bool > repro1_blocked{false};
    std::atomic< bool > repro1_released{false};

    // ---- shard1: create and fill with blobs, then delete half to create garbage for normal GC ----
    auto shard1 = create_shard(pg_id, 64 * Mi, "shard1");
    ASSERT_NE(shard1.id, 0u);

    std::map< pg_id_t, std::vector< shard_id_t > > shards{{pg_id, {shard1.id}}};
    std::map< pg_id_t, blob_id_t > pg_blob_id{{pg_id, 0}};
    put_blobs(shards, num_blobs_per_shard, pg_blob_id);

    // Delete half the blobs so pchunk_A has garbage that triggers normal GC (gc_garbage_rate_threshold=0).
    // This is the realistic production trigger: GC fires because the chunk has freed space.
    {
        std::map< shard_id_t, std::set< blob_id_t > > to_delete;
        for (blob_id_t b = 0; b < num_blobs_per_shard / 2; ++b)
            to_delete[shard1.id].insert(b);
        del_blobs(pg_id, to_delete);
    }

    // record this replica's local vchunk N and pchunk A for shard1
    auto vchunk_N = _obj_inst->get_shard_v_chunk_id(shard1.id);
    auto pchunk_A = _obj_inst->get_shard_p_chunk_id(shard1.id);
    ASSERT_TRUE(vchunk_N.has_value());
    ASSERT_TRUE(pchunk_A.has_value());

    // ---- arm the repro flips on exactly one follower so quorum (leader + other follower) is unaffected ----
    if (i_am_repro_follower) {
        auto repl_dev = _obj_inst->get_hs_pg(pg_id)->repl_dev_;
        auto dont_care = m_fc.create_condition("", flip::Operator::DONT_CARE, (int)0);
        flip::FlipFrequency freq;
        freq.set_count(1);
        freq.set_percent(100);

        // Flip 1: in SEAL_SHARD1 commit — spin until CREATE_SHARD2 log is in the log store before
        // release_chunk runs. Explicit guarantee that the race window actually exists.
        m_fc.inject_callback_flip< void, int64_t >(
            "wait_create_shard_in_log", {dont_care}, freq,
            std::function< void(int64_t) >([&, repl_dev](int64_t seal_lsn) {
                while (repl_dev->get_last_append_lsn() <= seal_lsn) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }
                LOGI("[StalePChunkRouteAfterGC] CREATE_SHARD2 in log store (last_append_lsn={} > seal_lsn={})",
                     repl_dev->get_last_append_lsn(), seal_lsn);
            }));

        // Flip 2: in CREATE_SHARD2 commit — pause before local_create_shard so GC can run in the race window.
        // NOTE: do NOT spin inside a commit callback (commit_ext runs on the iomgr I/O thread;
        // spinning there blocks log-append processing and causes deadlock in single-threaded executors).
        m_fc.inject_callback_flip< void >("pause_create_shard_commit", {dont_care}, freq,
                                          std::function< void() >([&]() {
                                              LOGI("[StalePChunkRouteAfterGC] pausing CREATE_SHARD commit");
                                              std::unique_lock< std::mutex > lk(repro1_mtx);
                                              repro1_blocked.store(true);
                                              repro1_cv.notify_all();
                                              repro1_cv.wait(lk, [&] { return repro1_released.load(); });
                                              LOGI("[StalePChunkRouteAfterGC] resuming CREATE_SHARD commit");
                                          }));
        LOGINFO("[StalePChunkRouteAfterGC] armed on follower replica={}, pg={}, vchunk={}, pchunk_A={}",
                g_helper->replica_num(), pg_id, vchunk_N.value(), pchunk_A.value());
    }

    g_helper->sync(); // make sure the hook is armed before the leader drives seal+create

    // ---- leader drives seal(shard1) then create(shard2) back-to-back, WITHOUT per-op sync barriers ----
    shard_id_t shard2_id = INVALID_UINT64_ID;
    run_on_pg_leader(pg_id, [&]() {
        auto tid = generateRandomTraceId();
        auto sealed = _obj_inst->shard_manager()->seal_shard(shard1.id, tid).get();
        RELEASE_ASSERT(!!sealed, "failed to seal shard1");
        auto created = _obj_inst->shard_manager()->create_shard(pg_id, 64 * Mi, "shard2", tid).get();
        RELEASE_ASSERT(!!created, "failed to create shard2");
        g_helper->set_uint64_id(created.value().id);
        LOGINFO("[StalePChunkRouteAfterGC] leader sealed shard1=0x{:x} and created shard2=0x{:x}", shard1.id,
                created.value().id);
    });

    // everyone learns shard2 id from IPC
    while ((shard2_id = g_helper->get_uint64_id()) == INVALID_UINT64_ID) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // ---- the laggy follower: exact reproduction of the production race ----
    // At this point raft commit ordering guarantees:
    //   SEAL_SHARD1 committed first  → vchunk_N is AVAILABLE, pchunk still A
    //   CREATE_SHARD2 commit is now queued, paused at the gate (before alloc_blks)
    // Normal GC fires because pchunk_A has garbage (deleted blobs), relocates A -> B.
    // Then the gate releases and alloc_blks resolves the live pchunk B.
    if (i_am_repro_follower) {
        {
            std::unique_lock< std::mutex > lk(repro1_mtx);
            ASSERT_TRUE(repro1_cv.wait_for(lk, std::chrono::seconds(120), [&] { return repro1_blocked.load(); }))
                << "CREATE_SHARD2 commit was never paused on the repro follower";
        }
        LOGINFO("[StalePChunkRouteAfterGC] follower replica={} sees CREATE_SHARD2 paused; "
                "vchunk={} is AVAILABLE (seal done), pchunk_A={}, running normal GC to remap A -> B",
                g_helper->replica_num(), vchunk_N.value(), pchunk_A.value());

        // Normal GC: chunk has garbage from deleted blobs (gc_garbage_rate_threshold=0 in the CTest entry).
        auto fut = _obj_inst->gc_manager()->submit_gc_task(task_priority::normal, pchunk_A.value());
        bool gc_ok = std::move(fut).get();
        ASSERT_TRUE(gc_ok) << "normal GC on pchunk=" << pchunk_A.value() << " failed";

        // release the gate: alloc_blks runs and resolves live pchunk B.
        {
            std::unique_lock< std::mutex > lk(repro1_mtx);
            repro1_released.store(true);
            repro1_cv.notify_all();
        }
    }

    // wait for shard2 to be created locally on every member.
    while (!_obj_inst->shard_manager()->get_shard(shard2_id, 0).get()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    g_helper->sync();

    // ---- verification: shard2's recorded p_chunk must match the live vchunk->pchunk mapping on EVERY replica ----
    auto v2 = _obj_inst->get_shard_v_chunk_id(shard2_id);
    auto p2 = _obj_inst->get_shard_p_chunk_id(shard2_id);
    ASSERT_TRUE(v2.has_value());
    ASSERT_TRUE(p2.has_value());
    ASSERT_EQ(v2.value(), vchunk_N.value())
        << "successor shard2 did not reuse shard1's vchunk (need --chunks_per_pg=1)";

    auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
    ASSERT_TRUE(pg_chunks != nullptr);
    auto live_pchunk = pg_chunks->at(v2.value());

    LOGINFO("[StalePChunkRouteAfterGC] replica={} shard2 vchunk={} stored_p_chunk={} live_p_chunk={} (original "
            "pchunk_A={})",
            g_helper->replica_num(), v2.value(), p2.value(), live_pchunk, pchunk_A.value());

    EXPECT_EQ(p2.value(), live_pchunk) << "shard2 on replica " << static_cast< int >(g_helper->replica_num())
                                       << " is routed to pchunk " << p2.value()
                                       << " but the live vchunk->pchunk mapping is " << live_pchunk
                                       << " (stale shard/blob route; see PG 39 / PG 3409)";

    if (i_am_repro_follower) {
        EXPECT_NE(live_pchunk, pchunk_A.value())
            << "expected GC to have relocated vchunk " << v2.value() << " off its original pchunk " << pchunk_A.value()
            << " (the race window was not actually exercised)";

        auto old_chunk = chunk_selector->get_extend_vchunk(pchunk_A.value());
        ASSERT_TRUE(old_chunk != nullptr);
        EXPECT_FALSE(old_chunk->m_pg_id.has_value())
            << "the original pchunk " << pchunk_A.value() << " should be an orphaned reserved chunk after GC remap";

        LOGINFO(
            "[StalePChunkRouteAfterGC] fix verified on laggy follower replica={}: shard2 followed the GC remap to live "
            "pchunk={} (original pchunk_A={} is now orphaned); no stale route",
            g_helper->replica_num(), live_pchunk, pchunk_A.value());
    }

    // ---- put blobs into shard2, then seal it ----
    const blob_id_t shard2_first_blob_id = pg_blob_id[pg_id];
    std::map< pg_id_t, std::vector< shard_id_t > > shard2_map{{pg_id, {shard2_id}}};
    put_blobs(shard2_map, num_blobs_per_shard, pg_blob_id);
    g_helper->sync();

    {
        auto pg_chunks_after = chunk_selector->get_pg_chunks(pg_id);
        ASSERT_TRUE(pg_chunks_after != nullptr);
        auto live_pchunk_after = pg_chunks_after->at(v2.value());
        EXPECT_EQ(p2.value(), live_pchunk_after)
            << "after putting blobs into shard2 on replica " << static_cast< int >(g_helper->replica_num())
            << ", shard2's recorded pchunk " << p2.value() << " diverged from the live mapping " << live_pchunk_after;
    }
    verify_get_blob(shard2_map, num_blobs_per_shard, false /* use_random_offset */, true /* wait_when_not_exist */,
                    {{pg_id, shard2_first_blob_id}});
    g_helper->sync();

    run_on_pg_leader(pg_id, [&]() {
        auto sealed2 = _obj_inst->shard_manager()->seal_shard(shard2_id, generateRandomTraceId()).get();
        RELEASE_ASSERT(!!sealed2, "failed to seal shard2");
        LOGINFO("[StalePChunkRouteAfterGC] leader sealed shard2=0x{:x}", shard2_id);
    });

    while (true) {
        auto s2 = _obj_inst->shard_manager()->get_shard(shard2_id, 0).get();
        if (s2 && s2.value().state == ShardInfo::State::SEALED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    g_helper->sync();

    // Remove flips here: all sync barriers and raft operations are complete, so
    // __callback_flip has long since returned on all replicas. No UAF possible.
    if (i_am_repro_follower) {
        m_fc.remove_flip("wait_create_shard_in_log");
        m_fc.remove_flip("pause_create_shard_commit");
    }
}

// ===================================================================================================
// StaleBlobRouteAfterSealAndGC: PUT_BLOB races with SEAL_SHARD pre_commit; sealed_lsn guard rejects it.
//
// Production scenario :
//   A PUT_BLOB whose admission check passed (shard OPEN) should be rejected if the shard gets
//   sealed before the blob is committed. The sealed_lsn guard in on_blob_put_commit must catch it.
//
// Exact sequence modelled (single replica, leader):
//   ① SEAL_SHARD pre_commit fires and PAUSES before changing state=SEALED
//      (flip "pause_seal_pre_commit").  At this point shard state is still OPEN.
//   ② _put_blob is called in the test thread. get_blk_alloc_hints sees state==OPEN → passes,
//      blk is allocated on pchunk_A.  The put is async (raft not yet committed).
//   ③ Gate releases → state = SEALED → SEAL_SHARD commit → sealed_lsn = X.
//   ④ PUT_BLOB commit (lsn = X+1): on_blob_put_commit checks lsn(X+1) >= sealed_lsn(X) → reject.
//      The allocated blk is freed; the blob does NOT land in the pg index.
//
// Verification: the late blob is absent from the index; bulk blobs are still readable.
//
// Runs on the leader replica only; no multi-replica complexity needed.
// Pause point: flip "pause_seal_pre_commit". Compiled out of release builds.
// ===================================================================================================
TEST_F(HomeObjectFixture, StaleBlobRouteAfterSealAndGC) {
    const pg_id_t pg_id = 1;
    const auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >();

    create_pg(pg_id);

    if (!am_i_in_pg(pg_id)) {
        g_helper->sync();
        g_helper->sync();
        return;
    }

    const bool i_am_leader = (g_helper->my_replica_id() == get_leader_id(pg_id));

    std::mutex repro2_mtx;
    std::condition_variable repro2_cv;
    std::atomic< bool > repro2_blocked{false};
    std::atomic< bool > repro2_released{false};

    // ---- shard1: create and fill with blobs ----
    auto shard1 = create_shard(pg_id, 64 * Mi, "shard1");
    ASSERT_NE(shard1.id, 0u);

    std::map< pg_id_t, std::vector< shard_id_t > > shards{{pg_id, {shard1.id}}};
    std::map< pg_id_t, blob_id_t > pg_blob_id{{pg_id, 0}};
    put_blobs(shards, num_blobs_per_shard, pg_blob_id);

    // ---- arm the flip on the leader: pause SEAL pre_commit before state=SEALED ----
    if (i_am_leader) {
        auto dont_care = m_fc.create_condition("", flip::Operator::DONT_CARE, (int)0);
        flip::FlipFrequency freq;
        freq.set_count(3); // 3 replicas all call callback_flip; count must be >= num_replicas
        freq.set_percent(100);
        m_fc.inject_callback_flip< void >("pause_seal_pre_commit", {dont_care}, freq, std::function< void() >([&]() {
                                              LOGI(
                                                  "[StaleBlobRouteAfterSealAndGC] pausing SEAL pre_commit BEFORE lock");
                                              std::unique_lock< std::mutex > lk(repro2_mtx);
                                              repro2_blocked.store(true);
                                              repro2_cv.notify_all();
                                              repro2_cv.wait(lk, [&] { return repro2_released.load(); });
                                              LOGI("[StaleBlobRouteAfterSealAndGC] resuming SEAL pre_commit");
                                          }));
        LOGINFO("[StaleBlobRouteAfterSealAndGC] armed pause_seal_pre_commit on leader replica={}",
                g_helper->replica_num());
    }

    g_helper->sync(); // make sure flip is armed on all replicas before proceeding

    // ---- leader: trigger seal and race put_blob ----
    blob_id_t late_blob_id [[maybe_unused]] = INVALID_UINT64_ID;
    if (i_am_leader) {
        // 1. Start seal_shard in a background thread so it runs concurrently.
        //    seal_shard will hit the gate in pre_commit and pause there.
        auto tid = generateRandomTraceId();
        bool seal_ok = false;
        std::thread seal_thread([&]() {
            auto r = std::move(_obj_inst->shard_manager()->seal_shard(shard1.id, tid)).get();
            seal_ok = r.hasValue();
        });

        // 2. Wait until pre_commit is paused (shard state is still OPEN).
        {
            std::unique_lock< std::mutex > lk(repro2_mtx);
            if (!repro2_cv.wait_for(lk, std::chrono::seconds(30), [&] { return repro2_blocked.load(); })) {
                repro2_released.store(true); // avoid deadlock if gate never fires
                repro2_cv.notify_all();
                seal_thread.join();
                m_fc.remove_flip("pause_seal_pre_commit");
                FAIL() << "SEAL pre_commit never reached the pause point";
            }
        }
        LOGINFO("[StaleBlobRouteAfterSealAndGC] leader sees SEAL pre_commit paused; shard state=OPEN; "
                "calling _put_blob with shard still OPEN");

        // 3. Call _put_blob in a background thread: shard state is OPEN → get_blk_alloc_hints
        //    passes → blk allocated.  The .get() will complete AFTER gate release lets raft commit.
        bool blob_rejected = false;
        std::thread blob_thread([&]() {
            auto blob = build_blob(num_blobs_per_shard);
            auto b = std::move(_obj_inst->_put_blob(shard1, std::move(blob), tid)).get();
            blob_rejected = !b.hasValue();
            LOGINFO("[StaleBlobRouteAfterSealAndGC] leader: _put_blob result: {}",
                    b.hasValue() ? "admitted" : "rejected");
        });

        // 4. Release the gate: state = SEALED, seal pre_commit returns → raft commits seal.
        //    After seal commit, sealed_lsn = lsn_seal.  Then put_blob commit fires and
        //    on_blob_put_commit checks lsn(put) >= sealed_lsn → rejects.
        {
            std::unique_lock< std::mutex > lk(repro2_mtx);
            repro2_released.store(true);
            repro2_cv.notify_all();
        }
        LOGINFO("[StaleBlobRouteAfterSealAndGC] leader gate released; state→SEALED; seal commit in flight");

        // 5. Wait for both background threads.
        blob_thread.join();
        seal_thread.join();
        m_fc.remove_flip("pause_seal_pre_commit");
        ASSERT_TRUE(seal_ok) << "seal_shard failed";

        EXPECT_TRUE(blob_rejected)
            << "[StaleBlobRouteAfterSealAndGC-fix] late _put_blob should have been rejected by sealed_lsn guard!";
        if (blob_rejected) {
            LOGINFO("[StaleBlobRouteAfterSealAndGC] leader: late blob correctly rejected (sealed_lsn guard worked)");
        }
        // propagate "no blob" to other replicas
        g_helper->set_uint64_id(INVALID_UINT64_ID);
    }

    g_helper->sync();

    // ---- verification: bulk blobs still readable on all replicas ----
    verify_get_blob(shards, num_blobs_per_shard, false /* random_offset */, true /* wait */);

    // ---- delete some blobs and trigger GC + verify vchunk/pchunk consistency ----
    std::map< shard_id_t, std::set< blob_id_t > > to_delete;
    const auto delete_count = num_blobs_per_shard / 2; // delete first half
    for (blob_id_t blob_id = 0; blob_id < delete_count; ++blob_id) {
        to_delete[shard1.id].insert(blob_id);
    }
    del_blobs(pg_id, to_delete);
    g_helper->sync();

    // trigger GC on the shard's pchunk
    auto pchunk_opt = _obj_inst->get_shard_p_chunk_id(shard1.id);
    ASSERT_TRUE(pchunk_opt.has_value()) << "Failed to get shard pchunk id";
    auto chunk_id = pchunk_opt.value();

    auto gc_mgr = _obj_inst->gc_manager();
    auto gc_fut = gc_mgr->submit_gc_task(task_priority::normal, chunk_id);
    bool gc_ok = std::move(gc_fut).get();
    ASSERT_TRUE(gc_ok) << "GC task failed on pchunk=" << chunk_id;

    g_helper->sync();

    // After GC, the old pchunk may be a reserved/orphaned chunk; get the live pchunk now.
    auto new_pchunk_opt = _obj_inst->get_shard_p_chunk_id(shard1.id);
    ASSERT_TRUE(new_pchunk_opt.has_value()) << "Failed to get shard pchunk id after GC";
    auto p_chunk_id_after_gc = new_pchunk_opt.value();

    // verify vchunk/pchunk consistency using the live pchunk
    auto chunk_selector = _obj_inst->chunk_selector();
    auto EXVchunk = chunk_selector->get_extend_vchunk(p_chunk_id_after_gc);
    ASSERT_TRUE(EXVchunk != nullptr) << "Failed to get extend vchunk";
    ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value()) << "Missing vchunk id";
    auto vchunk_id = EXVchunk->m_v_chunk_id.value();

    auto shard_v_chunk_id_opt = _obj_inst->get_shard_v_chunk_id(shard1.id);
    ASSERT_TRUE(shard_v_chunk_id_opt.has_value()) << "Failed to get shard vchunk id after GC";
    auto shard_vchunk_id = shard_v_chunk_id_opt.value();
    ASSERT_EQ(vchunk_id, shard_vchunk_id) << "shard's vchunk id mismatch after GC";

    auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
    ASSERT_EQ(pg_chunks->at(vchunk_id), p_chunk_id_after_gc)
        << "vchunk->pchunk mapping inconsistent: vchunk=" << vchunk_id << " pchunk=" << p_chunk_id_after_gc;

    ASSERT_TRUE(EXVchunk->m_pg_id.has_value()) << "Missing pg_id in EXVchunk";
    ASSERT_EQ(EXVchunk->m_pg_id.value(), pg_id) << "pg_id mismatch in EXVchunk";

    // verify all remaining blob's pchunk matches the live pchunk via index table
    const auto remaining_start = delete_count;
    auto index_table = _obj_inst->get_index_table(pg_id);
    ASSERT_NE(index_table, nullptr) << "Failed to get index table for pg=" << pg_id;
    for (blob_id_t blob_id = remaining_start; blob_id < num_blobs_per_shard; ++blob_id) {
        auto pbas_result = _obj_inst->get_blob_from_index_table(index_table, shard1.id, blob_id);
        ASSERT_TRUE(pbas_result.hasValue()) << "Failed to get blob pchunk for blob_id=" << blob_id << " after GC";
        ASSERT_EQ(pbas_result.value().chunk_num(), p_chunk_id_after_gc)
            << "Blob pchunk mismatch: blob_id=" << blob_id << " expected pchunk=" << p_chunk_id_after_gc
            << " actual=" << pbas_result.value().chunk_num();
    }

    LOGINFO("[StaleBlobRouteAfterSealAndGC] vchunk/pchunk consistency verified after GC delete");

    g_helper->sync();
}
#endif // _PRERELEASE