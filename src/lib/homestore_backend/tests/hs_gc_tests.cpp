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
                auto derived_shard_id =
                    make_new_shard_id(pg_id, shard_seq); // shard id start from 1
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
                total_blob_occupied_blk_count += 2; /*header and footer*/
                for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                    total_blob_occupied_blk_count += blk_count;
                }
            }
            // check pg durable entities
            ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);

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
            total_blob_occupied_blk_count += 2; /*header and footer*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }

        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, total_blob_occupied_blk_count);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
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
            total_blob_occupied_blk_count += 2; /*header and footer*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }
        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, total_blob_occupied_blk_count);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
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