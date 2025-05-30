#include "homeobj_fixture.hpp"

TEST_F(HomeObjectFixture, BasicGC) {
    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;
    auto num_blobs_per_shard = 10 * SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    std::map< pg_id_t, HSHomeObject::HS_PG* > HS_PG_map;
    std::map< shard_id_t, std::pair< homestore::chunk_num_t, homestore::chunk_num_t > > shard_to_v_p_chunk;
    auto chunk_selector = _obj_inst->chunk_selector();

    for (uint16_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        // do not use HS_PG_map[i] to change anything, const cast just for compiling
        HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
        pg_blob_id[i] = 0;
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = create_shard(i, 64 * Mi);
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    auto shard_blob_ids_map = put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // seal all shards and check
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
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

            shard_to_v_p_chunk[shard_id] = std::make_pair(vchunk_id, chunk_id);
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
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
    }

    // check pg index table, the valid blob index count should be equal to the blob count
    for (const auto& [pg_id, _] : pg_blob_id) {
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id]);
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

    bool all_deleted_blobs_have_been_gc{true};

    // wait until all the deleted blobs are gc
    while (true) {
        for (const auto& [shard_id, blob_to_blk_count] : shard_blob_ids_map) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();
            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            const auto available_blk = EXVchunk->available_blks();
            const auto total_blks = EXVchunk->get_total_blks();

            // now, the chunk state is not determined, maybe GC(being gc) or AVAILABLE(complete gc)
            uint32_t used_blks{2}; /* header and footer */

            for (const auto& [_, blk_count] : blob_to_blk_count) {
                used_blks += blk_count;
            }

            if (available_blk != total_blks - used_blks) {
                LOGINFO("shard={}, chunk_id={}, available_blk={}, total_blk={}, used_blks={}, waiting for gc", shard_id,
                        chunk_id, available_blk, total_blks, used_blks);
                all_deleted_blobs_have_been_gc = false;
                break;
            }
        }
        if (all_deleted_blobs_have_been_gc) break;
        all_deleted_blobs_have_been_gc = true;
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    // after half blobs have been deleted, the tombstone indexes(half of the total blobs) have been removed by gc
    for (const auto& [pg_id, _] : pg_blob_id) {
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id] / 2);
    }

    // verify blob data after gc
    std::map< shard_id_t, std::vector< blob_id_t > > remaining_shard_blobs;
    for (const auto& [shard_id, blob_to_blk_count] : shard_blob_ids_map) {
        for (const auto& [blob_id, _] : blob_to_blk_count) {
            remaining_shard_blobs[shard_id].push_back(blob_id);
        }
    }
    verify_shard_blobs(remaining_shard_blobs);

    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();

            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            ASSERT_TRUE(EXVchunk != nullptr);
            ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
            auto vchunk_id = EXVchunk->m_v_chunk_id.value();

            // after gc , pg_chunks should changes, the vchunk shoud change to a new pchunk.
            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

            // after gc, vchunk id of a shard should not change, but pchunk id of shard should change.
            ASSERT_EQ(vchunk_id, shard_to_v_p_chunk[shard_id].first);
            ASSERT_NE(chunk_id, shard_to_v_p_chunk[shard_id].second);
        }
    }

    // not use again
    shard_to_v_p_chunk.clear();

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
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
    }

    // delete remaining blobs
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map_for_deletion;
        for (const auto& shard_id : shard_vec) {
            shard_blob_ids_map_for_deletion[shard_id];
            auto& blob_to_blk_count = shard_blob_ids_map[shard_id];
            for (const auto& [blob_id, _] : blob_to_blk_count) {
                shard_blob_ids_map_for_deletion[shard_id].insert(blob_id);
            }
        }
        del_blobs(pg_id, shard_blob_ids_map_for_deletion);
    }

    // wait until all the deleted blobs are gc
    while (true) {
        for (const auto& [shard_id, blob_to_blk_count] : shard_blob_ids_map) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();
            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            const auto available_blk = EXVchunk->available_blks();
            const auto total_blks = EXVchunk->get_total_blks();

            if (available_blk != total_blks) {
                LOGINFO("shard={}, chunk_id={}, available_blk={}, total_blk={}, waiting for gc", shard_id, chunk_id,
                        available_blk, total_blks);
                all_deleted_blobs_have_been_gc = false;
                break;
            }
        }
        if (all_deleted_blobs_have_been_gc) break;
        all_deleted_blobs_have_been_gc = true;
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    // after all blobs have been deleted, the pg index table should be empty
    for (const auto& [pg_id, _] : pg_blob_id) {
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), 0);
    }

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, 0);
    }

    // if all blobs of shard are deleted , the shard should be deleted.
    // TODO:add more check after we have delete shard implementation
}

TEST_F(HomeObjectFixture, BasicEGC) {
    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;
    auto num_blobs_per_shard = 10 * SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    std::map< pg_id_t, HSHomeObject::HS_PG* > HS_PG_map;
    std::map< shard_id_t, std::pair< homestore::chunk_num_t, homestore::chunk_num_t > > shard_to_v_p_chunk;
    auto chunk_selector = _obj_inst->chunk_selector();

    for (uint16_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        // do not use HS_PG_map[i] to change anything, const cast just for compiling
        HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
        pg_blob_id[i] = 0;
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = create_shard(i, 64 * Mi);
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    auto shard_blob_ids_map = put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // check and build shard to vchunk/pchunk map
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
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
            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

            shard_to_v_p_chunk[shard_id] = std::make_pair(vchunk_id, chunk_id);
        }
    }

    // check pg index table, the valid blob index count should be equal to the blob count
    for (const auto& [pg_id, _] : pg_blob_id) {
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id]);
    }

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        uint64_t total_blob_occupied_blk_count{0};
        const auto& shard_vec = pg_shard_id_vec[pg_id];
        for (const auto& shard_id : shard_vec) {
            total_blob_occupied_blk_count += 1; /*header*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
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

    // do not seal shard and trigger gc mannually to simulate emergent gc
    auto gc_mgr = _obj_inst->gc_manager();

    // trigger egc for all shards
    std::vector< folly::SemiFuture< bool > > futs;
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();
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

    // veriy the block count in each chunk which holds a shard
    for (const auto& [shard_id, blob_to_blk_count] : shard_blob_ids_map) {
        auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
        ASSERT_TRUE(chunk_opt.has_value());
        auto chunk_id = chunk_opt.value();
        auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
        ASSERT_EQ(EXVchunk->m_state, ChunkState::INUSE);
        const auto available_blk = EXVchunk->available_blks();
        const auto total_blks = EXVchunk->get_total_blks();

        uint32_t used_blks{1}; /* only header*/

        for (const auto& [_, blk_count] : blob_to_blk_count) {
            used_blks += blk_count;
        }

        ASSERT_EQ(available_blk, total_blks - used_blks);
    }

    // after half blobs have been deleted, the tombstone indexes(half of the total blobs) have been removed by gc
    for (const auto& [pg_id, _] : pg_blob_id) {
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id] / 2);
    }

    // verify blob data after gc
    std::map< shard_id_t, std::vector< blob_id_t > > remaining_shard_blobs;
    for (const auto& [shard_id, blob_to_blk_count] : shard_blob_ids_map) {
        for (const auto& [blob_id, _] : blob_to_blk_count) {
            remaining_shard_blobs[shard_id].push_back(blob_id);
        }
    }
    verify_shard_blobs(remaining_shard_blobs);

    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
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

            // after gc, vchunk id of a shard should not change, but pchunk id of shard should change.
            ASSERT_EQ(vchunk_id, shard_to_v_p_chunk[shard_id].first);
            ASSERT_NE(chunk_id, shard_to_v_p_chunk[shard_id].second);
            shard_to_v_p_chunk[shard_id].second = chunk_id; // update the pchunk id after gc
        }
    }

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        uint64_t total_blob_occupied_blk_count{0};
        const auto& shard_vec = pg_shard_id_vec[pg_id];
        for (const auto& shard_id : shard_vec) {
            total_blob_occupied_blk_count += 1; /*header and footer*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
    }

    // delete remaining blks
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map_for_deletion;
        for (const auto& shard_id : shard_vec) {
            shard_blob_ids_map_for_deletion[shard_id];
            auto& blob_to_blk_count = shard_blob_ids_map[shard_id];
            for (const auto& [blob_id, _] : blob_to_blk_count) {
                shard_blob_ids_map_for_deletion[shard_id].insert(blob_id);
            }
        }
        del_blobs(pg_id, shard_blob_ids_map_for_deletion);
    }

    // trigger egc for all shards
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();
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

    // after all blobs have been deleted, the pg index table should be empty
    for (const auto& [pg_id, _] : pg_blob_id) {
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), 0);
    }

    for (const auto& [shard_id, _] : shard_blob_ids_map) {
        auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
        ASSERT_TRUE(chunk_opt.has_value());
        auto chunk_id = chunk_opt.value();
        auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
        const auto available_blk = EXVchunk->available_blks();
        const auto total_blks = EXVchunk->get_total_blks();

        // the shard is not sealed, so there is only the shard header for each shard
        ASSERT_EQ(available_blk + 1, total_blks);
    }

    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
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

            // after gc, vchunk id of a shard should not change, but pchunk id of shard should change.
            ASSERT_EQ(vchunk_id, shard_to_v_p_chunk[shard_id].first);
            ASSERT_NE(chunk_id, shard_to_v_p_chunk[shard_id].second);
        }
    }

    // not use again
    shard_to_v_p_chunk.clear();

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        // only shard header left, so equal to the num of shards in this pg
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, pg_shard_id_vec[pg_id].size());
    }

    // TODO:: add more check after we have delete shard implementation
}