#include "homeobj_fixture.hpp"

#include "lib/homestore_backend/index_kv.hpp"
#include "generated/resync_blob_data_generated.h"

#include <homestore/replication_service.hpp>

TEST_F(HomeObjectFixture, BasicEquivalence) {
    auto shard_mgr = _obj_inst->shard_manager();
    auto pg_mgr = _obj_inst->pg_manager();
    auto blob_mgr = _obj_inst->blob_manager();
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(shard_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(pg_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(blob_mgr.get()));
}

TEST_F(HomeObjectFixture, BasicPutGetDelBlobWRestart) {
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
    constexpr pg_id_t pg_id{1};
    // Generate test data
    uint64_t num_shards_per_pg = 3;
    uint64_t num_blobs_per_shard = 5;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;

    auto& shard_list = pg_shard_id_vec[pg_id];
    create_pg(pg_id);
    for (uint64_t i = 0; i < num_shards_per_pg; i++) {
        auto shard = create_shard(pg_id, 64 * Mi);
        shard_list.emplace_back(shard.id);
        pg_blob_id[i] = 0;
        LOGINFO("pg {} shard {}", pg_id, shard.id);
    }
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    PG* pg;
    {
        auto lg = std::shared_lock(_obj_inst->_pg_lock);
        auto iter = _obj_inst->_pg_map.find(pg_id);
        ASSERT_TRUE(iter != _obj_inst->_pg_map.end());
        pg = iter->second.get();
    }

    //Construct shards as [sealed, open, filtered]
    seal_shard(pg->shards_.front()->info.id);
    ASSERT_EQ(pg->shards_.front()->info.state, homeobject::ShardInfo::State::SEALED);
    //Filter out the last shard
    auto snp_lsn = pg->shards_.back()->info.lsn - 1;
    //Delete some blobs: delete the first blob of each shard
    blob_id_t current_blob_id{0};
    for (auto& shard : pg->shards_) {
        del_blob(pg->pg_info_.id, shard->info.id, current_blob_id);
        current_blob_id += num_blobs_per_shard;
    }

    auto pg_iter =
        std::make_shared< HSHomeObject::PGBlobIterator >(*_obj_inst, pg->pg_info_.replica_set_uuid, snp_lsn);
    ASSERT_EQ(pg_iter->shard_list_.size(), num_shards_per_pg - 1);
    // Created blob sizes are distributed in range (1, 16kb)
    pg_iter->max_batch_size_ = 16 * 1024;

    // Verify PG meta data
    sisl::io_blob_safe meta_blob;
    pg_iter->create_pg_snapshot_data(meta_blob);
    ASSERT_TRUE(meta_blob.size() > 0);

    SyncMessageHeader* header = r_cast< SyncMessageHeader* >(meta_blob.bytes());
    ASSERT_EQ(header->msg_type, SyncMessageType::PG_META);
    auto pg_msg = GetSizePrefixedResyncPGMetaData(meta_blob.cbytes() + sizeof(SyncMessageHeader));
    ASSERT_EQ(pg_msg->pg_id(), pg->pg_info_.id);
    auto u1 = pg_msg->replica_set_uuid();
    auto u2 = pg->pg_info_.replica_set_uuid;
    ASSERT_EQ(std::string(u1->begin(), u1->end()), std::string(u2.begin(), u2.end()));
    ASSERT_EQ(pg_msg->pg_size(), pg->pg_info_.size);
    ASSERT_EQ(pg_msg->chunk_size(), pg->pg_info_.chunk_size);
    ASSERT_EQ(pg_msg->blob_seq_num(), pg->durable_entities().blob_sequence_num.load());
    ASSERT_EQ(pg_msg->shard_seq_num(), pg->shard_sequence_num_);

    auto msg_members = pg_msg->members();
    ASSERT_EQ(msg_members->size(), pg->pg_info_.members.size());
    for (auto m : *msg_members) {
        uuids::uuid id{};
        std::copy_n(m->uuid()->data(), 16, id.begin());
        auto it = pg->pg_info_.members.find(PGMember{id});
        ASSERT_TRUE(it != pg->pg_info_.members.end());
        ASSERT_EQ(m->name()->str(), it->name);
        ASSERT_EQ(m->priority(), it->priority);
    }

    auto idx = 0;
    ASSERT_EQ(pg->shards_.size()-1, pg_msg->shard_ids()->size());
    for (auto& shard : pg->shards_) {
        if (shard->info.lsn > snp_lsn) { continue; }
        ASSERT_EQ(shard->info.id, pg_msg->shard_ids()->Get(idx++));
    }

    //Verify shard meta data
    current_blob_id = 0;
    for (auto& shard : pg->shards_) {
        auto shard_seq_num = HSHomeObject::get_sequence_num_from_shard_id(shard->info.id);
        auto batch_id = 0;
        objId oid(shard_seq_num, batch_id++);
        if (shard->info.lsn > snp_lsn) {
            ASSERT_FALSE(pg_iter->update_cursor(oid));
            continue;
        }
        LOGINFO("shard meta, oid {}", oid.to_string());
        ASSERT_TRUE(pg_iter->update_cursor(oid));
        ASSERT_TRUE(pg_iter->generate_shard_blob_list());
        ASSERT_EQ(pg_iter->cur_blob_list_.size(), num_blobs_per_shard);
        sisl::io_blob_safe meta_data;
        ASSERT_TRUE(pg_iter->create_shard_snapshot_data(meta_data));

        SyncMessageHeader* header = r_cast< SyncMessageHeader* >(meta_data.bytes());
        ASSERT_EQ(header->msg_type, SyncMessageType::SHARD_META);
        auto shard_msg = GetSizePrefixedResyncShardMetaData(meta_data.cbytes() + sizeof(SyncMessageHeader));
        ASSERT_EQ(shard_msg->shard_id(), shard->info.id);
        ASSERT_EQ(shard_msg->pg_id(), pg->pg_info_.id);
        ASSERT_EQ(shard_msg->state(), static_cast<uint8_t>(shard->info.state));
        ASSERT_EQ(shard_msg->created_lsn(), shard->info.lsn);
        ASSERT_EQ(shard_msg->created_time(), shard->info.created_time);
        ASSERT_EQ(shard_msg->last_modified_time(), shard->info.last_modified_time);
        ASSERT_EQ(shard_msg->total_capacity_bytes(), shard->info.total_capacity_bytes);

        //Verify blob data
        uint64_t packed_blob_size{0};
        auto is_finished = false;
        //skip the first blob(deleted) of the shard
        current_blob_id++;
        while (!is_finished) {
            oid = objId(shard_seq_num, batch_id++);
            ASSERT_TRUE(pg_iter->update_cursor(oid));
            sisl::io_blob_safe blob_batch;
            ASSERT_TRUE(pg_iter->create_blobs_snapshot_data(blob_batch));
            header = r_cast< SyncMessageHeader* >(blob_batch.bytes());
            ASSERT_EQ(header->msg_type, SyncMessageType::SHARD_BATCH);
            auto blob_msg = GetSizePrefixedResyncBlobDataBatch(blob_batch.cbytes() + sizeof(SyncMessageHeader));
            LOGINFO("blob batch, oid {}, blob_cnt {}", oid.to_string(), blob_msg->blob_list()->size());
            for (auto i = 0; i < static_cast< int >(blob_msg->blob_list()->size()); i++) {
                auto b = blob_msg->blob_list()->Get(i);
                ASSERT_EQ(b->blob_id(), current_blob_id++);
                ASSERT_EQ(b->state(), static_cast<uint8_t>(ResyncBlobState::NORMAL));
                auto blob_data = b->data()->Data();
                auto header = r_cast< HSHomeObject::BlobHeader const* >(blob_data);
                ASSERT_TRUE(header->valid());
                auto g = _obj_inst->blob_manager()->get(shard->info.id, b->blob_id(), 0, 0).get();
                ASSERT_TRUE(!!g);
                auto result = std::move(g.value());
                EXPECT_EQ(result.body.size(), header->blob_size);
                ASSERT_TRUE(
                    memcmp(result.body.cbytes(), blob_data+header->data_offset, header->blob_size) == 0);
                packed_blob_size++;
                LOGDEBUG(
                    "[{}]Get blob pg {}, shard {}, blob {}, data_len {}, blob_len {}, header_len {}, user_key_len {}, data {}",
                    packed_blob_size, pg->pg_info_.id, shard->info.id, b->blob_id(),
                    b->data()->size(), header->blob_size, sizeof(HSHomeObject::BlobHeader), header->user_key_size,
                    hex_bytes(result.body.cbytes(), 5));
            }
            is_finished = blob_msg->is_last_batch();
        }
        ASSERT_EQ(packed_blob_size, num_blobs_per_shard-1);
    }
    //Verify last obj
    ASSERT_TRUE(pg_iter->update_cursor(objId(LAST_OBJ_ID)));
}

TEST_F(HomeObjectFixture, SnapshotReceiveHandler) {
    constexpr uint64_t snp_lsn = 1;
    constexpr uint64_t num_shards_per_pg = 3;
    constexpr uint64_t num_open_shards_per_pg = 2; // Should be less than num_shards_per_pg
    constexpr uint64_t num_batches_per_shard = 5;
    constexpr uint64_t num_blobs_per_batch = 5;
    constexpr int corrupted_blob_percentage = 10;             // Percentage of blobs with state = CORRUPTED
    constexpr int unexpected_corrupted_batch_percentage = 15; // Percentage of batches with unexpected data corruption

    // We have to create a PG first to init repl_dev
    constexpr pg_id_t pg_id = 1;
    create_pg(pg_id); // to create repl dev
    auto iter = _obj_inst->_pg_map.find(pg_id);
    ASSERT_TRUE(iter != _obj_inst->_pg_map.end());
    auto pg = iter->second.get();
    PGStats stats;
    ASSERT_TRUE(_obj_inst->pg_manager()->get_stats(pg_id, stats));
    auto r_dev = homestore::HomeStore::instance()->repl_service().get_repl_dev(stats.replica_set_uuid);
    ASSERT_TRUE(r_dev.hasValue());

    auto handler = std::make_unique< homeobject::HSHomeObject::SnapshotReceiveHandler >(*_obj_inst, r_dev.value());
    handler->reset_context(snp_lsn, pg_id);

    // Step 1: Test write pg meta - cannot test full logic since the PG already exists
    // Generate ResyncPGMetaData message
    LOGINFO("TESTING: applying meta for pg {}", pg_id);
    constexpr auto blob_seq_num = num_shards_per_pg * num_batches_per_shard * num_blobs_per_batch;
    flatbuffers::FlatBufferBuilder builder;
    std::vector< flatbuffers::Offset< Member > > members;
    std::vector uuid(stats.replica_set_uuid.begin(), stats.replica_set_uuid.end());
    for (auto& member : stats.members) {
        auto id = std::vector< std::uint8_t >(std::get< 0 >(member).begin(), std::get< 0 >(member).end());
        members.push_back(CreateMemberDirect(builder, &id, std::get< 1 >(member).c_str(), 100));
    }
    std::vector< uint64_t > shard_ids;
    for (uint64_t i = 1; i <= num_shards_per_pg; i++) {
        shard_ids.push_back(i);
    }
    auto pg_entry =
        CreateResyncPGMetaDataDirect(builder, pg_id, &uuid, pg->pg_info_.size, pg->pg_info_.chunk_size, blob_seq_num, num_shards_per_pg, &members, &shard_ids);
    builder.Finish(pg_entry);
    auto pg_meta = GetResyncPGMetaData(builder.GetBufferPointer());
    auto ret = handler->process_pg_snapshot_data(*pg_meta);
    ASSERT_EQ(ret, 0);
    builder.Reset();

    // Step 2: Test shard and blob batches
    std::random_device rd; // Random generators for blob corruption
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> corrupt_dis(1, 100);
    std::uniform_int_distribution<> random_bytes_dis(1, 16 * 1024);

    blob_id_t cur_blob_id{0};
    for (uint64_t i = 1; i <= num_shards_per_pg; i++) {
        LOGINFO("TESTING: applying meta for shard {}", i);

        // Step 2-1: Test write shard meta
        // Generate ResyncShardMetaData message
        ShardInfo shard;
        shard.id = i;
        shard.state = i <= num_shards_per_pg - num_open_shards_per_pg
            ? ShardInfo::State::SEALED
            : ShardInfo::State::OPEN; // Open shards arrive at last
        shard.created_time = get_time_since_epoch_ms();
        shard.last_modified_time = shard.created_time;
        shard.total_capacity_bytes = 1024 * Mi;
        shard.lsn = snp_lsn;

        auto v_chunk_id = _obj_inst->chunk_selector()->get_most_available_blk_chunk(pg_id);

        auto shard_entry = CreateResyncShardMetaData(builder, shard.id, pg_id, static_cast< uint8_t >(shard.state),
                                                     shard.lsn, shard.created_time, shard.last_modified_time,
                                                     shard.total_capacity_bytes, v_chunk_id.value());
        builder.Finish(shard_entry);
        auto shard_meta = GetResyncShardMetaData(builder.GetBufferPointer());
        auto ret = handler->process_shard_snapshot_data(*shard_meta);
        builder.Reset();
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(handler->get_shard_cursor(), shard.id);
        ASSERT_EQ(handler->get_next_shard(),
                  i == num_shards_per_pg ? HSHomeObject::SnapshotReceiveHandler::shard_list_end_marker : i + 1);

        auto res = _obj_inst->shard_manager()->get_shard(shard.id).get();
        ASSERT_TRUE(!!res);
        auto shard_res = std::move(res.value());
        ASSERT_EQ(shard_res.id, shard.id);
        ASSERT_EQ(shard_res.state, shard.state);
        ASSERT_EQ(shard_res.created_time, shard.created_time);
        ASSERT_EQ(shard_res.last_modified_time, shard.last_modified_time);
        ASSERT_EQ(shard_res.total_capacity_bytes, shard.total_capacity_bytes);
        ASSERT_EQ(shard_res.lsn, shard.lsn);

        // Step 2-2: Test write blob batch data
        // Generate ResyncBlobDataBatch message
        std::map< blob_id_t, std::tuple< Blob, bool > > blob_map;
        for (uint64_t j = 1; j <= num_batches_per_shard; j++) {
            // Don't test unexpected corruption on the last batch, since for simplicity we're not simulating resending
            bool is_corrupted_batch =
                j < num_batches_per_shard && corrupt_dis(gen) <= unexpected_corrupted_batch_percentage;
            LOGINFO("TESTING: applying blobs for shard {} batch {}, is_corrupted {}", shard.id, j, is_corrupted_batch);
            std::vector< flatbuffers::Offset< ResyncBlobData > > blob_entries;
            for (uint64_t k = 0; k < num_blobs_per_batch; k++) {
                auto blob_state = corrupt_dis(gen) <= corrupted_blob_percentage ? ResyncBlobState::CORRUPTED
                                                                                : ResyncBlobState::NORMAL;

                // Construct raw blob buffer
                auto blob = build_blob(cur_blob_id);
                const auto aligned_hdr_size =
                    sisl::round_up(sizeof(HSHomeObject::BlobHeader) + blob.user_key.size(), io_align);
                sisl::io_blob_safe blob_raw(aligned_hdr_size + blob.body.size(), io_align);
                HSHomeObject::BlobHeader hdr;
                hdr.type = HSHomeObject::DataHeader::data_type_t::BLOB_INFO;
                hdr.shard_id = shard.id;
                hdr.blob_id = cur_blob_id;
                hdr.hash_algorithm = HSHomeObject::BlobHeader::HashAlgorithm::CRC32;
                hdr.blob_size = blob.body.size();
                hdr.user_key_size = blob.user_key.size();
                hdr.object_offset = blob.object_off;
                hdr.data_offset = aligned_hdr_size;
                _obj_inst->compute_blob_payload_hash(hdr.hash_algorithm, blob.body.cbytes(), blob.body.size(),
                                                     reinterpret_cast< uint8_t* >(blob.user_key.data()),
                                                     blob.user_key.size(), hdr.hash,
                                                     HSHomeObject::BlobHeader::blob_max_hash_len);

                std::memcpy(blob_raw.bytes(), &hdr, sizeof(HSHomeObject::BlobHeader));
                if (!blob.user_key.empty()) {
                    std::memcpy((blob_raw.bytes() + sizeof(HSHomeObject::BlobHeader)), blob.user_key.data(),
                                blob.user_key.size());
                }
                std::memcpy(blob_raw.bytes() + aligned_hdr_size, blob.body.cbytes(), blob.body.size());

                // Simulate blob data corruption - tamper with random bytes
                if (is_corrupted_batch || blob_state == ResyncBlobState::CORRUPTED) {
                    LOGINFO("Simulating corrupted blob data for shard {} blob {}", shard.id, cur_blob_id);
                    constexpr int corrupted_bytes = 5;
                    for (auto i = 0; i < corrupted_bytes; i++) {
                        auto offset = random_bytes_dis(gen) % blob_raw.size();
                        auto byte = random_bytes_dis(gen) % 256;
                        blob_raw.bytes()[offset] = byte;
                        LOGINFO("Changing byte at offset {} to simulate data corruption", offset, byte);
                    }
                }

                std::vector data(blob_raw.bytes(), blob_raw.bytes() + blob_raw.size());
                blob_entries.push_back(
                    CreateResyncBlobDataDirect(builder, cur_blob_id, static_cast< uint8_t >(blob_state), &data));
                if (!is_corrupted_batch) {
                    blob_map[cur_blob_id] =
                        std::make_tuple< Blob, bool >(std::move(blob), blob_state == ResyncBlobState::CORRUPTED);
                }
                cur_blob_id++;
            }
            builder.Finish(CreateResyncBlobDataBatchDirect(builder, &blob_entries, true));
            auto blob_batch = GetResyncBlobDataBatch(builder.GetBufferPointer());
            ret = handler->process_blobs_snapshot_data(*blob_batch, j, j == num_batches_per_shard);
            if (is_corrupted_batch) {
                ASSERT_NE(ret, 0);
            } else {
                ASSERT_EQ(ret, 0);
            }
            builder.Reset();
            ASSERT_EQ(handler->get_shard_cursor(), shard.id);
            ASSERT_EQ(handler->get_next_shard(),
                      i == num_shards_per_pg ? HSHomeObject::SnapshotReceiveHandler::shard_list_end_marker : i + 1);
        }

        // Verify blobs
        for (const auto& b : blob_map) {
            auto blob_id = b.first;
            auto& blob = std::get< 0 >(b.second);
            auto is_corrupted = std::get< 1 >(b.second);

            auto res = _obj_inst->blob_manager()->get(shard.id, blob_id, 0, blob.body.size()).get();
            if (is_corrupted) {
                ASSERT_FALSE(!!res);
            } else {
                ASSERT_TRUE(!!res);
                auto blob_res = std::move(res.value());
                ASSERT_EQ(blob_res.body.size(), blob.body.size());
                ASSERT_EQ(std::memcmp(blob_res.body.bytes(), blob.body.cbytes(), blob_res.body.size()), 0);
            }
        }

        // Verify chunk of sealed shards are successfully released
        if (shard.state == ShardInfo::State::SEALED) {
            auto v = _obj_inst->get_shard_v_chunk_id(shard.id);
            ASSERT_TRUE(v.has_value());
            ASSERT_EQ(v.value(), v_chunk_id.value());
            ASSERT_TRUE(_obj_inst->chunk_selector()->is_chunk_available(pg_id, v.value()));
        }
    }
}
