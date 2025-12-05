#include "homeobj_fixture.hpp"
#include "generated/resync_blob_data_generated.h"
#include <homestore/replication_service.hpp>

// CP related tests
TEST_F(HomeObjectFixture, HSHomeObjectCPTestBasic) {
    // Step-1: create a PG and a shard
    std::vector< std::pair< pg_id_t, shard_id_t > > pg_shard_id_vec;
    create_pg(1 /* pg_id */);
    auto shard_info = create_shard(1 /* pg_id */, 64 * Mi, "shard meta");
    pg_shard_id_vec.emplace_back(1 /* pg_id */, shard_info.id);
    LOGINFO("pg={} shard {}", 1, shard_info.id);
    {
        // Step-2: write some dirty pg information and add to dirt list;
        auto lg = std::unique_lock(_obj_inst->_pg_lock);
        for (auto& [_, pg] : _obj_inst->_pg_map) {
            auto hs_pg = static_cast< HSHomeObject::HS_PG* >(pg.get());
            hs_pg->durable_entities_.blob_sequence_num = 54321; // fake some random blob seq number to make it dirty;
            hs_pg->is_dirty_.store(true);

            // test multiple update to the dirty list;
            // only the last update should be kept;
            hs_pg->durable_entities_.blob_sequence_num = 12345; // fake some random blob seq number to make it dirty;
            hs_pg->is_dirty_.store(true);
        }
    }

    restart();

    EXPECT_TRUE(_obj_inst->_pg_map.size() == 1);
    {
        auto lg = std::shared_lock(_obj_inst->_pg_lock);
        for (auto& [_, pg] : _obj_inst->_pg_map) {
            auto hs_pg = static_cast< HSHomeObject::HS_PG* >(pg.get());
            EXPECT_EQ(hs_pg->durable_entities_.blob_sequence_num, 12345);
        }
    }
}

// Snapshot resync related tests
TEST_F(HomeObjectFixture, PGBlobIterator) {
    constexpr pg_id_t pg_id{1};
    // Generate test data
    // Construct shards as [sealed, empty, open, filtered]
    uint64_t num_shards_per_pg = 4;
    uint64_t empty_shard_seq = 2;
    uint64_t num_blobs_per_shard = 5;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;

    auto& shard_list = pg_shard_id_vec[pg_id];
    create_pg(pg_id);
    for (uint64_t i = 0; i < num_shards_per_pg; i++) {
        auto shard = create_shard(pg_id, 64 * Mi, "shard meta" + std::to_string(i));
        if (i != empty_shard_seq - 1) { shard_list.emplace_back(shard.id); }
        LOGINFO("pg={} shard {}", pg_id, shard.id);
    }
    pg_blob_id[pg_id] = 0;
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    auto pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(pg != nullptr);

    seal_shard(pg->shards_.front()->info.id);
    ASSERT_EQ(pg->shards_.front()->info.state, homeobject::ShardInfo::State::SEALED);
    // Filter out the last shard
    auto snp_lsn = pg->shards_.back()->info.lsn - 1;
    // Delete some blobs: delete the first blob of each shard
    blob_id_t current_blob_id{0};
    for (auto& shard : shard_list) {
        del_blob(pg->pg_info_.id, shard, current_blob_id);
        current_blob_id += num_blobs_per_shard;
    }

    auto pg_iter = std::make_shared< HSHomeObject::PGBlobIterator >(*_obj_inst, pg->pg_info_.replica_set_uuid, snp_lsn);
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
    ASSERT_EQ(pg->shards_.size() - 1, pg_msg->shard_ids()->size());
    for (auto& shard : pg->shards_) {
        if (shard->info.lsn > snp_lsn) { continue; }
        ASSERT_EQ(shard->info.id, pg_msg->shard_ids()->Get(idx++));
    }

    // Verify shard meta data
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
        if (shard_seq_num != empty_shard_seq) {
            ASSERT_EQ(pg_iter->cur_blob_list_.size(), num_blobs_per_shard);
        } else {
            ASSERT_EQ(pg_iter->cur_blob_list_.size(), 0);
        }

        sisl::io_blob_safe meta_data;
        ASSERT_TRUE(pg_iter->create_shard_snapshot_data(meta_data));

        SyncMessageHeader* msg_hdr = r_cast< SyncMessageHeader* >(meta_data.bytes());
        ASSERT_EQ(msg_hdr->msg_type, SyncMessageType::SHARD_META);
        auto shard_msg = GetSizePrefixedResyncShardMetaData(meta_data.cbytes() + sizeof(SyncMessageHeader));
        ASSERT_EQ(shard_msg->shard_id(), shard->info.id);
        ASSERT_EQ(shard_msg->pg_id(), pg->pg_info_.id);
        ASSERT_EQ(shard_msg->state(), static_cast< uint8_t >(shard->info.state));
        ASSERT_EQ(shard_msg->created_lsn(), shard->info.lsn);
        ASSERT_EQ(shard_msg->created_time(), shard->info.created_time);
        ASSERT_EQ(shard_msg->last_modified_time(), shard->info.last_modified_time);
        ASSERT_EQ(shard_msg->total_capacity_bytes(), shard->info.total_capacity_bytes);
        EXPECT_TRUE(std::memcmp(shard_msg->meta()->data(), shard->info.meta, ShardInfo::meta_length) == 0);

        // Verify blob data
        uint64_t packed_blob_size{0};
        auto is_finished = false;
        if (shard_seq_num != empty_shard_seq) {
            // Skip the first blob(deleted) of the shard
            current_blob_id++;
        }
        while (!is_finished) {
            oid = objId(shard_seq_num, batch_id++);
            ASSERT_TRUE(pg_iter->update_cursor(oid));
            sisl::io_blob_safe blob_batch;
            ASSERT_TRUE(pg_iter->create_blobs_snapshot_data(blob_batch));
            msg_hdr = r_cast< SyncMessageHeader* >(blob_batch.bytes());
            ASSERT_EQ(msg_hdr->msg_type, SyncMessageType::SHARD_BATCH);
            auto blob_msg = GetSizePrefixedResyncBlobDataBatch(blob_batch.cbytes() + sizeof(SyncMessageHeader));
            LOGINFO("blob batch, oid {}, blob_cnt {}", oid.to_string(), blob_msg->blob_list()->size());
            for (auto i = 0; i < static_cast< int >(blob_msg->blob_list()->size()); i++) {
                auto b = blob_msg->blob_list()->Get(i);
                ASSERT_EQ(b->blob_id(), current_blob_id++);
                ASSERT_EQ(b->state(), static_cast< uint8_t >(ResyncBlobState::NORMAL));
                auto blob_data = b->data()->Data();
                auto blob_header = r_cast< HSHomeObject::BlobHeader const* >(blob_data);
                ASSERT_TRUE(blob_header->valid());
                auto g = _obj_inst->blob_manager()->get(shard->info.id, b->blob_id(), 0, 0).get();
                ASSERT_TRUE(!!g);
                auto result = std::move(g.value());
                EXPECT_EQ(result.body.size(), blob_header->blob_size);
                ASSERT_TRUE(
                    memcmp(result.body.cbytes(), blob_data + blob_header->data_offset, blob_header->blob_size) == 0);
                packed_blob_size++;
                LOGDEBUG("[{}]Get blob pg={}, shard {}, blob {}, data_len {}, blob_len {}, header_len {}, user_key_len "
                         "{}, data {}",
                         packed_blob_size, pg->pg_info_.id, shard->info.id, b->blob_id(), b->data()->size(),
                         blob_header->blob_size, sizeof(HSHomeObject::BlobHeader), blob_header->user_key_size,
                         hex_bytes(result.body.cbytes(), 5));
            }
            is_finished = blob_msg->is_last_batch();
        }
        if (shard_seq_num != empty_shard_seq) {
            ASSERT_EQ(packed_blob_size, num_blobs_per_shard - 1);
        } else {
            ASSERT_EQ(packed_blob_size, 0);
        }
    }
    // Verify last obj
    ASSERT_TRUE(pg_iter->update_cursor(objId(LAST_OBJ_ID)));
}

TEST_F(HomeObjectFixture, SnapshotReceiveHandler) {
    constexpr uint64_t snp_lsn = 1;
    constexpr uint64_t num_shards_per_pg = 3;
    constexpr uint64_t num_open_shards_per_pg = 2; // Should be less than num_shards_per_pg
    constexpr uint64_t num_batches_per_shard = 5;
    constexpr uint64_t num_blobs_per_batch = 5;
    constexpr int corrupted_blob_percentage = 9;              // Percentage of blobs with state = CORRUPTED
    constexpr int unexpected_corrupted_batch_percentage = 15; // Percentage of batches with unexpected data corruption

    // We have to create a PG first to init repl_dev
    constexpr pg_id_t pg_id = 1;
    create_pg(pg_id); // to create repl dev
    auto pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(pg != nullptr);
    PGStats stats;
    ASSERT_TRUE(_obj_inst->pg_manager()->get_stats(pg_id, stats));
    auto r_dev = homestore::HomeStore::instance()->repl_service().get_repl_dev(stats.replica_set_uuid);
    ASSERT_TRUE(r_dev.hasValue());

    auto handler = std::make_unique< homeobject::HSHomeObject::SnapshotReceiveHandler >(*_obj_inst, r_dev.value());
    handler->reset_context_and_metrics(snp_lsn, pg_id);

    // Step 1: Test write pg meta - cannot test full logic since the PG already exists
    // Generate ResyncPGMetaData message
    LOGINFO("TESTING: applying meta for pg={}", pg_id);
    ASSERT_TRUE(handler->is_valid_obj_id(objId(0, 0)));

    constexpr auto blob_seq_num = num_shards_per_pg * num_batches_per_shard * num_blobs_per_batch;
    flatbuffers::FlatBufferBuilder builder;
    std::vector< flatbuffers::Offset< Member > > members;
    std::vector uuid(stats.replica_set_uuid.begin(), stats.replica_set_uuid.end());
    for (auto& member : stats.members) {
        auto priority = member.id == stats.leader_id ? 1 : 0;
        auto id = std::vector< std::uint8_t >(member.id.begin(), member.id.end());
        members.push_back(CreateMemberDirect(builder, &id, member.name.c_str(), priority));
    }
    std::vector< uint64_t > shard_ids;
    for (uint64_t i = 1; i <= num_shards_per_pg; i++) {
        shard_ids.push_back(i);
    }
    auto pg_entry =
        CreateResyncPGMetaDataDirect(builder, pg_id, &uuid, pg->pg_info_.size, pg->pg_info_.expected_member_num,
                                     pg->pg_info_.chunk_size, blob_seq_num, num_shards_per_pg, &members, &shard_ids);
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
        ASSERT_TRUE(handler->is_valid_obj_id(objId(HSHomeObject::get_sequence_num_from_shard_id(i), 0)));

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
        auto meta_str = "shard meta:" + std::to_string(i);
        std::memcpy(shard.meta, meta_str.c_str(),meta_str.length());
        shard.meta[meta_str.size()] = '\0';

        auto v_chunk_id = _obj_inst->chunk_selector()->get_most_available_blk_chunk(shard.id, pg_id);

        auto shard_entry = CreateResyncShardMetaData(builder, shard.id, pg_id, static_cast< uint8_t >(shard.state),
                                                     shard.lsn, shard.created_time, shard.last_modified_time,
                                                     shard.total_capacity_bytes, v_chunk_id.value());
        builder.Finish(shard_entry);
        auto shard_meta = GetResyncShardMetaData(builder.GetBufferPointer());
        auto status = handler->process_shard_snapshot_data(*shard_meta);
        builder.Reset();
        ASSERT_EQ(status, 0);
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
            ASSERT_TRUE(handler->is_valid_obj_id(objId(HSHomeObject::get_sequence_num_from_shard_id(shard.id), j)));

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
                const auto aligned_hdr_size = sisl::round_up(sizeof(HSHomeObject::BlobHeader), _obj_inst->_data_block_size);
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
                if (!blob.user_key.empty()) { std::memcpy(hdr.user_key, blob.user_key.data(), blob.user_key.size()); }
                _obj_inst->compute_blob_payload_hash(hdr.hash_algorithm, blob.body.cbytes(), blob.body.size(), hdr.hash,
                                                     HSHomeObject::BlobHeader::blob_max_hash_len);
                hdr.seal();

                std::memcpy(blob_raw.bytes(), &hdr, sizeof(HSHomeObject::BlobHeader));
                std::memcpy(blob_raw.bytes() + hdr.data_offset, blob.body.cbytes(), blob.body.size());

                // Simulate blob data corruption - tamper with random bytes
                if (is_corrupted_batch || blob_state == ResyncBlobState::CORRUPTED) {
                    LOGINFO("Simulating corrupted blob data for shard {} blob {}", shard.id, cur_blob_id);
                    constexpr int corrupted_bytes = 5;
                    for (auto pos = 0; pos < corrupted_bytes; pos++) {
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

            auto result = _obj_inst->blob_manager()->get(shard.id, blob_id, 0, blob.body.size()).get();
            if (is_corrupted) {
                ASSERT_FALSE(!!result);
            } else {
                ASSERT_TRUE(!!result);
                auto blob_res = std::move(result.value());
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
