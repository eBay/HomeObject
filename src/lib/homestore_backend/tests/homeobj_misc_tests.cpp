#include "homeobj_fixture.hpp"
#include "generated/resync_blob_data_generated.h"
#include <homestore/replication_service.hpp>
#include <homestore/blkdata_service.hpp>

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
    auto snp_lsn = pg->shards_.back()->info.create_lsn - 1;
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
        if (shard->info.create_lsn > snp_lsn) { continue; }
        ASSERT_EQ(shard->info.id, pg_msg->shard_ids()->Get(idx++));
    }

    // Verify shard meta data
    current_blob_id = 0;
    for (auto& shard : pg->shards_) {
        auto shard_seq_num = HSHomeObject::get_sequence_num_from_shard_id(shard->info.id);
        auto batch_id = 0;
        objId oid(shard_seq_num, batch_id++);
        if (shard->info.create_lsn > snp_lsn) {
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
        // A shard sealed after the snapshot LSN is downgraded to OPEN for snapshot consistency.
        auto expected_state = shard->info.state;
        auto expected_sealed_lsn = shard->info.sealed_lsn;
        if (expected_state == ShardInfo::State::SEALED && shard->info.sealed_lsn > snp_lsn) {
            expected_state = ShardInfo::State::OPEN;
            expected_sealed_lsn = static_cast< uint64_t >(INT64_MAX);
        }
        ASSERT_EQ(shard_msg->state(), static_cast< uint8_t >(expected_state));
        ASSERT_EQ(shard_msg->created_lsn(), shard->info.create_lsn);
        ASSERT_EQ(shard_msg->sealed_lsn(), expected_sealed_lsn);
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

// Simulates a GC race where blob blkID changes between generate_shard_blob_list and load_blob_data.
// Injects a cross-shard pbas into cur_blob_list_ so verify_blob fails (shard_id mismatch in blob header).
// The retry inside load_blob_data_with_blkid re-reads the index, finds the updated pbas, and re-reads
// the blob successfully. create_blobs_snapshot_data should return true with the blob marked NORMAL.
TEST_F(HomeObjectFixture, PGBlobIteratorGCMoveDetection) {
    constexpr pg_id_t pg_id{1};
    create_pg(pg_id);
    auto shard_1_info = create_shard(pg_id, 64 * Mi, "shard1");
    auto shard_2_info = create_shard(pg_id, 64 * Mi, "shard2");
    auto shard_1_id = shard_1_info.id;
    auto shard_2_id = shard_2_info.id;

    // blob_0 -> shard_1, blob_1 -> shard_2
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_map{{pg_id, {shard_1_id, shard_2_id}}};
    std::map< pg_id_t, blob_id_t > pg_blob_id{{pg_id, 0}};
    put_blobs(pg_shard_map, 1 /* num_blobs_per_shard */, pg_blob_id);

    auto pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(pg != nullptr);

    auto snp_lsn = pg->shards_.back()->info.create_lsn;
    auto pg_iter = std::make_shared< HSHomeObject::PGBlobIterator >(*_obj_inst, pg->pg_info_.replica_set_uuid, snp_lsn);
    ASSERT_EQ(pg_iter->shard_list_.size(), 2u);
    pg_iter->max_batch_size_ = 1 * Mi;

    auto shard_1_seq_num = HSHomeObject::get_sequence_num_from_shard_id(shard_1_id);
    ASSERT_TRUE(pg_iter->update_cursor(objId(shard_1_seq_num, 0)));
    ASSERT_TRUE(pg_iter->generate_shard_blob_list());
    ASSERT_EQ(pg_iter->cur_blob_list_.size(), 1u);

    // Save the correct pbas for blob_0 in shard_1
    auto correct_pbas = pg_iter->cur_blob_list_[0].pbas;

    // Get shard_2's blob_1 pbas — its blob header has shard_2_id, so verify_blob against shard_1_id will fail.
    auto index_table = _obj_inst->get_index_table(pg_id);
    auto shard_2_blob_pbas = _obj_inst->get_blob_from_index_table(index_table, shard_2_id, 1 /* blob_id */);
    ASSERT_TRUE(shard_2_blob_pbas.hasValue());
    ASSERT_NE(shard_2_blob_pbas.value(), correct_pbas);

    // Inject stale pbas (simulating GC moved blob_0 to a new location already reflected in the
    // index table, but cur_blob_list_ still holds the old blkID).
    // The index still points to correct_pbas, so load_blob_data_with_blkid will:
    //   1. Read from shard_2_blob_pbas → verify_blob fails (shard_id mismatch)
    //   2. Re-read index → finds correct_pbas != shard_2_blob_pbas → retry
    //   3. Read from correct_pbas → verify_blob passes → NORMAL
    pg_iter->cur_blob_list_[0].pbas = shard_2_blob_pbas.value();

    sisl::io_blob_safe shard_meta_blob;
    ASSERT_TRUE(pg_iter->create_shard_snapshot_data(shard_meta_blob));

    objId batch_1_oid(shard_1_seq_num, 1);
    ASSERT_TRUE(pg_iter->update_cursor(batch_1_oid));

    sisl::io_blob_safe data_blob;
    ASSERT_TRUE(pg_iter->create_blobs_snapshot_data(data_blob));

    auto* msg_hdr = r_cast< SyncMessageHeader* >(data_blob.bytes());
    ASSERT_EQ(msg_hdr->msg_type, SyncMessageType::SHARD_BATCH);
    auto blob_msg = GetSizePrefixedResyncBlobDataBatch(data_blob.cbytes() + sizeof(SyncMessageHeader));
    ASSERT_EQ(blob_msg->blob_list()->size(), 1u);
    EXPECT_EQ(blob_msg->blob_list()->Get(0)->blob_id(), 0u);
    EXPECT_EQ(blob_msg->blob_list()->Get(0)->state(), static_cast< uint8_t >(ResyncBlobState::NORMAL));
    EXPECT_TRUE(blob_msg->is_last_batch());

    pg_iter->stop();
}

// Simulates the race where a blob is tombstoned (deleted) after generate_shard_blob_list captured its pbas.
// verify_blob fails on the stale read; the index lookup returns UNKNOWN_BLOB (tombstone) so !current_pbas
// is true — treated as unchanged — and the blob is returned as CORRUPTED.
TEST_F(HomeObjectFixture, PGBlobIteratorGCTombstoneDetection) {
    constexpr pg_id_t pg_id{1};
    create_pg(pg_id);
    auto shard_1_info = create_shard(pg_id, 64 * Mi, "shard1");
    auto shard_2_info = create_shard(pg_id, 64 * Mi, "shard2");
    auto shard_1_id = shard_1_info.id;
    auto shard_2_id = shard_2_info.id;

    // blob_0 -> shard_1, blob_1 -> shard_2
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_map{{pg_id, {shard_1_id, shard_2_id}}};
    std::map< pg_id_t, blob_id_t > pg_blob_id{{pg_id, 0}};
    put_blobs(pg_shard_map, 1 /* num_blobs_per_shard */, pg_blob_id);

    auto pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(pg != nullptr);

    auto snp_lsn = pg->shards_.back()->info.create_lsn;
    auto pg_iter = std::make_shared< HSHomeObject::PGBlobIterator >(*_obj_inst, pg->pg_info_.replica_set_uuid, snp_lsn);
    pg_iter->max_batch_size_ = 1 * Mi;

    auto shard_1_seq_num = HSHomeObject::get_sequence_num_from_shard_id(shard_1_id);
    ASSERT_TRUE(pg_iter->update_cursor(objId(shard_1_seq_num, 0)));
    ASSERT_TRUE(pg_iter->generate_shard_blob_list());
    ASSERT_EQ(pg_iter->cur_blob_list_.size(), 1u);

    // Get shard_2's blob_1 pbas to inject as a stale blkid for blob_0 in shard_1.
    auto index_table = _obj_inst->get_index_table(pg_id);
    auto shard_2_blob_pbas = _obj_inst->get_blob_from_index_table(index_table, shard_2_id, 1 /* blob_id */);
    ASSERT_TRUE(shard_2_blob_pbas.hasValue());

    // Inject stale pbas and tombstone blob_0 so the index lookup returns UNKNOWN_BLOB.
    // Flow: read from shard_2's location → verify_blob fails (shard_id mismatch) →
    //       re-read index for (shard_1_id, blob_0) → UNKNOWN_BLOB (tombstoned) →
    //       !current_pbas is true → return CORRUPTED.
    pg_iter->cur_blob_list_[0].pbas = shard_2_blob_pbas.value();
    del_blob(pg_id, shard_1_id, 0 /* blob_id */);

    sisl::io_blob_safe shard_meta_blob;
    ASSERT_TRUE(pg_iter->create_shard_snapshot_data(shard_meta_blob));

    objId batch_1_oid(shard_1_seq_num, 1);
    ASSERT_TRUE(pg_iter->update_cursor(batch_1_oid));

    // READ_FAILED is returned so the snapshot restarts; generate_shard_blob_list will then pick up
    // tombstone_pbas for blob_0 and skip it cleanly on the next attempt.
    sisl::io_blob_safe data_blob;
    ASSERT_FALSE(pg_iter->create_blobs_snapshot_data(data_blob));

    pg_iter->stop();
}

// Verifies that a shard sealed after the snapshot LSN cutoff is presented as OPEN to the snapshot
// receiver (Case A), while a shard sealed at or before the cutoff remains SEALED (Case B).
//
// Without this conversion a receiver applying the snapshot could release the shard's backing chunk
// too early, before log replay has a chance to replay the seal_shard entry, leading to stale
// physical-chunk references and write failures.
TEST_F(HomeObjectFixture, PGBlobIteratorSealedLsnCutoff) {
    constexpr pg_id_t pg_id{1};
    create_pg(pg_id);
    auto shard_1_info = create_shard(pg_id, 64 * Mi, "shard1");
    auto shard_2_info = create_shard(pg_id, 64 * Mi, "shard2");

    // Seal shard_1 after shard_2 is created so that shard_1.sealed_lsn > shard_2.create_lsn.
    seal_shard(shard_1_info.id);

    auto pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(pg != nullptr);

    // Identify shards by id to be independent of sort order in shard_list_.
    Shard* pg_shard_1 = nullptr;
    Shard* pg_shard_2 = nullptr;
    for (auto& s : pg->shards_) {
        if (s->info.id == shard_1_info.id) pg_shard_1 = s.get();
        if (s->info.id == shard_2_info.id) pg_shard_2 = s.get();
    }
    ASSERT_TRUE(pg_shard_1 != nullptr && pg_shard_2 != nullptr);
    ASSERT_EQ(pg_shard_1->info.state, ShardInfo::State::SEALED);
    // Guarantee that sealing happened after shard_2 was created.
    ASSERT_GT(pg_shard_1->info.sealed_lsn, pg_shard_2->info.create_lsn);

    // Case A: snp_lsn falls between shard_1.create_lsn and shard_1.sealed_lsn.
    // shard_1 must be downgraded to OPEN so the receiver does not release its chunk prematurely.
    {
        auto snp_lsn = pg_shard_2->info.create_lsn;
        ASSERT_GT(snp_lsn, pg_shard_1->info.create_lsn);
        ASSERT_LT(snp_lsn, pg_shard_1->info.sealed_lsn);

        auto pg_iter =
            std::make_shared< HSHomeObject::PGBlobIterator >(*_obj_inst, pg->pg_info_.replica_set_uuid, snp_lsn);
        ASSERT_EQ(pg_iter->shard_list_.size(), 2u);

        auto it1 = std::find_if(pg_iter->shard_list_.begin(), pg_iter->shard_list_.end(),
                                [&](const auto& e) { return e.info.id == shard_1_info.id; });
        ASSERT_NE(it1, pg_iter->shard_list_.end());
        EXPECT_EQ(it1->info.state, ShardInfo::State::OPEN);
        EXPECT_EQ(it1->info.sealed_lsn, static_cast< uint64_t >(INT64_MAX));

        auto it2 = std::find_if(pg_iter->shard_list_.begin(), pg_iter->shard_list_.end(),
                                [&](const auto& e) { return e.info.id == shard_2_info.id; });
        ASSERT_NE(it2, pg_iter->shard_list_.end());
        EXPECT_EQ(it2->info.state, ShardInfo::State::OPEN);
    }

    // Case B: snp_lsn >= shard_1.sealed_lsn.
    // shard_1 was fully sealed within the snapshot range and must remain SEALED.
    {
        auto snp_lsn = pg_shard_1->info.sealed_lsn;

        auto pg_iter =
            std::make_shared< HSHomeObject::PGBlobIterator >(*_obj_inst, pg->pg_info_.replica_set_uuid, snp_lsn);
        ASSERT_EQ(pg_iter->shard_list_.size(), 2u);

        auto it1 = std::find_if(pg_iter->shard_list_.begin(), pg_iter->shard_list_.end(),
                                [&](const auto& e) { return e.info.id == shard_1_info.id; });
        ASSERT_NE(it1, pg_iter->shard_list_.end());
        EXPECT_EQ(it1->info.state, ShardInfo::State::SEALED);
        EXPECT_EQ(it1->info.sealed_lsn, pg_shard_1->info.sealed_lsn);
    }
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
        shard.create_lsn = snp_lsn;
        auto meta_str = "shard meta:" + std::to_string(i);
        std::memcpy(shard.meta, meta_str.c_str(), meta_str.length());
        shard.meta[meta_str.size()] = '\0';

        auto exVchunk = _obj_inst->chunk_selector()->pick_most_available_blk_chunk(shard.id, pg_id);
        RELEASE_ASSERT(exVchunk != nullptr, "chunk selection failed with v_chunk_id={} in pg={}", shard.id, pg_id);
        RELEASE_ASSERT(exVchunk->m_v_chunk_id.has_value(), "v_chunk_id should have value for selected chunk for pg={}",
                       pg_id);
        auto shard_entry =
            CreateResyncShardMetaData(builder, shard.id, pg_id, static_cast< uint8_t >(shard.state), shard.create_lsn,
                                      shard.created_time, shard.last_modified_time, shard.total_capacity_bytes,
                                      exVchunk->m_v_chunk_id.value(), 0 /* meta */, shard.sealed_lsn);
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
        ASSERT_EQ(shard_res.create_lsn, shard.create_lsn);
        ASSERT_EQ(shard_res.sealed_lsn, shard.sealed_lsn);

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
                const auto aligned_hdr_size =
                    sisl::round_up(sizeof(HSHomeObject::BlobHeader), _obj_inst->_data_block_size);
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
            ASSERT_EQ(v.value(), exVchunk->m_v_chunk_id.value());
            ASSERT_TRUE(_obj_inst->chunk_selector()->is_chunk_available(pg_id, v.value()));
        }
    }
}

// Test: verify the case (https://jirap.corp.ebay.com/browse/SDSTOR-23023) when the snapshot receiver re-delivers a blob
// batch that was already indexed (because the IndexSvc journal survived a crash but the AppendBlkAllocator CP superblk
// did not), the allocator watermarks are correctly restored via commit_blk(recommit=true) so that subsequent
// allocations cannot silently reuse the already-occupied blocks.
//
// Scenario:
//   1. Partial first delivery (blobs 0..num_blobs_partial-1) — written, committed, indexed.
//   2. Crash simulated via VChunk::reset(), zeroing the allocator watermarks exactly as
//      AppendBlkAllocator::on_meta_blk_found() would after a CP that missed the allocator flush.
//   3. Full second delivery (blobs 0..num_blobs_total-1) — blobs 0..num_blobs_partial-1 hit the
//      dedup path and must restore watermarks; blobs num_blobs_partial..num_blobs_total-1 are new
//      and must be allocated strictly above the already-written blocks.
//
// Without the fix the allocator watermarks stay at 0 after step 3, so the new blobs (step 3) get
// allocated from blk 0, silently overwriting the data written in step 1.
TEST_F(HomeObjectFixture, SnapshotReceiveHandlerAllocatorResyncAfterCrash) {
    constexpr uint64_t snp_lsn = 1;
    constexpr pg_id_t pg_id = 1;
    constexpr blob_id_t num_blobs_partial = 2; // blobs written before the simulated crash
    constexpr blob_id_t num_blobs_total = 4;   // full set re-delivered after crash

    // ---- Setup: create PG and handler (mirrors SnapshotReceiveHandler test) ----
    create_pg(pg_id);
    auto pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_NE(pg, nullptr);
    PGStats stats;
    ASSERT_TRUE(_obj_inst->pg_manager()->get_stats(pg_id, stats));
    auto r_dev = homestore::HomeStore::instance()->repl_service().get_repl_dev(stats.replica_set_uuid);
    ASSERT_TRUE(r_dev.hasValue());

    auto handler = std::make_unique< homeobject::HSHomeObject::SnapshotReceiveHandler >(*_obj_inst, r_dev.value());
    handler->reset_context_and_metrics(snp_lsn, pg_id);

    // ---- PG meta ----
    flatbuffers::FlatBufferBuilder builder;
    std::vector< flatbuffers::Offset< Member > > members;
    std::vector< uint8_t > uuid(stats.replica_set_uuid.begin(), stats.replica_set_uuid.end());
    for (auto& member : stats.members) {
        auto priority = member.id == stats.leader_id ? 1 : 0;
        auto id = std::vector< uint8_t >(member.id.begin(), member.id.end());
        members.push_back(CreateMemberDirect(builder, &id, member.name.c_str(), priority));
    }
    std::vector< uint64_t > shard_ids = {1};
    auto pg_entry = CreateResyncPGMetaDataDirect(builder, pg_id, &uuid, pg->pg_info_.size,
                                                 pg->pg_info_.expected_member_num, pg->pg_info_.chunk_size,
                                                 num_blobs_total /*blob_seq_num*/, 1 /*shard_seq_num*/, &members,
                                                 &shard_ids);
    builder.Finish(pg_entry);
    ASSERT_EQ(handler->process_pg_snapshot_data(*GetResyncPGMetaData(builder.GetBufferPointer())), 0);
    builder.Reset();

    // ---- Shard meta ----
    constexpr shard_id_t shard_id = 1;
    auto exVchunk = _obj_inst->chunk_selector()->pick_most_available_blk_chunk(shard_id, pg_id);
    ASSERT_NE(exVchunk, nullptr);
    ASSERT_TRUE(exVchunk->m_v_chunk_id.has_value());

    auto shard_entry = CreateResyncShardMetaData(
        builder, shard_id, pg_id, static_cast< uint8_t >(ShardInfo::State::OPEN), snp_lsn /*create_lsn*/,
        get_time_since_epoch_ms() /*created_time*/, get_time_since_epoch_ms() /*modified_time*/, 1024 * Mi /*capacity*/,
        exVchunk->m_v_chunk_id.value(), 0 /*meta*/, 0 /*sealed_lsn*/);
    builder.Finish(shard_entry);
    ASSERT_EQ(handler->process_shard_snapshot_data(*GetResyncShardMetaData(builder.GetBufferPointer())), 0);
    builder.Reset();

    // ---- Helper: build a blob batch covering [start_blob_id, end_blob_id) ----
    auto make_blob_batch = [&](flatbuffers::FlatBufferBuilder& b, blob_id_t start_blob_id, blob_id_t end_blob_id,
                               bool is_last) {
        std::vector< flatbuffers::Offset< ResyncBlobData > > blob_entries;
        for (blob_id_t blob_id = start_blob_id; blob_id < end_blob_id; blob_id++) {
            auto blob = build_blob(blob_id);
            const auto aligned_hdr_size = sisl::round_up(sizeof(HSHomeObject::BlobHeader), _obj_inst->_data_block_size);
            sisl::io_blob_safe blob_raw(aligned_hdr_size + blob.body.size(), io_align);

            HSHomeObject::BlobHeader hdr;
            hdr.type = HSHomeObject::DataHeader::data_type_t::BLOB_INFO;
            hdr.shard_id = shard_id;
            hdr.blob_id = blob_id;
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

            std::vector< uint8_t > data(blob_raw.bytes(), blob_raw.bytes() + blob_raw.size());
            blob_entries.push_back(
                CreateResyncBlobDataDirect(b, blob_id, static_cast< uint8_t >(ResyncBlobState::NORMAL), &data));
        }
        b.Finish(CreateResyncBlobDataBatchDirect(b, &blob_entries, is_last));
    };

    // ---- First delivery: partial batch (blobs 0..num_blobs_partial-1), not the last batch ----
    // Simulates the pre-crash state: these blobs are written to disk, committed, and indexed.
    // is_last_batch=false so update_snp_info_sb (the CP+shard-cursor write) is NOT triggered,
    // matching the real crash scenario where the shard was only partially received.
    LOGINFO("TEST: first (partial) delivery, blobs [0, {})", num_blobs_partial);
    make_blob_batch(builder, 0, num_blobs_partial, false /*is_last*/);
    ASSERT_EQ(handler->process_blobs_snapshot_data(*GetResyncBlobDataBatch(builder.GetBufferPointer()), 1 /*batch_num*/,
                                                   false /*is_last_batch*/),
              0);
    builder.Reset();

    // Confirm only the partial set is readable and indexed
    for (blob_id_t blob_id = 0; blob_id < num_blobs_partial; blob_id++) {
        auto result = _obj_inst->blob_manager()->get(shard_id, blob_id, 0, 0).get();
        ASSERT_TRUE(!!result) << "blob_id=" << blob_id << " should be readable after partial delivery";
    }
    for (blob_id_t blob_id = num_blobs_partial; blob_id < num_blobs_total; blob_id++) {
        auto result = _obj_inst->blob_manager()->get(shard_id, blob_id, 0, 0).get();
        ASSERT_FALSE(!!result) << "blob_id=" << blob_id << " should NOT exist before full delivery";
    }

    // Capture the blk_ids assigned to the partial set — these are the blocks the allocator must
    // protect during re-delivery so new blobs don't overwrite them.
    auto index_table = _obj_inst->get_hs_pg(pg_id)->index_table_;
    ASSERT_NE(index_table, nullptr);
    std::vector< homestore::MultiBlkId > partial_blk_ids;
    for (blob_id_t blob_id = 0; blob_id < num_blobs_partial; blob_id++) {
        auto res = _obj_inst->get_blob_from_index_table(index_table, shard_id, blob_id);
        ASSERT_TRUE(res.hasValue()) << "blob_id=" << blob_id << " must be in index after partial delivery";
        partial_blk_ids.push_back(res.value());
    }

    // ---- Simulate crash: reset allocator watermarks to zero ----
    // Replicates AppendBlkAllocator::on_meta_blk_found() recovering from a CP that captured
    // the IndexSvc BTree entries for blobs 0..num_blobs_partial-1 but did NOT flush the
    // AppendBlkAllocator commit_offset superblock. Both watermarks (m_last_append_offset,
    // m_commit_offset) are restored to 0, so the allocator considers the chunk fully empty.
    auto p_chunk_id = _obj_inst->get_shard_p_chunk_id(shard_id);
    ASSERT_TRUE(p_chunk_id.has_value());
    auto vchunk = _obj_inst->chunk_selector()->m_chunks.at(*p_chunk_id);
    ASSERT_NE(vchunk, nullptr);

    const auto used_blks_after_partial = vchunk->get_used_blks();
    ASSERT_GT(used_blks_after_partial, 0u) << "allocator must have advanced after partial delivery";

    LOGINFO("TEST: simulating crash — resetting allocator watermarks (used_blks was {})", used_blks_after_partial);
    vchunk->reset(); // zeroes m_last_append_offset and m_commit_offset
    ASSERT_EQ(vchunk->get_used_blks(), 0u) << "used_blks must be 0 after reset (simulating crashed CP)";

    // ---- Second delivery: full batch (blobs 0..num_blobs_total-1), is_last_batch=true ----
    // Blobs 0..num_blobs_partial-1 are already in the index → dedup path fires.
    //   Without fix: watermarks stay at 0; blobs num_blobs_partial..num_blobs_total-1 get
    //                allocated from blk 0, silently overwriting the data for blobs 0..num_blobs_partial-1.
    //   With fix:    commit_blk(recommit=true) restores watermarks to cover the partial set;
    //                new blobs are then allocated strictly above them.
    // Blobs num_blobs_partial..num_blobs_total-1 are new → normal write path.
    LOGINFO("TEST: second (full) delivery after crash, blobs [0, {})", num_blobs_total);
    handler->ctx_->cur_batch_num = 0; // reset so batch_num=1 is accepted again
    make_blob_batch(builder, 0, num_blobs_total, true /*is_last*/);
    ASSERT_EQ(handler->process_blobs_snapshot_data(*GetResyncBlobDataBatch(builder.GetBufferPointer()), 1 /*batch_num*/,
                                                   true /*is_last_batch*/),
              0);
    builder.Reset();

    // ---- Verify allocator watermarks advanced beyond the partial set ----
    // The new blobs (num_blobs_partial..num_blobs_total-1) must have been allocated above the
    // partial set, so used_blks must now exceed what the partial delivery alone had consumed.
    const auto used_blks_after_full = vchunk->get_used_blks();
    LOGINFO("TEST: used_blks after full delivery: {} (partial delivery had {})", used_blks_after_full,
            used_blks_after_partial);
    ASSERT_GT(used_blks_after_full, used_blks_after_partial)
        << "new blobs must have been allocated above the partial set, advancing the watermark further";

    // ---- Verify no overlap: a fresh alloc must land ABOVE all blobs (partial + new) ----
    // Core safety check: without the fix, alloc_blks() returns blk 0 (watermark stayed 0),
    // overlapping the data written for blobs 0..num_blobs_partial-1 in the first delivery.
    homestore::MultiBlkId new_blk_id;
    homestore::blk_alloc_hints hints;
    hints.chunk_id_hint = *p_chunk_id;
    auto alloc_status =
        homestore::data_service().alloc_blks(homestore::data_service().get_blk_size(), hints, new_blk_id);
    ASSERT_EQ(alloc_status, homestore::BlkAllocStatus::SUCCESS);

    for (const auto& existing_blk : partial_blk_ids) {
        auto existing_end = existing_blk.blk_num() + existing_blk.blk_count();
        ASSERT_GE(new_blk_id.blk_num(), existing_end)
            << "new allocation at blk " << new_blk_id.blk_num() << " must not overlap partial blob at blks ["
            << existing_blk.blk_num() << ", " << existing_end << ")";
    }
    homestore::data_service().async_free_blk(new_blk_id).get();

    // ---- Verify all blobs are readable with correct content ----
    // Blobs 0..num_blobs_partial-1: must retain the data from the first delivery (not overwritten
    // by the new-blob allocations in the second delivery).
    // Blobs num_blobs_partial..num_blobs_total-1: must be readable as newly written.
    for (blob_id_t blob_id = 0; blob_id < num_blobs_total; blob_id++) {
        auto result = _obj_inst->blob_manager()->get(shard_id, blob_id, 0, 0).get();
        ASSERT_TRUE(!!result) << "blob_id=" << blob_id << " must be readable after full delivery";
        auto expected = build_blob(blob_id);
        ASSERT_EQ(result->body.size(), expected.body.size());
        ASSERT_EQ(std::memcmp(result->body.bytes(), expected.body.cbytes(), expected.body.size()), 0)
            << "blob_id=" << blob_id << " data must match expected content";
    }
}
