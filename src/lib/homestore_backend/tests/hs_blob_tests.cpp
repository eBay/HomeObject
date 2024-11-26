#include "homeobj_fixture.hpp"

#include "lib/homestore_backend/index_kv.hpp"
#include "generated/resync_blob_data_generated.h"

TEST_F(HomeObjectFixture, BasicEquivalence) {
    auto shard_mgr = _obj_inst->shard_manager();
    auto pg_mgr = _obj_inst->pg_manager();
    auto blob_mgr = _obj_inst->blob_manager();
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(shard_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(pg_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(blob_mgr.get()));
}

TEST_F(HomeObjectFixture, BasicPutGetDelBlobWRestart) {
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

TEST_F(HomeObjectFixture, SnapshotReceiveHandler) {}
