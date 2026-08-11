#include "homeobj_fixture.hpp"
#include <homestore/blk.h>
#include <homestore/btree/btree_req.hpp>
#include <homestore/btree/btree_kv.hpp>
#include <random>
#include "lib/homestore_backend/hs_homeobject.hpp"

using namespace homeobject;
using BlobHeader = HSHomeObject::BlobHeader;

// Helper function to delete a blob from index table
static void delete_blob_from_index(shared< homestore::IndexTable< BlobRouteKey, BlobRouteValue > > pg_index_table,
                                   shard_id_t shard_id, blob_id_t blob_id) {
    LOGINFO("Deleting blob from index, shard_id={}, blob_id={}", shard_id, blob_id);
    BlobRouteKey blob_key{BlobRoute{shard_id, blob_id}};
    BlobRouteValue out_value;
    homestore::BtreeSingleRemoveRequest remove_req{&blob_key, &out_value};
    auto status = pg_index_table->remove(remove_req);
    ASSERT_TRUE(status == homestore::btree_status_t::success)
        << "Failed to remove blob key from index table, status=" << status;
}

static void delete_shard_from_index(shared< homestore::IndexTable< BlobRouteKey, BlobRouteValue > > pg_index_table,
                                    shard_id_t missing_shard_id) {
    LOGINFO("Deleting shard from index, shard_id={}", missing_shard_id);
    auto start_key = BlobRouteKey{BlobRoute{missing_shard_id, 0}};
    auto end_key = BlobRouteKey{BlobRoute{missing_shard_id, std::numeric_limits< uint64_t >::max()}};
    homestore::BtreeRangeRemoveRequest< BlobRouteKey > range_remove_req{
        homestore::BtreeKeyRange< BlobRouteKey >{
            std::move(start_key), true /* inclusive */, std::move(end_key), true /* inclusive */
        },
        nullptr, std::numeric_limits< uint32_t >::max(),
        [](homestore::BtreeKey const& key, homestore::BtreeValue const& value) -> bool { return true; }};

    auto status = pg_index_table->remove(range_remove_req);
    ASSERT_TRUE(status == homestore::btree_status_t::success || status == homestore::btree_status_t::not_found)
        << "Failed to remove shard keys from index table, status=" << status;
}

// Helper function to corrupt a blob's data
static void corrupt_blob_data(shared< homestore::IndexTable< BlobRouteKey, BlobRouteValue > > pg_index_table,
                              shard_id_t shard_id, blob_id_t blob_id) {
    auto& data_service = homestore::data_service();
    const auto blk_size = data_service.get_blk_size();

    BlobRouteKey blob_key{BlobRoute{shard_id, blob_id}};
    BlobRouteValue out_value;
    homestore::BtreeSingleGetRequest blob_get_req{&blob_key, &out_value};

    auto status = pg_index_table->get(blob_get_req);
    ASSERT_TRUE(status == homestore::btree_status_t::success)
        << "Failed to get blob key from index table, status=" << status;

    auto pbas = out_value.pbas();
    auto total_size = pbas.blk_count() * blk_size;
    sisl::sg_list data_sgs;
    data_sgs.size = total_size;
    data_sgs.iovs.emplace_back(iovec{.iov_base = iomanager.iobuf_alloc(blk_size, total_size), .iov_len = total_size});

    data_service.async_read(pbas, data_sgs, total_size)
        .thenValue([&](auto&& err) {
            if (err) {
                LOGE("Failed to read blob data, blob_id={}, err={}", blob_id, err.message());
                iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                throw std::runtime_error(fmt::format("Failed to read blob data: {}", err.message()));
            }

            auto* data_ptr = reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base);
            for (size_t i = 0; i < data_sgs.iovs[0].iov_len / 2; i++) {
                data_ptr[i] ^= 0xFF; // Flip first half of data
            }

            return data_service.async_write(data_sgs, pbas).thenValue([data_sgs = std::move(data_sgs)](auto&& err) {
                iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                ASSERT_FALSE(err) << "Failed to write corrupted blob data";
            });
        })
        .get();
}

// Helper function to make a blob inconsistent (valid but different hash)
static void make_blob_inconsistent(shared< homestore::IndexTable< BlobRouteKey, BlobRouteValue > > pg_index_table,
                                   shard_id_t shard_id, blob_id_t blob_id, HSHomeObject* obj_inst) {
    auto& data_service = homestore::data_service();
    const auto blk_size = data_service.get_blk_size();

    BlobRouteKey blob_key{BlobRoute{shard_id, blob_id}};
    BlobRouteValue out_value;
    homestore::BtreeSingleGetRequest blob_get_req{&blob_key, &out_value};

    auto status = pg_index_table->get(blob_get_req);
    ASSERT_TRUE(status == homestore::btree_status_t::success) << "Failed to get blob key from index table";

    auto pbas = out_value.pbas();
    auto total_size = pbas.blk_count() * blk_size;
    sisl::sg_list data_sgs;
    data_sgs.size = total_size;
    data_sgs.iovs.emplace_back(iovec{.iov_base = iomanager.iobuf_alloc(blk_size, total_size), .iov_len = total_size});

    data_service.async_read(pbas, data_sgs, total_size)
        .thenValue([&](auto&& err) {
            if (err) {
                LOGE("Failed to read blob data, blob_id={}, err={}", blob_id, err.message());
                iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                throw std::runtime_error(fmt::format("Failed to read blob data: {}", err.message()));
            }

            // Modify blob data and recompute valid hash
            uint8_t* read_buf = r_cast< uint8_t* >(data_sgs.iovs[0].iov_base);
            auto header = r_cast< BlobHeader* >(read_buf);
            uint8_t* blob_bytes = read_buf + header->data_offset;

            std::mt19937 rng{std::random_device{}()};
            std::uniform_int_distribution< int > dist(0, 255);

            for (size_t i = 0; i < header->blob_size / 2; i++) {
                blob_bytes[i] ^= static_cast< uint8_t >(dist(rng));
            }

            uint8_t computed_hash[BlobHeader::blob_max_hash_len]{};
            obj_inst->compute_blob_payload_hash(header->hash_algorithm, blob_bytes, header->blob_size, computed_hash,
                                                BlobHeader::blob_max_hash_len);

            std::memcpy(header->hash, computed_hash, BlobHeader::blob_max_hash_len);
            std::memset(header->header_hash, 0, BlobHeader::blob_max_hash_len);
            uint32_t computed_header_hash = crc32_ieee(0, (uint8_t*)header, sizeof(BlobHeader));
            std::memcpy(header->header_hash, &computed_header_hash, sizeof(uint32_t));

            if (!obj_inst->verify_blob(data_sgs.iovs[0].iov_base, header->shard_id, header->blob_id)) {
                LOGE("Blob verification failed after modification, blob_id={}", blob_id);
                iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                throw std::runtime_error(fmt::format("Blob verification failed for blob_id={}", blob_id));
            }

            return data_service.async_write(data_sgs, pbas).thenValue([data_sgs = std::move(data_sgs)](auto&& err) {
                iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
                ASSERT_FALSE(err) << "Failed to write inconsistent blob data";
            });
        })
        .get();
}

// Helper function to verify missing blobs in scrub report
// missing_blobs[blob_route] = set of peers that HAVE the blob; peer_id is missing it iff not in that set.
static void verify_missing_blobs(std::shared_ptr< ScrubManager::DeepScrubReport > report, const peer_id_t& peer_id,
                                 const BlobRoute& expected_blob) {
    const auto& missing_blobs = report->get_missing_blobs();
    auto it = missing_blobs.find(expected_blob);
    EXPECT_TRUE(it != missing_blobs.end())
        << "Missing blob should be reported for shard_id=" << expected_blob.shard << ", blob_id=" << expected_blob.blob;
    if (it != missing_blobs.end()) {
        EXPECT_TRUE(it->second.count(peer_id) == 0)
            << "peer_id=" << peer_id << " should not have the blob (it is missing on this peer)";
    }
}

// Helper function to verify corrupted blobs in scrub report
static void verify_corrupted_blobs(std::shared_ptr< ScrubManager::DeepScrubReport > report, const peer_id_t& peer_id,
                                   const BlobRoute& expected_blob) {
    const auto& corrupted_blobs = report->get_corrupted_blobs();
    auto it = corrupted_blobs.find(peer_id);
    EXPECT_TRUE(it != corrupted_blobs.end()) << "Corrupted blob should be reported for peer_id=" << peer_id;
    if (it != corrupted_blobs.end()) {
        EXPECT_TRUE(it->second.count(expected_blob) == 1) << "Expected corrupted blob should be in the report";
    }
}

// Helper function to verify missing shards in scrub report
// missing_shard_ids[shard_id] = set of peers that HAVE the shard; peer_id is missing it if not in that set.
static void verify_missing_shards(std::shared_ptr< ScrubManager::DeepScrubReport > report, const peer_id_t& peer_id,
                                  shard_id_t expected_shard) {
    const auto& missing_shards = report->get_missing_shard_ids();
    auto it = missing_shards.find(expected_shard);
    EXPECT_TRUE(it != missing_shards.end()) << "Missing shard should be reported for shard_id=" << expected_shard;
    if (it != missing_shards.end()) {
        EXPECT_TRUE(it->second.count(peer_id) == 0)
            << "peer_id=" << peer_id << " should not have the shard (it is missing on this peer)";
    }
}

TEST_F(HomeObjectFixture, BasicScrubTest) {
    const pg_id_t pg_id = 1;
    create_pg(pg_id);
    auto scrub_mgr = _obj_inst->scrub_manager();

    // empty pg scrub should report no issues
    run_on_pg_leader(pg_id, [&]() {
        // Deep scrub on empty PG should complete without errors
        auto scrub_report = scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        ASSERT_NE(scrub_report, nullptr) << "Deep scrub report should not be null for empty PG";
        auto deep_scrub_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(scrub_report);
        ASSERT_NE(deep_scrub_report, nullptr) << "Should be DeepScrubReport";

        // Empty PG should have no issues
        EXPECT_TRUE(deep_scrub_report->get_corrupted_shards().empty()) << "Empty PG should have no corrupted shards";
        EXPECT_TRUE(deep_scrub_report->get_corrupted_pg_metas().empty())
            << "No PG metas should be corrupted in normal case";
        EXPECT_TRUE(deep_scrub_report->get_inconsistent_shard_metas().empty())
            << "No shard metas should be inconsistent in normal case";
        EXPECT_TRUE(deep_scrub_report->get_missing_shard_ids().empty()) << "Empty PG should have no missing shards";
        EXPECT_TRUE(deep_scrub_report->get_missing_blobs().empty()) << "Empty PG should have no missing blobs";

        EXPECT_TRUE(deep_scrub_report->get_corrupted_blobs().empty()) << "Empty PG should have no corrupted blobs";
        EXPECT_TRUE(deep_scrub_report->get_inconsistent_blobs().empty())
            << "Empty PG should have no inconsistent blobs";

        // Shallow scrub on empty PG
        scrub_report = scrub_mgr->submit_scrub_task(pg_id, false /* is_deep */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        EXPECT_TRUE(scrub_report->get_corrupted_shards().empty()) << "Empty PG should have no corrupted shards";
        EXPECT_TRUE(scrub_report->get_corrupted_pg_metas().empty()) << "No PG metas should be corrupted in normal case";
        EXPECT_TRUE(scrub_report->get_inconsistent_shard_metas().empty())
            << "No shard metas should be inconsistent in normal case";
        EXPECT_TRUE(scrub_report->get_missing_shard_ids().empty()) << "Empty PG should have no missing shards";
        EXPECT_TRUE(scrub_report->get_missing_blobs().empty()) << "Empty PG should have no missing blobs";
    });

    const uint64_t num_shards = SISL_OPTIONS["num_shards"].as< uint64_t >();
    // follower test uses indices 0-2, leader test uses indices 3-5 in blob_op_shard_id
    const uint64_t num_blobs_per_shard = std::max(SISL_OPTIONS["num_blobs"].as< uint64_t >(), uint64_t{6});
    const uint64_t shard_size = 64 * Mi;

    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    pg_blob_id[pg_id] = 0;

    std::map< shard_id_t, std::map< blob_id_t, uint64_t > > shard_blob_ids_map;

    // Create multiple shards
    for (uint64_t i = 0; i < num_shards; i++) {
        auto shard_info = create_shard(pg_id, shard_size, "shard meta");
        pg_shard_id_vec[pg_id].push_back(shard_info.id);
        LOGINFO("Created pg={} shard={} (shard {}/{})", pg_id, shard_info.id, i + 1, num_shards);
    }

    // pg with empty shard scrub should report no issues
    run_on_pg_leader(pg_id, [&]() {
        // Deep scrub on PG with empty shards should complete without errors
        auto scrub_report = scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        ASSERT_NE(scrub_report, nullptr) << "Deep scrub report should not be null for PG with empty shards";
        auto deep_scrub_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(scrub_report);
        ASSERT_NE(deep_scrub_report, nullptr) << "Should be DeepScrubReport";

        // PG with empty shards should have no issues
        EXPECT_TRUE(deep_scrub_report->get_corrupted_shards().empty())
            << "PG with empty shards should have no corrupted shards";
        EXPECT_TRUE(deep_scrub_report->get_corrupted_pg_metas().empty())
            << "No PG metas should be corrupted in normal case";
        EXPECT_TRUE(deep_scrub_report->get_inconsistent_shard_metas().empty())
            << "No shard metas should be inconsistent in normal case";
        EXPECT_TRUE(deep_scrub_report->get_missing_shard_ids().empty())
            << "PG with empty shards should have no missing shards";
        EXPECT_TRUE(deep_scrub_report->get_missing_blobs().empty())
            << "PG with empty shards should have no missing blobs";

        EXPECT_TRUE(deep_scrub_report->get_corrupted_blobs().empty())
            << "PG with empty shards should have no corrupted blobs";
        EXPECT_TRUE(deep_scrub_report->get_inconsistent_blobs().empty())
            << "PG with empty shards should have no inconsistent blobs";

        // Shallow scrub on PG with empty shards should complete without errors
        scrub_report = scrub_mgr->submit_scrub_task(pg_id, false /* is_deep */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        EXPECT_TRUE(scrub_report->get_corrupted_shards().empty())
            << "PG with empty shards should have no corrupted shards";
        EXPECT_TRUE(scrub_report->get_corrupted_pg_metas().empty()) << "No PG metas should be corrupted in normal case";
        EXPECT_TRUE(scrub_report->get_inconsistent_shard_metas().empty())
            << "No shard metas should be inconsistent in normal case";
        EXPECT_TRUE(scrub_report->get_missing_shard_ids().empty())
            << "PG with empty shards should have no missing shards";
        EXPECT_TRUE(scrub_report->get_missing_blobs().empty()) << "PG with empty shards should have no missing blobs";
    });

    // Create blobs in all shards
    shard_blob_ids_map = put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);
    LOGINFO("Created {} blobs per shard, total {} blobs", num_blobs_per_shard, num_shards * num_blobs_per_shard);

    // Verify blobs were created
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // everything is healthy, deep scrub should report no issues.
    run_on_pg_leader(pg_id, [&]() {
        // Deep scrub on healthy PG should complete without errors
        auto scrub_report = scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        ASSERT_NE(scrub_report, nullptr) << "Deep scrub report should not be null for healthy PG";
        auto deep_scrub_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(scrub_report);
        ASSERT_NE(deep_scrub_report, nullptr) << "Should be DeepScrubReport";

        // Healthy PG with blobs should have no issues
        EXPECT_TRUE(deep_scrub_report->get_corrupted_shards().empty()) << "Healthy PG should have no corrupted shards";
        EXPECT_TRUE(deep_scrub_report->get_corrupted_pg_metas().empty())
            << "No PG metas should be corrupted in normal case";
        EXPECT_TRUE(deep_scrub_report->get_inconsistent_shard_metas().empty())
            << "No shard metas should be inconsistent in normal case";
        EXPECT_TRUE(deep_scrub_report->get_missing_shard_ids().empty()) << "Healthy PG should have no missing shards";
        EXPECT_TRUE(deep_scrub_report->get_missing_blobs().empty()) << "Healthy PG should have no missing blobs";
        EXPECT_TRUE(deep_scrub_report->get_corrupted_blobs().empty()) << "Healthy PG should have no corrupted blobs";
        EXPECT_TRUE(deep_scrub_report->get_inconsistent_blobs().empty())
            << "Healthy PG should have no inconsistent blobs";

        // Shallow scrub on healthy PG should complete without errors
        scrub_report = scrub_mgr->submit_scrub_task(pg_id, false /* is_deep */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        EXPECT_TRUE(scrub_report->get_corrupted_shards().empty()) << "Healthy PG should have no corrupted shards";
        EXPECT_TRUE(scrub_report->get_corrupted_pg_metas().empty()) << "No PG metas should be corrupted in normal case";
        EXPECT_TRUE(scrub_report->get_inconsistent_shard_metas().empty())
            << "No shard metas should be inconsistent in normal case";
        EXPECT_TRUE(scrub_report->get_missing_shard_ids().empty()) << "Healthy PG should have no missing shards";
        EXPECT_TRUE(scrub_report->get_missing_blobs().empty()) << "Healthy PG should have no missing blobs";
    });

    g_helper->sync();

    const auto hs_pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(hs_pg) << "PG should exist for pg_id=" << pg_id;

    ASSERT_GE(num_shards, 2u) << "BasicScrubTest requires at least 2 shards";
    // First shard: simulate a missing shard (entire shard deleted from followers)
    const auto missing_shard_id = shard_blob_ids_map.begin()->first;
    // Second shard: simulate blob-level issues; this shard still exists on followers
    const auto blob_op_shard_id = std::next(shard_blob_ids_map.begin())->first;
    auto it = shard_blob_ids_map[blob_op_shard_id].begin();
    const auto missing_blob_id = it->first;
    const auto corrupted_blob_id = (++it)->first;
    const auto inconsistent_blob_id = (++it)->first;

    // TODO:: add corruptted shard and corrupted pg meta after we have the implementation for corrupting them.

    // Corrupt data on followers
    run_on_pg_follower(pg_id, [&]() {
        auto& pg_index_table = hs_pg->index_table_;

        // 1. Remove missing_shard_id to simulate missing shard
        delete_shard_from_index(pg_index_table, missing_shard_id);

        // 2. Delete missing_blob_id from blob_op_shard_id (different shard, still exists on follower)
        delete_blob_from_index(pg_index_table, blob_op_shard_id, missing_blob_id);

        // 3. Make corrupted_blob_id corrupted
        corrupt_blob_data(pg_index_table, blob_op_shard_id, corrupted_blob_id);

        // 4. Make inconsistent_blob_id inconsistent (valid but different hash)
        make_blob_inconsistent(pg_index_table, blob_op_shard_id, inconsistent_blob_id, _obj_inst.get());
    });

    g_helper->sync();

    run_on_pg_leader(pg_id, [&]() {
        // do deep scrub and check the scrub report
        auto scrub_report = scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        ASSERT_NE(scrub_report, nullptr) << "Deep scrub report should not be null";
        auto deep_scrub_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(scrub_report);
        ASSERT_NE(deep_scrub_report, nullptr) << "Should be DeepScrubReport";

        const auto& members = (hs_pg->pg_info_).members;
        std::set< peer_id_t > follower_peer_ids;
        const auto& leader_uuid = _obj_inst->our_uuid();
        for (const auto& member : members) {
            if (member.id == leader_uuid) { continue; }
            follower_peer_ids.insert(member.id);
        }

        // Verify missing blobs, missing shards, and corrupted blobs for all followers
        for (const auto& peer_id : follower_peer_ids) {
            verify_missing_blobs(deep_scrub_report, peer_id, BlobRoute{blob_op_shard_id, missing_blob_id});
            verify_missing_shards(deep_scrub_report, peer_id, missing_shard_id);
            verify_corrupted_blobs(deep_scrub_report, peer_id, BlobRoute{blob_op_shard_id, corrupted_blob_id});
        }

        // False positive guard: exactly one shard should be missing, no healthy shard must leak in.
        EXPECT_EQ(deep_scrub_report->get_missing_shard_ids().size(), 1u)
            << "Exactly one shard should be reported missing after follower corruption";

        // False positive guard: each follower must have exactly the one blob we corrupted.
        {
            const auto& all_corrupted = deep_scrub_report->get_corrupted_blobs();
            for (const auto& peer_id : follower_peer_ids) {
                auto cit = all_corrupted.find(peer_id);
                if (cit != all_corrupted.end()) {
                    EXPECT_EQ(cit->second.size(), 1u)
                        << "Follower peer_id=" << peer_id << " should have exactly 1 corrupted blob";
                }
            }
        }

        const auto inconsistent_blobs = deep_scrub_report->get_inconsistent_blobs();
        EXPECT_TRUE(inconsistent_blobs.size() == 1)
            << "Inconsistent blob should be reported in deep scrub report for one of the followers";
        const auto it = inconsistent_blobs.find(BlobRoute{blob_op_shard_id, inconsistent_blob_id});
        EXPECT_TRUE(it != inconsistent_blobs.end())
            << "The inconsistent blob should be reported in deep scrub report for blob_id=" << inconsistent_blob_id;
        auto& inconsistent_blob_peers = it->second;

        // inconsistent_blob_peers should contains all the peers.
        EXPECT_TRUE(inconsistent_blob_peers.size() == follower_peer_ids.size() + 1)
            << "Inconsistent blob should be reported in deep scrub report for all followers";
        for (const auto& peer_id : follower_peer_ids) {
            EXPECT_TRUE(inconsistent_blob_peers.count(peer_id) == 1)
                << "The inconsistent blob should be reported in deep scrub report for peer_id=" << peer_id;
        }
        EXPECT_TRUE(inconsistent_blob_peers.count(leader_uuid) == 1)
            << "The inconsistent blob should be reported in deep scrub report for leader peer_id=" << leader_uuid;

        // do shallow scrub， shallow scrub can only find missing blob/shard
        auto shallow_scrub_report = scrub_mgr->submit_scrub_task(pg_id, false, SCRUB_TRIGGER_TYPE::MANUALLY).get();
        ASSERT_NE(shallow_scrub_report, nullptr) << "Shallow scrub report should not be null";

        auto miss_blob_in_shallow_report = shallow_scrub_report->get_missing_blobs();
        EXPECT_TRUE(miss_blob_in_shallow_report.size() == num_blobs_per_shard + 1)
            << "Should report all blobs from missing_shard_id (" << num_blobs_per_shard
            << ") plus 1 from blob_op_shard_id, got " << miss_blob_in_shallow_report.size();
        {
            auto it = miss_blob_in_shallow_report.find(BlobRoute{blob_op_shard_id, missing_blob_id});
            EXPECT_TRUE(it != miss_blob_in_shallow_report.end())
                << "The missing blob should be reported in shallow scrub report";
            if (it != miss_blob_in_shallow_report.end()) {
                for (const auto& peer_id : follower_peer_ids) {
                    EXPECT_TRUE(it->second.count(peer_id) == 0)
                        << "Follower peer_id=" << peer_id << " should not have the missing blob";
                }
            }
        }

        // Verify each individual blob from missing_shard_id appears in the report.
        // Checking only the total count is insufficient: if the reported blob_ids were wrong
        // but the count matched, the test would still pass.
        for (const auto& [blob_id, _] : shard_blob_ids_map[missing_shard_id]) {
            auto it = miss_blob_in_shallow_report.find(BlobRoute{missing_shard_id, blob_id});
            EXPECT_TRUE(it != miss_blob_in_shallow_report.end())
                << "Blob " << blob_id << " from missing_shard_id=" << missing_shard_id
                << " should be reported in shallow scrub report";
            if (it != miss_blob_in_shallow_report.end()) {
                for (const auto& peer_id : follower_peer_ids) {
                    EXPECT_TRUE(it->second.count(peer_id) == 0)
                        << "Follower peer_id=" << peer_id << " should not have blob " << blob_id
                        << " from missing_shard_id";
                }
            }
        }

        // missing_shard_ids[shard_id] = peers that have the shard; followers are absent from that set.
        const auto missing_shards_in_shallow_report = shallow_scrub_report->get_missing_shard_ids();
        EXPECT_TRUE(missing_shards_in_shallow_report.size() == 1)
            << "One missing shard should be reported in shallow scrub report";
        {
            auto it = missing_shards_in_shallow_report.find(missing_shard_id);
            EXPECT_TRUE(it != missing_shards_in_shallow_report.end())
                << "The missing shard should be reported in shallow scrub report";
            if (it != missing_shards_in_shallow_report.end()) {
                for (const auto& peer_id : follower_peer_ids) {
                    EXPECT_TRUE(it->second.count(peer_id) == 0)
                        << "Follower peer_id=" << peer_id << " should not have the missing shard";
                }
            }
        }
    });

    g_helper->sync();

    // Test case for leader missing/corrupted
    LOGINFO("Starting leader missing/corrupted test case");

    // Get new blob ids for leader corruption test.
    // Must use blob_op_shard_id (not missing_shard_id) because followers deleted the entire
    // missing_shard_id — no hash comparison is possible for blobs in that shard.
    // blob_op_shard_id exists on both leader and followers; skip the first 3 blobs already
    // used by the follower test (missing/corrupted/inconsistent at indices 0/1/2).
    auto& leader_shard_blobs = shard_blob_ids_map[blob_op_shard_id];
    auto leader_it = leader_shard_blobs.begin();
    std::advance(leader_it, 3);
    const auto leader_missing_blob_id = leader_it->first;
    const auto leader_corrupted_blob_id = (++leader_it)->first;
    const auto leader_inconsistent_blob_id = (++leader_it)->first;

    // Corrupt data on leader
    run_on_pg_leader(pg_id, [&]() {
        auto& pg_index_table = hs_pg->index_table_;

        // 1. Delete leader_missing_blob_id from pg_index table on leader
        delete_blob_from_index(pg_index_table, blob_op_shard_id, leader_missing_blob_id);
        LOGINFO("Deleted blob {} from leader index table", leader_missing_blob_id);

        // 2. Make leader_corrupted_blob_id corrupted on leader
        corrupt_blob_data(pg_index_table, blob_op_shard_id, leader_corrupted_blob_id);
        LOGINFO("Corrupted blob {} on leader", leader_corrupted_blob_id);

        // 3. Make leader_inconsistent_blob_id inconsistent on leader
        make_blob_inconsistent(pg_index_table, blob_op_shard_id, leader_inconsistent_blob_id, _obj_inst.get());
        LOGINFO("Made blob {} inconsistent on leader", leader_inconsistent_blob_id);
    });

    g_helper->sync();

    // Run scrub and verify both leader and follower corruptions are detected
    run_on_pg_leader(pg_id, [&]() {
        LOGINFO("Running deep scrub to detect both leader and follower corruptions");
        auto scrub_report = scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        ASSERT_NE(scrub_report, nullptr) << "Deep scrub report should not be null";
        auto deep_scrub_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(scrub_report);
        ASSERT_NE(deep_scrub_report, nullptr) << "Should be DeepScrubReport";

        const auto& leader_uuid = _obj_inst->our_uuid();
        const auto& members = (hs_pg->pg_info_).members;
        std::set< peer_id_t > follower_peer_ids;
        for (const auto& member : members) {
            if (member.id != leader_uuid) { follower_peer_ids.insert(member.id); }
        }

        // ========== Verify Missing Blobs ==========
        LOGINFO("Verifying missing blobs detection");
        verify_missing_blobs(deep_scrub_report, leader_uuid, BlobRoute{blob_op_shard_id, leader_missing_blob_id});
        for (const auto& peer_id : follower_peer_ids) {
            verify_missing_blobs(deep_scrub_report, peer_id, BlobRoute{blob_op_shard_id, missing_blob_id});
        }

        // ========== Verify Missing Shards ==========
        LOGINFO("Verifying missing shards detection");
        for (const auto& peer_id : follower_peer_ids) {
            verify_missing_shards(deep_scrub_report, peer_id, missing_shard_id);
        }

        // ========== Verify Corrupted Blobs ==========
        LOGINFO("Verifying corrupted blobs detection");
        verify_corrupted_blobs(deep_scrub_report, leader_uuid, BlobRoute{blob_op_shard_id, leader_corrupted_blob_id});
        for (const auto& peer_id : follower_peer_ids) {
            verify_corrupted_blobs(deep_scrub_report, peer_id, BlobRoute{blob_op_shard_id, corrupted_blob_id});
        }

        // False positive guard: still only one missing shard (follower's missing_shard_id);
        // leader corruption was blob-level only, no shard should be newly added.
        EXPECT_EQ(deep_scrub_report->get_missing_shard_ids().size(), 1u)
            << "Exactly one shard should be reported missing in combined leader+follower test";

        // False positive guard: each node should have exactly the one blob we corrupted.
        {
            const auto& all_corrupted = deep_scrub_report->get_corrupted_blobs();
            auto lit = all_corrupted.find(leader_uuid);
            if (lit != all_corrupted.end()) {
                EXPECT_EQ(lit->second.size(), 1u) << "Leader should have exactly 1 corrupted blob";
            }
            for (const auto& peer_id : follower_peer_ids) {
                auto fit = all_corrupted.find(peer_id);
                if (fit != all_corrupted.end()) {
                    EXPECT_EQ(fit->second.size(), 1u)
                        << "Follower peer_id=" << peer_id << " should have exactly 1 corrupted blob";
                }
            }
        }

        // ========== Verify Inconsistent Blobs ==========
        const auto inconsistent_blobs = deep_scrub_report->get_inconsistent_blobs();
        LOGINFO("Verifying inconsistent blobs detection, inconsistent_blobs.size()={}", inconsistent_blobs.size());

        // Should have 2 inconsistent blobs: one from follower test, one from leader test
        EXPECT_TRUE(inconsistent_blobs.size() == 2)
            << "Should have 2 inconsistent blobs (1 from follower, 1 from leader)";

        // Verify leader's inconsistent blob
        auto leader_inconsistent_it = inconsistent_blobs.find(BlobRoute{blob_op_shard_id, leader_inconsistent_blob_id});
        EXPECT_TRUE(leader_inconsistent_it != inconsistent_blobs.end())
            << "The leader's inconsistent blob should be reported in deep scrub report";
        if (leader_inconsistent_it != inconsistent_blobs.end()) {
            auto& inconsistent_blob_peers = leader_inconsistent_it->second;
            // All peers including leader should be in the inconsistent blob report
            EXPECT_TRUE(inconsistent_blob_peers.size() == follower_peer_ids.size() + 1)
                << "Leader's inconsistent blob should be reported for all peers including leader";
            EXPECT_TRUE(inconsistent_blob_peers.count(leader_uuid) == 1)
                << "Leader should be in the inconsistent blob peers";
            for (const auto& peer_id : follower_peer_ids) {
                EXPECT_TRUE(inconsistent_blob_peers.count(peer_id) == 1)
                    << "Follower peer_id=" << peer_id << " should be in leader's inconsistent blob peers";
            }
        }

        // Verify follower's inconsistent blob (from earlier test)
        auto follower_inconsistent_it = inconsistent_blobs.find(BlobRoute{blob_op_shard_id, inconsistent_blob_id});
        EXPECT_TRUE(follower_inconsistent_it != inconsistent_blobs.end())
            << "The follower's inconsistent blob should be reported in deep scrub report";
        if (follower_inconsistent_it != inconsistent_blobs.end()) {
            auto& inconsistent_blob_peers = follower_inconsistent_it->second;
            // All peers should be in the inconsistent blob report
            EXPECT_TRUE(inconsistent_blob_peers.size() == follower_peer_ids.size() + 1)
                << "Follower's inconsistent blob should be reported for all peers";
            EXPECT_TRUE(inconsistent_blob_peers.count(leader_uuid) == 1)
                << "Leader should be in follower's inconsistent blob peers";
            for (const auto& peer_id : follower_peer_ids) {
                EXPECT_TRUE(inconsistent_blob_peers.count(peer_id) == 1)
                    << "Follower peer_id=" << peer_id << " should be in follower's inconsistent blob peers";
            }
        }
    });

    g_helper->sync();
}

// Test leader missing an entire shard: followers have the shard but leader's index doesn't.
// Verifies that deep and shallow scrub both detect the missing shard and all its blobs.
TEST_F(HomeObjectFixture, LeaderMissingShardTest) {
    const pg_id_t pg_id = 1;
    create_pg(pg_id);
    auto scrub_mgr = _obj_inst->scrub_manager();

    const uint64_t num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >();
    const uint64_t shard_size = 64 * Mi;

    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    pg_blob_id[pg_id] = 0;

    // Two shards: one will be deleted from the leader's index, one stays healthy on all peers.
    auto missing_shard_info = create_shard(pg_id, shard_size, "leader missing shard");
    auto healthy_shard_info = create_shard(pg_id, shard_size, "healthy shard");
    pg_shard_id_vec[pg_id] = {missing_shard_info.id, healthy_shard_info.id};

    const auto leader_missing_shard_id = missing_shard_info.id;
    const auto healthy_shard_id = healthy_shard_info.id;

    auto shard_blob_ids_map = put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    g_helper->sync();

    const auto hs_pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(hs_pg) << "PG should exist for pg_id=" << pg_id;

    // Simulate leader losing the shard by removing it from the local B-tree index only.
    // Followers still have the shard, so scrub should detect the leader as missing it.
    run_on_pg_leader(pg_id, [&]() {
        delete_shard_from_index(hs_pg->index_table_, leader_missing_shard_id);
        LOGINFO("Deleted shard {} from leader index to simulate leader missing shard", leader_missing_shard_id);
    });

    g_helper->sync();

    run_on_pg_leader(pg_id, [&]() {
        const auto& leader_uuid = _obj_inst->our_uuid();
        std::set< peer_id_t > follower_peer_ids;
        for (const auto& member : hs_pg->pg_info_.members) {
            if (member.id != leader_uuid) { follower_peer_ids.insert(member.id); }
        }

        // ===== Deep scrub =====
        auto scrub_report = scrub_mgr->submit_scrub_task(pg_id, true, SCRUB_TRIGGER_TYPE::MANUALLY).get();
        ASSERT_NE(scrub_report, nullptr) << "Deep scrub report should not be null";
        auto deep_scrub_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(scrub_report);
        ASSERT_NE(deep_scrub_report, nullptr) << "Should be DeepScrubReport";

        // The missing shard must appear in missing_shard_ids.
        // missing_shard_ids[shard_id] = peers that HAVE the shard; leader absent from this set.
        verify_missing_shards(deep_scrub_report, leader_uuid, leader_missing_shard_id);
        {
            const auto& missing_shards = deep_scrub_report->get_missing_shard_ids();
            auto it = missing_shards.find(leader_missing_shard_id);
            ASSERT_TRUE(it != missing_shards.end()) << "leader_missing_shard_id must be in missing_shard_ids";
            for (const auto& peer_id : follower_peer_ids) {
                EXPECT_TRUE(it->second.count(peer_id) == 1)
                    << "Follower peer_id=" << peer_id << " should be in peer set (it has the shard)";
            }
            // Healthy shard must not appear as missing.
            EXPECT_TRUE(missing_shards.find(healthy_shard_id) == missing_shards.end())
                << "Healthy shard should not be reported as missing";
        }

        // Every blob in the missing shard must be reported as missing on the leader.
        for (const auto& [blob_id, _] : shard_blob_ids_map[leader_missing_shard_id]) {
            verify_missing_blobs(deep_scrub_report, leader_uuid, BlobRoute{leader_missing_shard_id, blob_id});
        }

        // Blobs from the healthy shard must not appear in missing_blobs.
        {
            const auto& missing_blobs = deep_scrub_report->get_missing_blobs();
            for (const auto& [blob_id, _] : shard_blob_ids_map[healthy_shard_id]) {
                EXPECT_TRUE(missing_blobs.find(BlobRoute{healthy_shard_id, blob_id}) == missing_blobs.end())
                    << "Healthy blob " << blob_id << " should not be reported as missing";
            }
        }

        // ===== Shallow scrub =====
        auto shallow_scrub_report = scrub_mgr->submit_scrub_task(pg_id, false, SCRUB_TRIGGER_TYPE::MANUALLY).get();
        ASSERT_NE(shallow_scrub_report, nullptr) << "Shallow scrub report should not be null";

        // Missing shard must be detected in shallow scrub as well.
        {
            const auto& shallow_missing_shards = shallow_scrub_report->get_missing_shard_ids();
            auto it = shallow_missing_shards.find(leader_missing_shard_id);
            EXPECT_TRUE(it != shallow_missing_shards.end()) << "Missing shard should appear in shallow scrub report";
            if (it != shallow_missing_shards.end()) {
                EXPECT_TRUE(it->second.count(leader_uuid) == 0) << "Leader should not have the missing shard";
                for (const auto& peer_id : follower_peer_ids) {
                    EXPECT_TRUE(it->second.count(peer_id) == 1)
                        << "Follower peer_id=" << peer_id << " should have the shard";
                }
            }
        }

        // Shallow scrub missing_blobs should contain exactly the blobs from the missing shard.
        EXPECT_EQ(shallow_scrub_report->get_missing_blobs().size(), num_blobs_per_shard)
            << "Missing blob count should equal num_blobs_per_shard";
        {
            const auto& shallow_missing_blobs = shallow_scrub_report->get_missing_blobs();
            for (const auto& [blob_id, _] : shard_blob_ids_map[leader_missing_shard_id]) {
                auto it = shallow_missing_blobs.find(BlobRoute{leader_missing_shard_id, blob_id});
                EXPECT_TRUE(it != shallow_missing_blobs.end())
                    << "Blob " << blob_id << " from missing shard should be in shallow scrub report";
                if (it != shallow_missing_blobs.end()) {
                    EXPECT_TRUE(it->second.count(leader_uuid) == 0) << "Leader should not have blob " << blob_id;
                    for (const auto& peer_id : follower_peer_ids) {
                        EXPECT_TRUE(it->second.count(peer_id) == 1)
                            << "Follower peer_id=" << peer_id << " should have blob " << blob_id;
                    }
                }
            }
        }
    });

    g_helper->sync();
}

// Test scrub superblock persistence across deep and shallow scrubs
TEST_F(HomeObjectFixture, ScrubSuperblockPersistenceTest) {
    const pg_id_t pg_id = 1;
    create_pg(pg_id);

    const uint64_t shard_size = 64 * Mi;
    create_shard(pg_id, shard_size, "shard_meta");
    auto scrub_mgr = _obj_inst->scrub_manager();

    run_on_pg_leader(pg_id, [&]() {
        // Get initial scrub superblock (should be newly created)
        auto initial_sb = scrub_mgr->get_scrub_superblk(pg_id);
        ASSERT_TRUE(initial_sb.has_value()) << "Should have scrub superblock";

        auto initial_deep_scrub_time = initial_sb->last_deep_scrub_timestamp;
        auto initial_shallow_scrub_time = initial_sb->last_shallow_scrub_timestamp;

        // Give some time to ensure timestamps will be different
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // Run a deep scrub
        scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        // Check that deep scrub timestamp updated
        auto after_deep_sb = scrub_mgr->get_scrub_superblk(pg_id);
        ASSERT_TRUE(after_deep_sb.has_value());
        EXPECT_GT(after_deep_sb->last_deep_scrub_timestamp, initial_deep_scrub_time)
            << "Deep scrub timestamp should be updated";
        EXPECT_EQ(after_deep_sb->last_shallow_scrub_timestamp, initial_shallow_scrub_time)
            << "Shallow scrub timestamp should not change after deep scrub";

        std::this_thread::sleep_for(std::chrono::seconds(2));

        // Run a shallow scrub
        scrub_mgr->submit_scrub_task(pg_id, false /* is_deep */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        // Check that shallow scrub timestamp updated
        auto after_shallow_sb = scrub_mgr->get_scrub_superblk(pg_id);
        ASSERT_TRUE(after_shallow_sb.has_value());
        EXPECT_EQ(after_shallow_sb->last_deep_scrub_timestamp, after_deep_sb->last_deep_scrub_timestamp)
            << "Deep scrub timestamp should not change after shallow scrub";
        EXPECT_GT(after_shallow_sb->last_shallow_scrub_timestamp, after_deep_sb->last_shallow_scrub_timestamp)
            << "Shallow scrub timestamp should be updated";
    });

    g_helper->sync();
}

// Test cancel scrub task
TEST_F(HomeObjectFixture, CancelScrubTaskTest) {
    const pg_id_t pg_id = 1;
    create_pg(pg_id);
    auto scrub_mgr = _obj_inst->scrub_manager();

    const uint64_t shard_size = 64 * Mi;
    auto shard_info = create_shard(pg_id, shard_size, "shard meta");

    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    pg_shard_id_vec[pg_id].push_back(shard_info.id);
    pg_blob_id[pg_id] = 0;

    const uint64_t num_blobs = 10;
    put_blobs(pg_shard_id_vec, num_blobs, pg_blob_id);
    g_helper->sync();

    // Submit a scrub task and then cancel it
    run_on_pg_leader(pg_id, [&]() {
        auto scrub_future = scrub_mgr->submit_scrub_task(pg_id, true, SCRUB_TRIGGER_TYPE::MANUALLY);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        scrub_mgr->cancel_scrub_task(pg_id);
        LOGINFO("Cancelled scrub task for pg={}", pg_id);
        auto scrub_report = std::move(scrub_future).get();
        LOGINFO("Scrub task cancelled, report: {}", scrub_report ? "present" : "null");

        // The critical invariant: cancel must clear in_scrubbing so that a subsequent
        // submit_scrub_task is accepted. A null return here means the state was not cleaned up.
        auto followup_report = scrub_mgr->submit_scrub_task(pg_id, true, SCRUB_TRIGGER_TYPE::MANUALLY).get();
        EXPECT_NE(followup_report, nullptr) << "A new scrub task should be accepted after cancellation; "
                                               "null means in_scrubbing was not cleared";
        scrub_mgr->cancel_scrub_task(pg_id);
        LOGINFO("Cancel non-existent scrub task for pg={} - should not crash", pg_id);
    });

    g_helper->sync();
}

// Test concurrent scrubs on multiple PGs
TEST_F(HomeObjectFixture, ConcurrentScrubsOnMultiplePGsTest) {
    const uint64_t num_pgs = 3;
    const uint64_t shard_size = 64 * Mi;

    std::vector< pg_id_t > pg_ids;
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;

    // Create multiple PGs with shards and blobs
    for (uint64_t i = 1; i <= num_pgs; ++i) {
        pg_id_t pg_id = i;
        pg_ids.push_back(pg_id);
        create_pg(pg_id);
        auto shard_info = create_shard(pg_id, shard_size, "shard meta " + std::to_string(pg_id));
        pg_shard_id_vec[pg_id].push_back(shard_info.id);
        pg_blob_id[pg_id] = 0;
        put_blobs(pg_shard_id_vec, 5, pg_blob_id);
    }

    auto scrub_mgr = _obj_inst->scrub_manager();

    // Submit scrub tasks for all PGs concurrently
    std::vector< std::pair< pg_id_t, folly::SemiFuture< std::shared_ptr< ScrubManager::ShallowScrubReport > > > >
        scrub_futures;

    for (const auto& pg_id : pg_ids) {
        run_on_pg_leader(pg_id, [&]() {
            auto future = scrub_mgr->submit_scrub_task(pg_id, true, SCRUB_TRIGGER_TYPE::MANUALLY);
            scrub_futures.emplace_back(pg_id, std::move(future));
            LOGINFO("Submitted deep scrub for pg={}", pg_id);
        });
    }

    // Wait for all scrub tasks to complete and verify each report is clean
    for (auto& [pg_id, future] : scrub_futures) {
        auto report = std::move(future).get();
        ASSERT_NE(report, nullptr) << "Scrub report should not be null for pg=" << pg_id;

        auto deep_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(report);
        ASSERT_NE(deep_report, nullptr) << "Should be DeepScrubReport for pg=" << pg_id;

        EXPECT_TRUE(deep_report->get_missing_shard_ids().empty()) << "pg=" << pg_id << " should have no missing shards";
        EXPECT_TRUE(deep_report->get_missing_blobs().empty()) << "pg=" << pg_id << " should have no missing blobs";
        EXPECT_TRUE(deep_report->get_corrupted_blobs().empty()) << "pg=" << pg_id << " should have no corrupted blobs";
        EXPECT_TRUE(deep_report->get_inconsistent_blobs().empty())
            << "pg=" << pg_id << " should have no inconsistent blobs";
        LOGINFO("PG {} concurrent scrub completed cleanly", pg_id);
    }

    g_helper->sync();
}

// Test deleted blob filter in scrub report
TEST_F(HomeObjectFixture, ReconcileScrubReportTest) {
    const pg_id_t pg_id = 1;
    create_pg(pg_id);
    auto scrub_mgr = _obj_inst->scrub_manager();

    const uint64_t shard_size = 64 * Mi;
    auto shard_info = create_shard(pg_id, shard_size, "shard meta");

    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    pg_shard_id_vec[pg_id].push_back(shard_info.id);
    pg_blob_id[pg_id] = 0;

    std::map< shard_id_t, std::map< blob_id_t, uint64_t > > shard_blob_ids_map;

    // Create some blobs
    const uint64_t num_blobs = 10;
    shard_blob_ids_map = put_blobs(pg_shard_id_vec, num_blobs, pg_blob_id);
    const auto hs_pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(hs_pg) << "PG should exist for pg_id=" << pg_id;

    const auto shard_id = shard_info.id;
    auto& shard_blobs = shard_blob_ids_map[shard_id];

    // Select blobs to test:
    // - missing_blob_to_delete: will be missing from leader index AND deleted via blob delete
    // - missing_blob_not_deleted: will be missing from leader index but NOT deleted
    auto it = shard_blobs.begin();
    const auto missing_blob_to_delete = it->first;       // First blob: will be deleted via blob delete
    const auto missing_blob_not_deleted = (++it)->first; // Second blob: will NOT be deleted

    // Delete both blobs from index table to simulate missing blobs on followers
    run_on_pg_follower(pg_id, [&]() {
        auto& pg_index_table = hs_pg->index_table_;
        delete_blob_from_index(pg_index_table, shard_id, missing_blob_to_delete);
        delete_blob_from_index(pg_index_table, shard_id, missing_blob_not_deleted);
        LOGINFO("Deleted blobs {} and {} from follower index table", missing_blob_to_delete, missing_blob_not_deleted);
    });

    g_helper->sync();

    run_on_pg_leader(pg_id, [&]() {
        // only the blob that was deleted via blob delete should be filtered out, the other missing blob should be
        // reported in the scrub report
        std::set< peer_id_t > follower_peer_ids;
        const auto& leader_uuid = _obj_inst->our_uuid();
        const auto& members = (hs_pg->pg_info_).members;
        for (const auto& member : members) {
            if (member.id == leader_uuid) { continue; }
            follower_peer_ids.insert(member.id);
        }

        auto scrub_report =
            scrub_mgr->submit_scrub_task(pg_id, false /* shallow */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        // missing_blobs[blob_route] = peers that have the blob; followers are absent from that set.
        auto missing_blobs = scrub_report->get_missing_blobs();
        EXPECT_TRUE(missing_blobs.size() == 2) << "There should be two missing blobs in scrub report";
        for (const auto& blob_route :
             {BlobRoute{shard_id, missing_blob_to_delete}, BlobRoute{shard_id, missing_blob_not_deleted}}) {
            auto it = missing_blobs.find(blob_route);
            ASSERT_TRUE(it != missing_blobs.end()) << "Missing blob should be reported in scrub report";
            for (const auto& peer_id : follower_peer_ids) {
                EXPECT_TRUE(it->second.count(peer_id) == 0)
                    << "Follower peer_id=" << peer_id << " should not have the missing blob";
            }
        }

#ifdef _PRERELEASE
        set_callback_flip(
            "delete_missing_blob_through_raft", std::function< void() >([this, missing_blob_to_delete, shard_id]() {
                auto ret =
                    _obj_inst->blob_manager()->del(shard_id, missing_blob_to_delete, generateRandomTraceId()).get();
                if (!ret) {
                    FAIL() << "Blob deletion via raft failed for shard=" << shard_id
                           << " blob=" << missing_blob_to_delete << ", error=" << fmt::format("{}", ret.error());
                } else {
                    LOGINFO("Successfully deleted blob {} in shard {} via raft", missing_blob_to_delete, shard_id);
                }
                // wait for the del_blob to be committed on all the replicas.
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }));

        scrub_report = scrub_mgr->submit_scrub_task(pg_id, false /* shallow */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        remove_flip("delete_missing_blob_through_raft");

        // Verify the scrub report
        ASSERT_NE(scrub_report, nullptr) << "Scrub report should not be null";

        missing_blobs = scrub_report->get_missing_blobs();
        EXPECT_TRUE(missing_blobs.size() == 1) << "There should be one missing blob in scrub report after deletion";
        {
            auto it = missing_blobs.find(BlobRoute{shard_id, missing_blob_not_deleted});
            ASSERT_TRUE(it != missing_blobs.end())
                << "The missing blob that was not deleted should be reported in scrub report";
            for (const auto& peer_id : follower_peer_ids) {
                EXPECT_TRUE(it->second.count(peer_id) == 0)
                    << "Follower peer_id=" << peer_id << " should not have the missing blob";
            }
        }
#endif
    });

    g_helper->sync();
}

// Test add and remove PG from scrub manager
TEST_F(HomeObjectFixture, AddRemovePGScrubTest) {
    const pg_id_t pg_id = 1;
    const uint64_t shard_size = 64 * Mi;

    // Create PG and verify scrub superblock is created
    create_pg(pg_id);
    create_shard(pg_id, shard_size, "shard meta");

    auto scrub_mgr = _obj_inst->scrub_manager();

    // Verify scrub superblock exists
    run_on_pg_leader(pg_id, [&]() {
        auto sb = scrub_mgr->get_scrub_superblk(pg_id);
        ASSERT_TRUE(sb.has_value()) << "Scrub superblock should exist after PG creation";
        LOGINFO("Scrub superblock created for pg={}", pg_id);
    });

    // Run a scrub to update timestamps
    run_on_pg_leader(pg_id, [&]() {
        // Get initial timestamp before scrub
        auto sb_before = scrub_mgr->get_scrub_superblk(pg_id);
        ASSERT_TRUE(sb_before.has_value()) << "Scrub superblock should exist before scrub";
        uint64_t timestamp_before = sb_before->last_shallow_scrub_timestamp;
        LOGINFO("Timestamp before scrub: {}", timestamp_before);

        // Wait a bit to ensure timestamp will be different
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        auto report = scrub_mgr->submit_scrub_task(pg_id, false, SCRUB_TRIGGER_TYPE::MANUALLY).get();
        ASSERT_NE(report, nullptr) << "Scrub report should not be null";

        // Verify timestamp was updated after scrub
        auto sb_after = scrub_mgr->get_scrub_superblk(pg_id);
        ASSERT_TRUE(sb_after.has_value()) << "Scrub superblock should exist after scrub";
        uint64_t timestamp_after = sb_after->last_shallow_scrub_timestamp;
        EXPECT_GT(timestamp_after, timestamp_before) << "Shallow scrub timestamp should be updated after scrub";
        LOGINFO("Timestamp after scrub: {} (updated from {})", timestamp_after, timestamp_before);
    });
    g_helper->sync();

    // Now delete the PG - this should cancel any running scrub and remove superblock
    _obj_inst->pg_manager()->destroy_pg(pg_id);
    auto report = scrub_mgr->submit_scrub_task(pg_id, false, SCRUB_TRIGGER_TYPE::MANUALLY).get();
    ASSERT_EQ(report, nullptr) << "Scrub report should be null after PG deletion";
    LOGINFO("Scrub task for deleted pg={} returned null report as expected", pg_id);

    // Verify scrub superblock is cleaned up: remove_pg erases the entry from m_pg_scrub_sb_map,
    // so get_scrub_superblk must return nullopt after the PG is fully deleted.
    auto cleaned_sb = scrub_mgr->get_scrub_superblk(pg_id);
    EXPECT_FALSE(cleaned_sb.has_value()) << "Scrub superblock should be removed from scrub manager after PG deletion";
    LOGINFO("PG deleted, scrub superblock correctly cleaned up");
}

// Test local scrub methods
TEST_F(HomeObjectFixture, LocalScrubMethodsTest) {
    const pg_id_t pg_id = 1;
    create_pg(pg_id);
    auto scrub_mgr = _obj_inst->scrub_manager();

    const uint64_t shard_size = 64 * Mi;
    auto shard_info = create_shard(pg_id, shard_size, "shard meta");

    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    pg_shard_id_vec[pg_id].push_back(shard_info.id);
    pg_blob_id[pg_id] = 0;

    // Create blobs first
    const uint64_t num_blobs = 10;
    auto shard_blob_ids_map = put_blobs(pg_shard_id_vec, num_blobs, pg_blob_id);
    LOGINFO("Created {} blobs for local scrub test", num_blobs);
    const auto hs_pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(hs_pg) << "PG should exist for pg_id=" << pg_id;

    const auto shard_id = shard_info.id;
    auto& shard_blobs = shard_blob_ids_map[shard_id];

    auto it = shard_blobs.begin();
    const auto corrupted_blob_id = it->first;

    // corrupt one blob's data in the index table to simulate a blob-level corruption that should be detected by local
    // blob scrub.
    auto& pg_index_table = hs_pg->index_table_;
    corrupt_blob_data(pg_index_table, shard_id, corrupted_blob_id);

    auto my_uuid = _obj_inst->our_uuid();
    const int64_t scrub_lsn = hs_pg->repl_dev_->get_last_commit_lsn();

    // Test local_scrub_meta: covers range [0, shard_id]
    auto meta_req = std::make_shared< ScrubManager::scrub_req >(pg_id, 1, scrub_lsn, 0, 0, shard_id, UINT64_MAX,
                                                                SCRUB_TYPE::META, my_uuid);
    auto meta_result = scrub_mgr->local_scrub_meta(meta_req);
    ASSERT_NE(meta_result, nullptr) << "local_scrub_meta should return a result";
    LOGINFO("Meta scrub completed, {} entries", meta_result->entries.size());

    // Test local_scrub_blob (shallow): all entries should carry ScrubStatus::NONE
    auto shallow_req = std::make_shared< ScrubManager::scrub_req >(pg_id, 2, scrub_lsn, shard_id, 0, shard_id,
                                                                   UINT64_MAX, SCRUB_TYPE::SHALLOW_BLOB, my_uuid);
    auto shallow_result = scrub_mgr->local_scrub_blob(shallow_req);
    ASSERT_NE(shallow_result, nullptr) << "local_scrub_blob (shallow) should return a result";
    LOGINFO("Shallow blob scrub completed, {} entries", shallow_result->entries.size());
    for (const auto& [route, val] : shallow_result->entries) {
        auto* status = std::get_if< ScrubStatus >(&val);
        ASSERT_TRUE(status != nullptr) << "Shallow entry should carry ScrubStatus";
        EXPECT_EQ(*status, ScrubStatus::NONE) << "Shallow blob entry should be NONE";
    }

    // Test local_scrub_blob (deep): should detect the corrupted blob
    auto deep_req = std::make_shared< ScrubManager::scrub_req >(pg_id, 3, scrub_lsn, shard_id, 0, shard_id, UINT64_MAX,
                                                                SCRUB_TYPE::DEEP_BLOB, my_uuid);
    auto deep_result = scrub_mgr->local_scrub_blob(deep_req);
    ASSERT_NE(deep_result, nullptr) << "local_scrub_blob (deep) should return a result";
    LOGINFO("Deep blob scrub completed, {} entries", deep_result->entries.size());

    auto corrupted_it = deep_result->entries.find(BlobRoute{shard_id, corrupted_blob_id});
    EXPECT_TRUE(corrupted_it != deep_result->entries.end()) << "Corrupted blob should appear in deep scrub result";
    if (corrupted_it != deep_result->entries.end()) {
        auto* status = std::get_if< ScrubStatus >(&corrupted_it->second);
        ASSERT_TRUE(status != nullptr) << "Corrupted blob result should be ScrubStatus";
        EXPECT_EQ(*status, ScrubStatus::MISMATCH) << "Corrupted blob should have MISMATCH status";
        LOGINFO("Deep scrub correctly detected corrupted blob {}", corrupted_blob_id);
    }
}

// Test scrub request serialization and deserialization
TEST_F(HomeObjectFixture, ScrubRequestSerializationTest) {
    const pg_id_t pg_id = 10;
    auto my_uuid = _obj_inst->our_uuid();

    // Test META scrub_req serialization
    {
        auto req = std::make_shared< ScrubManager::scrub_req >(pg_id, 1, 100, 0, 0, UINT64_MAX, UINT64_MAX,
                                                               SCRUB_TYPE::META, my_uuid);
        auto buffer = req->build_flat_buffer();
        EXPECT_GT(buffer.size(), 0) << "Serialized buffer should not be empty";
        auto req_loaded = std::make_shared< ScrubManager::scrub_req >();
        bool load_success = req_loaded->load(buffer.data(), buffer.size());
        EXPECT_TRUE(load_success) << "Deserialization should succeed";

        EXPECT_EQ(req_loaded->pg_id, pg_id);
        EXPECT_EQ(req_loaded->req_id, 1u);
        EXPECT_EQ(req_loaded->scrub_lsn, 100);
        EXPECT_EQ(req_loaded->start_shard_id, 0u);
        EXPECT_EQ(req_loaded->end_shard_id, UINT64_MAX);
        EXPECT_EQ(req_loaded->scrub_type, SCRUB_TYPE::META);
        EXPECT_EQ(req_loaded->issuer_peer_id, my_uuid);
    }

    // Test DEEP_BLOB scrub_req serialization
    {
        auto req = std::make_shared< ScrubManager::scrub_req >(pg_id, 2, 200, 100, 0, 200, UINT64_MAX,
                                                               SCRUB_TYPE::DEEP_BLOB, my_uuid);
        auto buffer = req->build_flat_buffer();
        EXPECT_GT(buffer.size(), 0);
        auto req_loaded = std::make_shared< ScrubManager::scrub_req >();
        bool load_success = req_loaded->load(buffer.data(), buffer.size());
        EXPECT_TRUE(load_success);

        EXPECT_EQ(req_loaded->pg_id, pg_id);
        EXPECT_EQ(req_loaded->req_id, 2u);
        EXPECT_EQ(req_loaded->scrub_lsn, 200);
        EXPECT_EQ(req_loaded->start_shard_id, 100u);
        EXPECT_EQ(req_loaded->end_shard_id, 200u);
        EXPECT_EQ(req_loaded->scrub_type, SCRUB_TYPE::DEEP_BLOB);
    }

    // Test SHALLOW_BLOB scrub_req serialization
    {
        auto req = std::make_shared< ScrubManager::scrub_req >(pg_id, 3, 300, 0, 0, 100, UINT64_MAX,
                                                               SCRUB_TYPE::SHALLOW_BLOB, my_uuid);
        auto buffer = req->build_flat_buffer();
        EXPECT_GT(buffer.size(), 0);
        auto req_loaded = std::make_shared< ScrubManager::scrub_req >();
        bool load_success = req_loaded->load(buffer.data(), buffer.size());
        EXPECT_TRUE(load_success);

        EXPECT_EQ(req_loaded->pg_id, pg_id);
        EXPECT_EQ(req_loaded->req_id, 3u);
        EXPECT_EQ(req_loaded->scrub_lsn, 300);
        EXPECT_EQ(req_loaded->start_shard_id, 0u);
        EXPECT_EQ(req_loaded->end_shard_id, 100u);
        EXPECT_EQ(req_loaded->scrub_type, SCRUB_TYPE::SHALLOW_BLOB);
    }
}

// Test scrub_result serialization and deserialization.
// scrub_result carries three distinct entry kinds that follow different wire/load paths:
//   - uint64_t hash  : written as (status=NONE, hash=value); loaded back as uint64_t
//   - ScrubStatus::NONE : written as (status=NONE, hash=0);   loaded back as uint64_t(0)
//   - non-NONE ScrubStatus: written as (status=X, hash=0);    loaded back as ScrubStatus
TEST_F(HomeObjectFixture, ScrubResultSerializationTest) {
    const auto my_uuid = _obj_inst->our_uuid();
    const uint64_t req_id = 99;

    // ---- Case 1: hash entries (deep blob scrub) ----
    {
        auto result = std::make_shared< ScrubManager::scrub_result >(req_id, my_uuid);
        result->add_entry({10, 1, uint64_t{0xDEADBEEFCAFEBABEULL}});
        result->add_entry({10, 2, uint64_t{0x123456789ABCDEF0ULL}});

        auto buf = result->build_flat_buffer();
        EXPECT_GT(buf.size(), 0u);

        auto loaded = std::make_shared< ScrubManager::scrub_result >();
        EXPECT_TRUE(loaded->load(buf.data(), buf.size()));

        EXPECT_EQ(loaded->req_id, req_id);
        EXPECT_EQ(loaded->issuer_peer_id, my_uuid);
        EXPECT_EQ(loaded->entries.size(), 2u);

        auto it1 = loaded->entries.find(BlobRoute{10, 1});
        ASSERT_NE(it1, loaded->entries.end());
        auto* h1 = std::get_if< uint64_t >(&it1->second);
        ASSERT_NE(h1, nullptr) << "Hash entry should deserialize as uint64_t";
        EXPECT_EQ(*h1, 0xDEADBEEFCAFEBABEULL);

        auto it2 = loaded->entries.find(BlobRoute{10, 2});
        ASSERT_NE(it2, loaded->entries.end());
        auto* h2 = std::get_if< uint64_t >(&it2->second);
        ASSERT_NE(h2, nullptr);
        EXPECT_EQ(*h2, 0x123456789ABCDEF0ULL);
    }

    // ---- Case 2: ScrubStatus::NONE (shallow blob scrub existence entries) ----
    // On the wire NONE maps to (status=NONE, hash=0) and loads back as uint64_t(0).
    {
        auto result = std::make_shared< ScrubManager::scrub_result >(req_id + 1, my_uuid);
        result->add_entry({20, 1, ScrubStatus::NONE});
        result->add_entry({20, 2, ScrubStatus::NONE});

        auto buf = result->build_flat_buffer();
        auto loaded = std::make_shared< ScrubManager::scrub_result >();
        EXPECT_TRUE(loaded->load(buf.data(), buf.size()));

        EXPECT_EQ(loaded->entries.size(), 2u);
        for (const BlobRoute& route : {BlobRoute{20, 1}, BlobRoute{20, 2}}) {
            auto it = loaded->entries.find(route);
            ASSERT_NE(it, loaded->entries.end());
            auto* h = std::get_if< uint64_t >(&it->second);
            ASSERT_NE(h, nullptr) << "NONE entry should deserialize as uint64_t(0)";
            EXPECT_EQ(*h, 0u);
        }
    }

    // ---- Case 3: error status entries (IO_ERROR, MISMATCH) ----
    {
        auto result = std::make_shared< ScrubManager::scrub_result >(req_id + 2, my_uuid);
        result->add_entry({30, 1, ScrubStatus::IO_ERROR});
        result->add_entry({30, 2, ScrubStatus::MISMATCH});

        auto buf = result->build_flat_buffer();
        auto loaded = std::make_shared< ScrubManager::scrub_result >();
        EXPECT_TRUE(loaded->load(buf.data(), buf.size()));

        EXPECT_EQ(loaded->entries.size(), 2u);

        auto it1 = loaded->entries.find(BlobRoute{30, 1});
        ASSERT_NE(it1, loaded->entries.end());
        auto* s1 = std::get_if< ScrubStatus >(&it1->second);
        ASSERT_NE(s1, nullptr) << "IO_ERROR entry should deserialize as ScrubStatus";
        EXPECT_EQ(*s1, ScrubStatus::IO_ERROR);

        auto it2 = loaded->entries.find(BlobRoute{30, 2});
        ASSERT_NE(it2, loaded->entries.end());
        auto* s2 = std::get_if< ScrubStatus >(&it2->second);
        ASSERT_NE(s2, nullptr) << "MISMATCH entry should deserialize as ScrubStatus";
        EXPECT_EQ(*s2, ScrubStatus::MISMATCH);
    }

    // ---- Case 4: empty result ----
    {
        auto result = std::make_shared< ScrubManager::scrub_result >(req_id + 3, my_uuid);
        auto buf = result->build_flat_buffer();
        auto loaded = std::make_shared< ScrubManager::scrub_result >();
        EXPECT_TRUE(loaded->load(buf.data(), buf.size()));
        EXPECT_EQ(loaded->req_id, req_id + 3);
        EXPECT_TRUE(loaded->entries.empty()) << "Empty result should round-trip with no entries";
    }
}
