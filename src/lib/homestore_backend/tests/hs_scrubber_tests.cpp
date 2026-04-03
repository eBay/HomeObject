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
    BlobRouteKey blob_key{BlobRoute{shard_id, blob_id}};
    BlobRouteValue out_value;
    homestore::BtreeSingleRemoveRequest remove_req{&blob_key, &out_value};
    auto status = pg_index_table->remove(remove_req);
    ASSERT_TRUE(status == homestore::btree_status_t::success) << "Failed to remove blob key from index table";
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

            auto* data_ptr = reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base);
            for (size_t i = 0; i <= data_sgs.iovs[0].iov_len / 2; i++) {
                data_ptr[i] ^= 0xFF; // Flip first half of data
            }

            return data_service.async_write(data_sgs, pbas).thenValue([data_sgs = std::move(data_sgs)](auto&& err) {
                ASSERT_FALSE(err) << "Failed to write corrupted blob data";
                iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
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

            for (size_t i = 0; i <= header->blob_size / 2; i++) {
                blob_bytes[i] ^= static_cast< uint8_t >(dist(rng));
            }

            std::string user_key = header->user_key_size
                ? std::string((const char*)(read_buf + sizeof(BlobHeader)), (size_t)header->user_key_size)
                : std::string{};

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
                ASSERT_FALSE(err) << "Failed to write inconsistent blob data";
                iomanager.iobuf_free(reinterpret_cast< uint8_t* >(data_sgs.iovs[0].iov_base));
            });
        })
        .get();
}

// Helper function to verify missing blobs in scrub report
static void verify_missing_blobs(const ScrubManager::DeepScrubReport* report, const peer_id_t& peer_id,
                                 const BlobRoute& expected_blob) {
    const auto& missing_blobs = report->get_missing_blobs();
    auto it = missing_blobs.find(peer_id);
    EXPECT_TRUE(it != missing_blobs.end()) << "Missing blob should be reported for peer_id=" << peer_id;
    if (it != missing_blobs.end()) {
        EXPECT_TRUE(it->second.count(expected_blob) == 1) << "Expected missing blob should be in the report";
    }
}

// Helper function to verify corrupted blobs in scrub report
static void verify_corrupted_blobs(const ScrubManager::DeepScrubReport* report, const peer_id_t& peer_id,
                                   const BlobRoute& expected_blob) {
    const auto& corrupted_blobs = report->get_corrupted_blobs();
    auto it = corrupted_blobs.find(peer_id);
    EXPECT_TRUE(it != corrupted_blobs.end()) << "Corrupted blob should be reported for peer_id=" << peer_id;
    if (it != corrupted_blobs.end()) {
        EXPECT_TRUE(it->second.count(expected_blob) == 1) << "Expected corrupted blob should be in the report";
    }
}

// Helper function to verify missing shards in scrub report
static void verify_missing_shards(const ScrubManager::DeepScrubReport* report, const peer_id_t& peer_id,
                                  shard_id_t expected_shard) {
    const auto& missing_shards = report->get_missing_shard_ids();
    auto it = missing_shards.find(peer_id);
    EXPECT_TRUE(it != missing_shards.end()) << "Missing shard should be reported for peer_id=" << peer_id;
    if (it != missing_shards.end()) {
        EXPECT_TRUE(it->second.count(expected_shard) == 1) << "Expected missing shard should be in the report";
    }
}

TEST_F(HomeObjectFixture, BasicScrubTest) {
    const pg_id_t pg_id = 1;
    create_pg(pg_id);
    auto scrub_mgr = _obj_inst->scrub_manager();

    // empty pg scrub should report no issues
    run_on_pg_leader(pg_id, [&]() {
        // Deep scrub on empty PG should complete without errors
        auto scrub_report =
            scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY)
                .get();

        ASSERT_NE(scrub_report, nullptr) << "Deep scrub report should not be null for empty PG";
        auto deep_scrub_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(scrub_report);
        ASSERT_NE(deep_scrub_report, nullptr) << "Should be DeepScrubReport";

        // Empty PG should have no issues
        EXPECT_TRUE(deep_scrub_report->get_missing_blobs().empty()) << "Empty PG should have no missing blobs";
        EXPECT_TRUE(deep_scrub_report->get_missing_shard_ids().empty()) << "Empty PG should have no missing shards";
        EXPECT_TRUE(deep_scrub_report->get_corrupted_blobs().empty()) << "Empty PG should have no corrupted blobs";
        EXPECT_TRUE(deep_scrub_report->get_corrupted_shards().empty()) << "Empty PG should have no corrupted shards";
        EXPECT_TRUE(deep_scrub_report->get_inconsistent_blobs().empty())
            << "Empty PG should have no inconsistent blobs";

        // Shallow scrub on empty PG
        scrub_report =
            scrub_mgr->submit_scrub_task(pg_id, false /* is_deep */, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY)
                .get();

        ASSERT_NE(scrub_report, nullptr) << "Shallow scrub report should not be null for empty PG";
        auto shallow_scrub_report = std::dynamic_pointer_cast< ScrubManager::ShallowScrubReport >(scrub_report);
        ASSERT_NE(shallow_scrub_report, nullptr) << "Should be ShallowScrubReport";

        EXPECT_TRUE(shallow_scrub_report->get_missing_blobs().empty())
            << "Empty PG should have no missing blobs in shallow scrub";
        EXPECT_TRUE(shallow_scrub_report->get_missing_shard_ids().empty())
            << "Empty PG should have no missing shards in shallow scrub";
    });

    const uint64_t num_shards = SISL_OPTIONS["num_shards"].as< uint64_t >();
    const uint64_t num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >();
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
        // Deep scrub on empty PG should complete without errors
        auto scrub_report =
            scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY)
                .get();

        ASSERT_NE(scrub_report, nullptr) << "Deep scrub report should not be null for empty PG";
        auto deep_scrub_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(scrub_report);
        ASSERT_NE(deep_scrub_report, nullptr) << "Should be DeepScrubReport";

        // Empty PG should have no issues
        EXPECT_TRUE(deep_scrub_report->get_missing_blobs().empty()) << "Empty PG should have no missing blobs";
        EXPECT_TRUE(deep_scrub_report->get_missing_shard_ids().empty()) << "Empty PG should have no missing shards";
        EXPECT_TRUE(deep_scrub_report->get_corrupted_blobs().empty()) << "Empty PG should have no corrupted blobs";
        EXPECT_TRUE(deep_scrub_report->get_corrupted_shards().empty()) << "Empty PG should have no corrupted shards";
        EXPECT_TRUE(deep_scrub_report->get_inconsistent_blobs().empty())
            << "Empty PG should have no inconsistent blobs";

        // Shallow scrub on empty PG
        scrub_report =
            scrub_mgr->submit_scrub_task(pg_id, false /* is_deep */, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY)
                .get();

        ASSERT_NE(scrub_report, nullptr) << "Shallow scrub report should not be null for empty PG";
        auto shallow_scrub_report = std::dynamic_pointer_cast< ScrubManager::ShallowScrubReport >(scrub_report);
        ASSERT_NE(shallow_scrub_report, nullptr) << "Should be ShallowScrubReport";

        EXPECT_TRUE(shallow_scrub_report->get_missing_blobs().empty())
            << "Empty PG should have no missing blobs in shallow scrub";
        EXPECT_TRUE(shallow_scrub_report->get_missing_shard_ids().empty())
            << "Empty PG should have no missing shards in shallow scrub";
    });

    // Create blobs in all shards
    shard_blob_ids_map = put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);
    LOGINFO("Created {} blobs per shard, total {} blobs", num_blobs_per_shard, num_shards * num_blobs_per_shard);

    // Verify blobs were created
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // everything is healthy, deep scrub should report no issues.
    run_on_pg_leader(pg_id, [&]() {
        // do deep scrub
        auto scrub_report =
            scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY)
                .get();

        ASSERT_NE(scrub_report, nullptr) << "Deep scrub report should not be null";
        auto deep_scrub_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(scrub_report);
        ASSERT_NE(deep_scrub_report, nullptr) << "Should be DeepScrubReport";

        EXPECT_TRUE(deep_scrub_report->get_missing_blobs().empty()) << "No blobs should be missing in normal case";
        EXPECT_TRUE(deep_scrub_report->get_missing_shard_ids().empty()) << "No shards should be missing in normal case";
        EXPECT_TRUE(deep_scrub_report->get_corrupted_blobs().empty()) << "No blobs should be corrupted in normal case";
        EXPECT_TRUE(deep_scrub_report->get_corrupted_shards().empty())
            << "No shards should be corrupted in normal case";
        EXPECT_TRUE(deep_scrub_report->get_inconsistent_blobs().empty())
            << "No blobs should be inconsistent in normal case";
        EXPECT_TRUE(deep_scrub_report->get_corrupted_pg_metas().empty())
            << "No PG metas should be corrupted in normal case";

        // do shallow scrub
        scrub_report =
            scrub_mgr->submit_scrub_task(pg_id, false, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY).get();
        ASSERT_NE(scrub_report, nullptr) << "Shallow scrub report should not be null";
        auto shallow_scrub_report = std::dynamic_pointer_cast< ScrubManager::ShallowScrubReport >(scrub_report);
        ASSERT_NE(shallow_scrub_report, nullptr) << "Should be ShallowScrubReport";
        EXPECT_TRUE(shallow_scrub_report->get_missing_blobs().empty()) << "No blobs should be missing in normal case";
        EXPECT_TRUE(shallow_scrub_report->get_missing_shard_ids().empty())
            << "No shards should be missing in normal case";
    });

    g_helper->sync();

    const auto hs_pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(hs_pg) << "PG should exist for pg_id=" << pg_id;

    const auto missing_shard_id = shard_blob_ids_map.begin()->first;
    auto it = shard_blob_ids_map[missing_shard_id].begin();
    const auto missing_blob_id = it->first;
    const auto corrupted_blob_id = (++it)->first;
    const auto inconsistent_blob_id = (++it)->first;

    // TODO:: add corruptted shard and corrupted pg meta after we have the implementation for corrupting them.

    // Corrupt data on followers
    run_on_pg_follower(pg_id, [&]() {
        auto& pg_index_table = hs_pg->index_table_;

        // 1. Remove missing_shard_id to simulate missing shard
        _obj_inst->delete_shard_from_map(missing_shard_id);

        // 2. Delete missing_blob_id from pg_index table
        delete_blob_from_index(pg_index_table, missing_shard_id, missing_blob_id);

        // 3. Make corrupted_blob_id corrupted
        corrupt_blob_data(pg_index_table, missing_shard_id, corrupted_blob_id);

        // 4. Make inconsistent_blob_id inconsistent (valid but different hash)
        make_blob_inconsistent(pg_index_table, missing_shard_id, inconsistent_blob_id, _obj_inst.get());
    });

    g_helper->sync();

    run_on_pg_leader(pg_id, [&]() {
        // do deep scrub and check the scrub report
        auto scrub_report =
            scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY)
                .get();

        ASSERT_NE(scrub_report, nullptr) << "Deep scrub report should not be null";
        auto deep_scrub_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(scrub_report);
        ASSERT_NE(deep_scrub_report, nullptr) << "Should be DeepScrubReport";
        deep_scrub_report->print();

        const auto& members = (hs_pg->pg_info_).members;
        std::set< peer_id_t > follower_peer_ids;
        const auto& leader_uuid = _obj_inst->our_uuid();
        for (const auto& member : members) {
            if (member.id == leader_uuid) { continue; }
            follower_peer_ids.insert(member.id);
        }

        // Verify missing blobs, missing shards, and corrupted blobs for all followers
        for (const auto& peer_id : follower_peer_ids) {
            verify_missing_blobs(deep_scrub_report.get(), peer_id, BlobRoute{missing_shard_id, missing_blob_id});
            verify_missing_shards(deep_scrub_report.get(), peer_id, missing_shard_id);
            verify_corrupted_blobs(deep_scrub_report.get(), peer_id, BlobRoute{missing_shard_id, corrupted_blob_id});
        }

        const auto inconsistent_blobs = deep_scrub_report->get_inconsistent_blobs();
        EXPECT_TRUE(inconsistent_blobs.size() == 1)
            << "Inconsistent blob should be reported in deep scrub report for one of the followers";
        const auto it = inconsistent_blobs.find(BlobRoute{missing_shard_id, inconsistent_blob_id});
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
        scrub_report =
            scrub_mgr->submit_scrub_task(pg_id, false, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY).get();
        ASSERT_NE(scrub_report, nullptr) << "Shallow scrub report should not be null";
        auto shallow_scrub_report = std::dynamic_pointer_cast< ScrubManager::ShallowScrubReport >(scrub_report);
        ASSERT_NE(shallow_scrub_report, nullptr) << "Should be ShallowScrubReport";
        shallow_scrub_report->print();

        auto miss_blob_in_shallow_report = shallow_scrub_report->get_missing_blobs();
        EXPECT_TRUE(miss_blob_in_shallow_report.size() == follower_peer_ids.size())
            << "Missing blob should be reported in shallow scrub report for all followers";
        for (const auto& peer_id : follower_peer_ids) {
            auto it = miss_blob_in_shallow_report.find(peer_id);
            EXPECT_TRUE(it != miss_blob_in_shallow_report.end())
                << "Missing blob should be reported in shallow scrub report for peer_id=" << peer_id;
            EXPECT_TRUE(it->second.size() == 1)
                << "There should be one missing blob for each peer in shallow scrub report";
            EXPECT_TRUE(it->second.count(BlobRoute{missing_shard_id, missing_blob_id}) == 1)
                << "The missing blob should be reported in shallow scrub report for peer_id=" << peer_id;
        }

        // peers that have the missing shard should be reported in the shallow scrub report.
        const auto missing_shards_in_shallow_report = shallow_scrub_report->get_missing_shard_ids();
        EXPECT_TRUE(missing_shards_in_shallow_report.size() == follower_peer_ids.size())
            << "Missing shard should be reported in shallow scrub report for all followers";
        for (const auto& peer_id : follower_peer_ids) {
            auto it = missing_shards_in_shallow_report.find(peer_id);
            EXPECT_TRUE(it != missing_shards_in_shallow_report.end())
                << "Missing shard should be reported in shallow scrub report for peer_id=" << peer_id;
            EXPECT_TRUE(it->second.size() == 1)
                << "There should be one missing shard for each peer in shallow scrub report";
            EXPECT_TRUE(it->second.count(missing_shard_id) == 1)
                << "The missing shard should be reported in shallow scrub report for peer_id=" << peer_id;
        }
    });

    g_helper->sync();

    // Test case for leader missing/corrupted
    LOGINFO("Starting leader missing/corrupted test case");

    // Get new blob ids for leader corruption test
    auto& leader_shard_blobs = shard_blob_ids_map[missing_shard_id];
    auto leader_it = leader_shard_blobs.begin();
    std::advance(leader_it, 3); // Skip the first 3 blobs already used
    const auto leader_missing_blob_id = leader_it->first;
    const auto leader_corrupted_blob_id = (++leader_it)->first;
    const auto leader_inconsistent_blob_id = (++leader_it)->first;

    // Corrupt data on leader
    run_on_pg_leader(pg_id, [&]() {
        auto& pg_index_table = hs_pg->index_table_;

        // 1. Delete leader_missing_blob_id from pg_index table on leader
        delete_blob_from_index(pg_index_table, missing_shard_id, leader_missing_blob_id);
        LOGINFO("Deleted blob {} from leader index table", leader_missing_blob_id);

        // 2. Make leader_corrupted_blob_id corrupted on leader
        corrupt_blob_data(pg_index_table, missing_shard_id, leader_corrupted_blob_id);
        LOGINFO("Corrupted blob {} on leader", leader_corrupted_blob_id);

        // 3. Make leader_inconsistent_blob_id inconsistent on leader
        make_blob_inconsistent(pg_index_table, missing_shard_id, leader_inconsistent_blob_id, _obj_inst.get());
        LOGINFO("Made blob {} inconsistent on leader", leader_inconsistent_blob_id);
    });

    g_helper->sync();

    // Run scrub and verify both leader and follower corruptions are detected
    run_on_pg_leader(pg_id, [&]() {
        LOGINFO("Running deep scrub to detect both leader and follower corruptions");
        auto scrub_report =
            scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY)
                .get();

        ASSERT_NE(scrub_report, nullptr) << "Deep scrub report should not be null";
        auto deep_scrub_report = std::dynamic_pointer_cast< ScrubManager::DeepScrubReport >(scrub_report);
        ASSERT_NE(deep_scrub_report, nullptr) << "Should be DeepScrubReport";
        deep_scrub_report->print();

        const auto& leader_uuid = _obj_inst->our_uuid();
        const auto& members = (hs_pg->pg_info_).members;
        std::set< peer_id_t > follower_peer_ids;
        for (const auto& member : members) {
            if (member.id != leader_uuid) { follower_peer_ids.insert(member.id); }
        }

        // ========== Verify Missing Blobs ==========
        LOGINFO("Verifying missing blobs detection");
        verify_missing_blobs(deep_scrub_report.get(), leader_uuid, BlobRoute{missing_shard_id, leader_missing_blob_id});
        for (const auto& peer_id : follower_peer_ids) {
            verify_missing_blobs(deep_scrub_report.get(), peer_id, BlobRoute{missing_shard_id, missing_blob_id});
        }

        // ========== Verify Missing Shards ==========
        LOGINFO("Verifying missing shards detection");
        for (const auto& peer_id : follower_peer_ids) {
            verify_missing_shards(deep_scrub_report.get(), peer_id, missing_shard_id);
        }

        // ========== Verify Corrupted Blobs ==========
        LOGINFO("Verifying corrupted blobs detection");
        verify_corrupted_blobs(deep_scrub_report.get(), leader_uuid,
                               BlobRoute{missing_shard_id, leader_corrupted_blob_id});
        for (const auto& peer_id : follower_peer_ids) {
            verify_corrupted_blobs(deep_scrub_report.get(), peer_id, BlobRoute{missing_shard_id, corrupted_blob_id});
        }

        // ========== Verify Inconsistent Blobs ==========
        const auto inconsistent_blobs = deep_scrub_report->get_inconsistent_blobs();
        LOGINFO("Verifying inconsistent blobs detection, inconsistent_blobs.size()={}", inconsistent_blobs.size());

        // Should have 2 inconsistent blobs: one from follower test, one from leader test
        EXPECT_TRUE(inconsistent_blobs.size() == 2)
            << "Should have 2 inconsistent blobs (1 from follower, 1 from leader)";

        // Verify leader's inconsistent blob
        auto leader_inconsistent_it = inconsistent_blobs.find(BlobRoute{missing_shard_id, leader_inconsistent_blob_id});
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
        auto follower_inconsistent_it = inconsistent_blobs.find(BlobRoute{missing_shard_id, inconsistent_blob_id});
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

        LOGINFO("Leader and follower corruption test completed successfully");
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
        scrub_mgr->submit_scrub_task(pg_id, true /* is_deep */, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

        // Check that deep scrub timestamp updated
        auto after_deep_sb = scrub_mgr->get_scrub_superblk(pg_id);
        ASSERT_TRUE(after_deep_sb.has_value());
        EXPECT_GT(after_deep_sb->last_deep_scrub_timestamp, initial_deep_scrub_time)
            << "Deep scrub timestamp should be updated";
        EXPECT_EQ(after_deep_sb->last_shallow_scrub_timestamp, initial_shallow_scrub_time)
            << "Shallow scrub timestamp should not change after deep scrub";

        std::this_thread::sleep_for(std::chrono::seconds(2));

        // Run a shallow scrub
        scrub_mgr->submit_scrub_task(pg_id, false /* is_deep */, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY).get();

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
        auto scrub_future = scrub_mgr->submit_scrub_task(pg_id, true, false, SCRUB_TRIGGER_TYPE::MANUALLY);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        scrub_mgr->cancel_scrub_task(pg_id);
        LOGINFO("Cancelled scrub task for pg={}", pg_id);
        auto scrub_report = std::move(scrub_future).get();

        // The report might be null or have partial results due to cancellation
        // We just verify that cancel doesn't cause crash
        LOGINFO("Scrub task cancelled, report: {}", scrub_report ? "present" : "null");
    });

    // Test canceling when no task is running - should not crash
    run_on_pg_leader(pg_id, [&]() {
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
    std::vector< folly::SemiFuture< std::shared_ptr< ScrubManager::ShallowScrubReport > > > scrub_futures;

    for (const auto& pg_id : pg_ids) {
        run_on_pg_leader(pg_id, [&]() {
            auto future = scrub_mgr->submit_scrub_task(pg_id, true, false, SCRUB_TRIGGER_TYPE::MANUALLY);
            scrub_futures.push_back(std::move(future));
            LOGINFO("Submitted deep scrub for pg={}", pg_id);
        });
    }

    // Wait for all scrub tasks to complete
    for (size_t i = 0; i < scrub_futures.size(); ++i) {
        auto report = std::move(scrub_futures[i]).get();
        if (report) {
            LOGINFO("PG {} scrub completed, report present", pg_ids[i]);
        } else {
            LOGWARN("PG {} scrub returned null report", pg_ids[i]);
        }
    }

    g_helper->sync();
}

// Test deleted blob filter in scrub report
TEST_F(HomeObjectFixture, DeletedBlobFilterTest) {
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
        LOGINFO("Deleted blobs {} and {} from leader index table", missing_blob_to_delete, missing_blob_not_deleted);
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
            scrub_mgr->submit_scrub_task(pg_id, false /* shallow */, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY)
                .get();

        auto missing_blobs = scrub_report->get_missing_blobs();
        for (const auto& peer_id : follower_peer_ids) {
            auto it = missing_blobs.find(peer_id);
            ASSERT_TRUE(it != missing_blobs.end()) << "Missing blob for follower should be reported in scrub report";
            EXPECT_TRUE(it->second.size() == 2) << "There should be two missing blobs for leader in scrub report";
            EXPECT_TRUE(it->second.count(BlobRoute{shard_id, missing_blob_to_delete}) == 1)
                << "The missing blob that will be deleted should be reported in scrub report";
            EXPECT_TRUE(it->second.count(BlobRoute{shard_id, missing_blob_not_deleted}) == 1)
                << "The missing blob that will NOT be deleted should be reported in scrub report";
        }

#ifdef _PRERELEASE
        set_callback_flip(
            "delete_missing_blob_through_raft", std::function< void() >([this, missing_blob_to_delete, shard_id]() {
                auto ret =
                    _obj_inst->blob_manager()->del(shard_id, missing_blob_to_delete, generateRandomTraceId()).get();
                LOGINFO("Blob delete via callback flip completed, ret={}", ret.hasValue());
            }));

        scrub_report =
            scrub_mgr->submit_scrub_task(pg_id, false /* shallow */, false /* force */, SCRUB_TRIGGER_TYPE::MANUALLY)
                .get();

        remove_flip("delete_missing_blob_through_raft");

        // Verify the scrub report
        ASSERT_NE(scrub_report, nullptr) << "Scrub report should not be null";

        missing_blobs = scrub_report->get_missing_blobs();
        for (const auto& peer_id : follower_peer_ids) {
            auto it = missing_blobs.find(peer_id);
            ASSERT_TRUE(it != missing_blobs.end()) << "Missing blob for follower should be reported in scrub report";
            EXPECT_TRUE(it->second.size() == 1) << "There should be one missing blob for leader in scrub report";
            EXPECT_TRUE(it->second.count(BlobRoute{shard_id, missing_blob_not_deleted}) == 1)
                << "The missing blob that was not deleted should be reported in scrub report";
        }
#endif
    });

    g_helper->sync();
    LOGINFO("DeletedBlobFilterTest completed successfully");
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

        auto report = scrub_mgr->submit_scrub_task(pg_id, false, false, SCRUB_TRIGGER_TYPE::MANUALLY).get();
        ASSERT_NE(report, nullptr) << "Scrub report should not be null";

        // Verify timestamp was updated after scrub
        auto sb_after = scrub_mgr->get_scrub_superblk(pg_id);
        ASSERT_TRUE(sb_after.has_value()) << "Scrub superblock should exist after scrub";
        uint64_t timestamp_after = sb_after->last_shallow_scrub_timestamp;
        EXPECT_GT(timestamp_after, timestamp_before) << "Shallow scrub timestamp should be updated after scrub";
        LOGINFO("Timestamp after scrub: {} (updated from {})", timestamp_after, timestamp_before);
    });

    // Now delete the PG - this should cancel any running scrub and remove superblock
    run_on_pg_leader(pg_id, [&]() {
        _obj_inst->pg_manager()->destroy_pg(pg_id);
        LOGINFO("Deleted pg={}", pg_id);
    });

    // Run a scrub to update timestamps
    run_on_pg_leader(pg_id, [&]() {
        auto report = scrub_mgr->submit_scrub_task(pg_id, false, false, SCRUB_TRIGGER_TYPE::MANUALLY).get();
        ASSERT_EQ(report, nullptr) << "Scrub report should be null after PG deletion";
        LOGINFO("Scrub task for deleted pg={} returned null report as expected", pg_id);
    });

    // Wait for PG to be deleted
    std::this_thread::sleep_for(std::chrono::seconds(2));
    g_helper->sync();

    // Verify scrub superblock is cleaned up - get_scrub_superblk should return nullopt
    // Note: This might not be directly testable without internal access, so we just verify no crash
    LOGINFO("PG deleted, scrub manager should have cleaned up");
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

    g_helper->sync();

    const auto hs_pg = _obj_inst->get_hs_pg(pg_id);
    ASSERT_TRUE(hs_pg) << "PG should exist for pg_id=" << pg_id;

    const auto shard_id = shard_info.id;
    auto& shard_blobs = shard_blob_ids_map[shard_id];

    // Select blobs to corrupt
    auto it = shard_blobs.begin();
    const auto corrupted_blob_id = it->first;

    // Corrupt blobs on the local node
    run_on_pg_leader(pg_id, [&]() {
        auto& pg_index_table = hs_pg->index_table_;

        // Make corrupted_blob_id corrupted (corrupt data)
        corrupt_blob_data(pg_index_table, shard_id, corrupted_blob_id);
        LOGINFO("Corrupted blob {} on leader", corrupted_blob_id);
    });

    g_helper->sync();

    run_on_pg_leader(pg_id, [&]() {
        // Create a shard scrub request
        auto shard_req =
            std::make_shared< ScrubManager::shard_scrub_req >(1, 1, 0, _obj_inst->our_uuid(), pg_id, 0, 100, false);

        // Test local_scrub_shard (shallow)
        auto shallow_shard_map = scrub_mgr->local_scrub_shard(shard_req);
        ASSERT_NE(shallow_shard_map, nullptr);
        EXPECT_EQ(shallow_shard_map->get_scrub_type(), SCRUB_TYPE::SHALLOW_SHARD);
        LOGINFO("Shallow shard scrub returned map with {} shards", shallow_shard_map->shards.size());

        // Create a deep shard scrub request
        auto deep_shard_req =
            std::make_shared< ScrubManager::shard_scrub_req >(1, 1, 0, _obj_inst->our_uuid(), pg_id, 0, 100, true);

        // Test local_scrub_shard (deep)
        auto deep_shard_map = scrub_mgr->local_scrub_shard(deep_shard_req);
        ASSERT_NE(deep_shard_map, nullptr);
        EXPECT_EQ(deep_shard_map->get_scrub_type(), SCRUB_TYPE::DEEP_SHARD);
        LOGINFO("Deep shard scrub returned map with {} shards", deep_shard_map->shards.size());

        // Test scrub_pg_meta
        auto pg_meta_req =
            std::make_shared< ScrubManager::base_scrub_req >(1, 1, 0, _obj_inst->our_uuid(), pg_id, true);
        auto pg_meta_map = scrub_mgr->scrub_pg_meta(pg_meta_req);
        ASSERT_NE(pg_meta_map, nullptr);
        EXPECT_EQ(pg_meta_map->get_scrub_type(), SCRUB_TYPE::PG_META);
        LOGINFO("PG meta scrub completed");

        // Test local_scrub_blob (shallow)
        auto shallow_blob_req =
            std::make_shared< ScrubManager::blob_scrub_req >(1, 1, 0, _obj_inst->our_uuid(), pg_id, 0, 100, false);
        auto shallow_blob_map = scrub_mgr->local_scrub_blob(shallow_blob_req);
        // May be null if no blobs exist in range
        if (shallow_blob_map) {
            EXPECT_EQ(shallow_blob_map->get_scrub_type(), SCRUB_TYPE::SHALLOW_BLOB);
            LOGINFO("Shallow blob scrub completed");
        }

        // Test local_scrub_blob (deep) - should detect corrupted and inconsistent blobs
        auto deep_blob_req =
            std::make_shared< ScrubManager::blob_scrub_req >(1, 1, 0, _obj_inst->our_uuid(), pg_id, 0, 100, true);
        auto deep_blob_map = scrub_mgr->local_scrub_blob(deep_blob_req);
        ASSERT_NE(deep_blob_map, nullptr);
        EXPECT_EQ(deep_blob_map->get_scrub_type(), SCRUB_TYPE::DEEP_BLOB);
        auto deep_blob_map_cast = std::dynamic_pointer_cast< ScrubManager::DeepBlobScrubMap >(deep_blob_map);
        LOGINFO("Deep blob scrub completed, found {} blobs", deep_blob_map_cast->blobs.size());

        // Check for corrupted blob
        auto corrupted_it = deep_blob_map_cast->blobs.find(BlobRoute{shard_id, corrupted_blob_id});
        EXPECT_TRUE(corrupted_it != deep_blob_map_cast->blobs.end()) << "Corrupted blob should be in deep scrub result";
        if (corrupted_it != deep_blob_map_cast->blobs.end()) {
            auto result = std::get_if< ScrubResult >(&corrupted_it->second);
            ASSERT_TRUE(result != nullptr) << "Corrupted blob result should be ScrubResult";
            EXPECT_EQ(*result, ScrubResult::MISMATCH) << "Corrupted blob should have MISMATCH result";
            LOGINFO("Deep scrub correctly detected corrupted blob {}", corrupted_blob_id);
        }
    });

    g_helper->sync();
}

// Test scrub request serialization and deserialization
TEST_F(HomeObjectFixture, ScrubRequestSerializationTest) {
    const pg_id_t pg_id = 1;
    create_pg(pg_id);
    auto scrub_mgr = _obj_inst->scrub_manager();

    const uint64_t shard_size = 64 * Mi;
    create_shard(pg_id, shard_size, "shard meta");
    run_on_pg_leader(pg_id, [&]() {
        auto my_uuid = _obj_inst->our_uuid();

        // Test base_scrub_req serialization
        {
            auto req = std::make_shared< ScrubManager::base_scrub_req >(1, 1, 100, my_uuid, pg_id, true);

            // Serialize
            auto buffer = req->build_flat_buffer();
            EXPECT_GT(buffer.size(), 0) << "Serialized buffer should not be empty";

            // Deserialize
            auto req_loaded = std::make_shared< ScrubManager::base_scrub_req >();
            bool load_success = req_loaded->load(buffer.data(), buffer.size());
            EXPECT_TRUE(load_success) << "Deserialization should succeed";

            // Verify fields
            EXPECT_EQ(req_loaded->pg_id, pg_id);
            EXPECT_EQ(req_loaded->task_id, 1);
            EXPECT_EQ(req_loaded->req_id, 1);
            EXPECT_EQ(req_loaded->scrub_lsn, 100);

            LOGINFO("base_scrub_req serialization test passed");
        }

        // Test blob_scrub_req serialization
        {
            auto req = std::make_shared< ScrubManager::blob_scrub_req >(1, 2, 200, my_uuid, pg_id, 100, 200, true);

            // Serialize
            auto buffer = req->build_flat_buffer();
            EXPECT_GT(buffer.size(), 0);

            // Deserialize
            auto req_loaded = std::make_shared< ScrubManager::blob_scrub_req >();
            bool load_success = req_loaded->load(buffer.data(), buffer.size());
            EXPECT_TRUE(load_success);

            // Verify fields
            EXPECT_EQ(req_loaded->pg_id, pg_id);
            EXPECT_EQ(req_loaded->task_id, 1);
            EXPECT_EQ(req_loaded->req_id, 2);
            EXPECT_EQ(req_loaded->scrub_lsn, 200);
            EXPECT_EQ(req_loaded->start, 100);
            EXPECT_EQ(req_loaded->end, 200);
            EXPECT_TRUE(req_loaded->is_deep_scrub());
            EXPECT_EQ(req_loaded->get_scrub_type(), SCRUB_TYPE::DEEP_BLOB);

            LOGINFO("blob_scrub_req serialization test passed");
        }

        // Test shard_scrub_req serialization
        {
            auto req = std::make_shared< ScrubManager::shard_scrub_req >(1, 3, 300, my_uuid, pg_id, 0, 100, false);

            // Serialize
            auto buffer = req->build_flat_buffer();
            EXPECT_GT(buffer.size(), 0);

            // Deserialize
            auto req_loaded = std::make_shared< ScrubManager::shard_scrub_req >();
            bool load_success = req_loaded->load(buffer.data(), buffer.size());
            EXPECT_TRUE(load_success);

            // Verify fields
            EXPECT_EQ(req_loaded->pg_id, pg_id);
            EXPECT_EQ(req_loaded->task_id, 1);
            EXPECT_EQ(req_loaded->req_id, 3);
            EXPECT_EQ(req_loaded->scrub_lsn, 300);
            EXPECT_EQ(req_loaded->start, 0);
            EXPECT_EQ(req_loaded->end, 100);
            EXPECT_FALSE(req_loaded->is_deep_scrub());
            EXPECT_EQ(req_loaded->get_scrub_type(), SCRUB_TYPE::SHALLOW_SHARD);

            LOGINFO("shard_scrub_req serialization test passed");
        }
    });

    g_helper->sync();
}