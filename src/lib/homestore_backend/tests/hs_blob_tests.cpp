#include "homeobj_fixture.hpp"
#include "homeobj_fixture_http.hpp"

#include "lib/homestore_backend/index_kv.hpp"
#include <homestore/replication_service.hpp>
#include <limits>

TEST_F(HomeObjectFixture, HttpGetBlobChecksumBypass) {
    if (!SISL_OPTIONS["enable_http"].as< bool >()) { GTEST_SKIP() << "Requires --enable_http=true"; }

    create_pg(1);
    auto shard = create_shard(1, 64 * Mi, "shard meta");
    // Include embedded NUL and non-UTF-8 bytes to verify that HTTP returns the payload without text conversion.
    const std::string payload{"blob\0bytes\xff", 11};
    sisl::io_blob_safe body(payload.size());
    std::memcpy(body.bytes(), payload.data(), payload.size());
    put_blob(shard.id, Blob(std::move(body), "test-key", 123));
    const auto blob_id = g_helper->get_uint64_id();

    auto missing_pg_shard = shard;
    missing_pg_shard.placement_group = std::numeric_limits< pg_id_t >::max();
    auto missing_pg = _obj_inst->_get_blob(missing_pg_shard, blob_id, 0, 0, false, 0, true).get();
    ASSERT_FALSE(missing_pg);
    EXPECT_EQ(missing_pg.error().getCode(), BlobErrorCode::UNKNOWN_PG);

    HttpHelper http("127.0.0.1", 5000 + g_helper->replica_num());
    const auto resource = fmt::format("/api/v1/blob?shard_id={:#x}&blob_id={}", shard.id, blob_id);
    auto response = http.get(resource);
    ASSERT_EQ(response.code(), Pistache::Http::Code::Ok);
    EXPECT_EQ(response.body(), payload);
    response = http.get(resource + "&debug_skip_verify=false");
    ASSERT_EQ(response.code(), Pistache::Http::Code::Ok);
    EXPECT_EQ(response.body(), payload);
    response = http.get(resource + "&debug_skip_verify=true");
    ASSERT_EQ(response.code(), Pistache::Http::Code::Ok);
    EXPECT_EQ(response.body(), payload);
    response = http.get(fmt::format("/api/v1/blob?shard_id={}&blob_id={:#x}", shard.id, blob_id));
    ASSERT_EQ(response.code(), Pistache::Http::Code::Ok);
    EXPECT_EQ(response.body(), payload);
    EXPECT_EQ(http.get(fmt::format("/api/v1/blob?shard_id={}&blob_id=18446744073709551615", shard.id)).code(),
              Pistache::Http::Code::Not_Found);

#ifdef _PRERELEASE
    if (HS_BACKEND_DYNAMIC_CONFIG(check_traffic_ready_before_get)) {
        // The normal read must honor readiness; a debug read may inspect the local replica without quorum.
        set_basic_flip("get_blob_not_ready_for_traffic", 2);
        EXPECT_EQ(http.get(resource).code(), Pistache::Http::Code::Service_Unavailable);
        response = http.get(resource + "&debug_skip_verify=true");
        EXPECT_EQ(response.code(), Pistache::Http::Code::Ok);
        EXPECT_EQ(response.body(), payload);
        remove_flip("get_blob_not_ready_for_traffic");
    }
#endif

    auto hs_pg = _obj_inst->get_hs_pg(1);
    auto pbas = _obj_inst->get_blob_from_index_table(hs_pg->index_table_, shard.id, blob_id);
    ASSERT_TRUE(pbas);
    const auto size = pbas.value().blk_count() * hs_pg->repl_dev_->get_blk_size();
    sisl::io_blob_safe read_buf(size, 4096);
    sisl::sg_list sgs;
    sgs.size = size;
    sgs.iovs.emplace_back(iovec{.iov_base = read_buf.bytes(), .iov_len = size});
    ASSERT_FALSE(homestore::data_service().async_read(pbas.value(), sgs, size).get());
    auto* header = reinterpret_cast< HSHomeObject::BlobHeader* >(read_buf.bytes());
    ASSERT_TRUE(header->valid());
    ASSERT_LT(header->data_offset, size);
    const auto original_header = *header;
    const auto data_offset = header->data_offset;
    // Corrupt only the payload, leaving the header and its checksum intact.
    read_buf.bytes()[header->data_offset] ^= 0xff;
    ASSERT_FALSE(homestore::data_service().async_write(sgs, pbas.value()).get());

    auto verified = _obj_inst->blob_manager()->get(shard.id, blob_id).get();
    ASSERT_FALSE(verified);
    EXPECT_EQ(verified.error().getCode(), BlobErrorCode::CHECKSUM_MISMATCH);
    EXPECT_NE(verified.error().message.find("Hash mismatch"), std::string::npos);
    auto allow_partial_skip = _obj_inst->blob_manager()->get(shard.id, blob_id, 0, 0, true).get();
    // allow_skip_verify permits an optimized partial read; it must not skip verification for a full read.
    ASSERT_FALSE(allow_partial_skip);
    EXPECT_EQ(allow_partial_skip.error().getCode(), BlobErrorCode::CHECKSUM_MISMATCH);

    EXPECT_EQ(http.get(resource).code(), Pistache::Http::Code::Internal_Server_Error);
    response = http.get(resource + "&debug_skip_verify=false");
    EXPECT_EQ(response.code(), Pistache::Http::Code::Internal_Server_Error);
    EXPECT_NE(response.body().find("CHECKSUM_MISMATCH"), std::string::npos);
    response = http.get(resource + "&debug_skip_verify=true");
    ASSERT_EQ(response.code(), Pistache::Http::Code::Ok);
    auto corrupted_payload = payload;
    corrupted_payload[0] = static_cast< char >(static_cast< unsigned char >(corrupted_payload[0]) ^ 0xff);
    EXPECT_EQ(response.body(), corrupted_payload);

    auto expect_debug_body = [&](const std::string& expected) {
        ASSERT_FALSE(homestore::data_service().async_write(sgs, pbas.value()).get());
        EXPECT_EQ(http.get(resource).code(), Pistache::Http::Code::Internal_Server_Error);
        auto debug_response = http.get(resource + "&debug_skip_verify=true");
        ASSERT_EQ(debug_response.code(), Pistache::Http::Code::Ok);
        EXPECT_EQ(debug_response.body(), expected);
    };
    auto raw_allocation = [&] {
        return std::string(reinterpret_cast< const char* >(read_buf.cbytes()), read_buf.size());
    };

    // Exercise the post-verification range check directly: HTTP requests always ask for the entire blob.
    auto invalid_range = _obj_inst->_get_blob(shard, blob_id, payload.size() + 1, 0, false, 0, false).get();
    ASSERT_FALSE(invalid_range);
    EXPECT_EQ(invalid_range.error().getCode(), BlobErrorCode::CHECKSUM_MISMATCH);
    auto debug_range = _obj_inst->_get_blob(shard, blob_id, payload.size() + 1, 0, false, 0, true).get();
    ASSERT_TRUE(debug_range);
    EXPECT_EQ(std::string(reinterpret_cast< const char* >(debug_range.value().body.cbytes()),
                          debug_range.value().body.size()),
              raw_allocation());

    read_buf.bytes()[data_offset] ^= 0xff;
    ASSERT_FALSE(homestore::data_service().async_write(sgs, pbas.value()).get());
    invalid_range = _obj_inst->_get_blob(shard, blob_id, payload.size() + 1, 0, false, 0, false).get();
    ASSERT_FALSE(invalid_range);
    EXPECT_EQ(invalid_range.error().getCode(), BlobErrorCode::INVALID_ARG);
    debug_range = _obj_inst->_get_blob(shard, blob_id, payload.size() - 1, 2, false, 0, true).get();
    ASSERT_TRUE(debug_range);
    EXPECT_EQ(std::string(reinterpret_cast< const char* >(debug_range.value().body.cbytes()),
                          debug_range.value().body.size()),
              raw_allocation());
    read_buf.bytes()[data_offset] ^= 0xff;

    // Invalid payload bounds must return the entire allocation, not attempt an out-of-bounds copy.
    header->data_offset = size;
    header->seal();
    expect_debug_body(raw_allocation());

    *header = original_header;
    header->blob_size = 0;
    header->seal();
    expect_debug_body(raw_allocation());

    *header = original_header;
    header->blob_size = std::numeric_limits< uint32_t >::max();
    header->seal();
    expect_debug_body(raw_allocation());

    *header = original_header;
    header->data_offset = sizeof(HSHomeObject::BlobHeader) - 1;
    header->seal();
    expect_debug_body(raw_allocation());

    // With safe payload bounds, even a bad header checksum or identity should not suppress the debug payload.
    *header = original_header;
    header->header_hash[0] ^= 1;
    expect_debug_body(corrupted_payload);

    *header = original_header;
    header->blob_id ^= 1;
    header->seal();
    expect_debug_body(corrupted_payload);

    *header = original_header;
    header->shard_id ^= 1;
    header->seal();
    expect_debug_body(corrupted_payload);

    *header = original_header;
    header->user_key_size = HSHomeObject::BlobHeader::max_user_key_length + 1;
    header->seal();
    expect_debug_body(corrupted_payload);

    *header = original_header;
    header->hash_algorithm = static_cast< HSHomeObject::BlobHeader::HashAlgorithm >(0xff);
    expect_debug_body(corrupted_payload);

    // Exercise several simultaneous failures to ensure debug validation continues after the first one.
    *header = original_header;
    header->shard_id ^= 1;
    header->blob_id ^= 1;
    header->header_hash[0] ^= 1;
    expect_debug_body(corrupted_payload);

    // A truncated allocation cannot safely be interpreted as a BlobHeader.
    auto short_header = _obj_inst->do_verify_blob(read_buf.cbytes(), shard.id, blob_id, false, sizeof(*header) - 1);
    ASSERT_FALSE(short_header);
    auto debug_short_header =
        _obj_inst->do_verify_blob(read_buf.cbytes(), shard.id, blob_id, true, sizeof(*header) - 1);
    ASSERT_TRUE(debug_short_header);
    EXPECT_FALSE(debug_short_header->payload_bounds_valid);

    // Restore the original bytes before deleting so the fixture can observe the blob during deletion.
    *header = original_header;
    read_buf.bytes()[data_offset] ^= 0xff;
    ASSERT_FALSE(homestore::data_service().async_write(sgs, pbas.value()).get());
    del_blob(1, shard.id, blob_id);
    EXPECT_EQ(http.get(resource + "&debug_skip_verify=true").code(), Pistache::Http::Code::Not_Found);
}

TEST_F(HomeObjectFixture, HttpGetBlobInvalidRequest) {
    if (!SISL_OPTIONS["enable_http"].as< bool >()) { GTEST_SKIP() << "Requires --enable_http=true"; }

    HttpHelper http("127.0.0.1", 5000 + g_helper->replica_num());
    for (const auto& query :
         {"", "?shard_id=1", "?blob_id=0", "?shard_id=&blob_id=0", "?shard_id=-1&blob_id=0", "?shard_id=1&blob_id=-1",
          "?shard_id=1junk&blob_id=0", "?shard_id=0x&blob_id=0", "?shard_id=1&blob_id=18446744073709551616",
          "?shard_id=1&blob_id=0&debug_skip_verify=1", "?shard_id=1&blob_id=0&debug_skip_verify="}) {
        EXPECT_EQ(http.get(std::string("/api/v1/blob") + query).code(), Pistache::Http::Code::Bad_Request) << query;
    }
    EXPECT_EQ(http.get("/api/v1/blob?shard_id=0&blob_id=0").code(), Pistache::Http::Code::Not_Found);
    EXPECT_EQ(http.get("/api/v1/blob?shard_id=0&blob_id=0&debug_skip_verify=true").code(),
              Pistache::Http::Code::Not_Found);
}

TEST_F(HomeObjectFixture, BasicEquivalence) {
    auto shard_mgr = _obj_inst->shard_manager();
    auto pg_mgr = _obj_inst->pg_manager();
    auto blob_mgr = _obj_inst->blob_manager();
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(shard_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(pg_mgr.get()));
    EXPECT_EQ(_obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(blob_mgr.get()));
}
TEST_F(HomeObjectFixture, BasicPutGetDelBlobWithRestart) {
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
            auto shard = create_shard(i, 64 * Mi, "shard meta");
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
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

    //  Put blob after restart to test the persistence of blob sequence number
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
                    auto tid = generateRandomTraceId();
                    auto g = _obj_inst->blob_manager()->del(shard_id, blob_id, tid).get();
                    ASSERT_TRUE(g);
                    LOGINFO("delete blob shard {} blob {}, trace_id={}", shard_id, blob_id, tid);
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
        auto hs_pg = _obj_inst->get_hs_pg(pg_id);
        ASSERT_TRUE(hs_pg != nullptr);
        auto index_table = hs_pg->index_table_;
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

TEST_F(HomeObjectFixture, BasicPutGetDelBlobOnExistPGWithDiskLost) {
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
            auto shard = create_shard(i, 64 * Mi, "shard meta");
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    /*
     * restart with one disk lost, exist pg should support put, get, delete.
     */
    restart(0, 0, 1);
    // current num_pgs = 2, pg{2} is diskdown
    LOGI("restart with one disk lost");
    std::set< pg_id_t > lost_pg{2};
    std::map< pg_id_t, std::vector< shard_id_t > > exist_pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > exist_pg_blob_id;

    for (const auto& pair : pg_shard_id_vec) {
        if (!lost_pg.contains(pair.first)) {
            exist_pg_shard_id_vec.emplace(pair);
            exist_pg_blob_id[pair.first] = pg_blob_id[pair.first];
        }
    }
    const uint64_t exist_num_pgs = num_pgs - 1;
    // Verify all get blobs after restart
    verify_get_blob(exist_pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats after restart
    verify_obj_count(exist_num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    //  Put blob after restart to test the persistence of blob sequence number
    put_blobs(exist_pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs with random offset and length.
    verify_get_blob(exist_pg_shard_id_vec, num_blobs_per_shard, true /* use_random_offset */);

    // Verify the stats after put blobs after restart
    verify_obj_count(exist_num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, false /* deleted */);

    // Delete all blobs
    del_all_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify the stats after restart
    verify_obj_count(exist_num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);
}

TEST_F(HomeObjectFixture, BasicPutGetDelBlobWithDiskBack) {
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
            auto shard = create_shard(i, 64 * Mi, "shard meta");
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    // Restart with disk lost
    restart(0, 0, 1);
    // Restart with disk back
    restart();

    // Verify all get blobs after restart
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    //  Put blob after restart to test the persistence of blob sequence number
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
                    auto tid = generateRandomTraceId();
                    auto g = _obj_inst->blob_manager()->del(shard_id, blob_id, tid).get();
                    ASSERT_TRUE(g);
                    LOGINFO("delete blob shard {} blob {}, trace_id={}", shard_id, blob_id, tid);
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
        auto hs_pg = _obj_inst->get_hs_pg(pg_id);
        ASSERT_TRUE(hs_pg != nullptr);
        auto index_table = hs_pg->index_table_;
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

    // Restart with disk lost
    restart(0, 0, 1);
    // Restart with disk back
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

TEST_F(HomeObjectFixture, BasicPutGetDelOnAllPGWithDiskLost) {
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
            auto shard = create_shard(i, 64 * Mi, "shard meta");
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    // degrade_pg_id = 2, leader's replica_num=0, chose the replica_num=2 membet lost one disk.
    pg_id_t degrade_pg_id = 2;
    const uint8_t lost_disk_replica_num = 2;

    g_helper->sync();
    if (g_helper->replica_num() == lost_disk_replica_num) {
        // reset with one member lost one disk
        restart(0, 0, 1);

        // send read request to disk lost member
        blob_id_t current_blob_id{0};
        uint32_t off = 0, len = 0;
        for (const auto& shard_id : pg_shard_id_vec[degrade_pg_id]) {
            for (uint64_t k = 0; k < num_blobs_per_shard; k++) {
                auto tid = generateRandomTraceId();
                LOGI("going to verify blob pg={} shard {} blob {} trace_id={}", degrade_pg_id, shard_id,
                     current_blob_id, tid);
                auto blob = build_blob(current_blob_id);
                len = blob.body.size();
                auto g = _obj_inst->blob_manager()->get(shard_id, current_blob_id, off, len, false, tid).get();
                ASSERT_TRUE(g.hasError())
                    << "degraded pg on error member should return get blob fail, shard_id " << shard_id << " blob_id "
                    << current_blob_id << " replica number " << g_helper->replica_num();
                current_blob_id++;
            }
        }

        // send put/del blob request to disk lost member
        auto put_shard_id = pg_shard_id_vec[degrade_pg_id].back();
        auto put_blob = build_blob(current_blob_id);
        auto tid = generateRandomTraceId();

        LOGDEBUG("Put blob pg={} shard {} blob {} size {} data {} trace_id={} on disk lost member", degrade_pg_id,
                 put_shard_id, current_blob_id, put_blob.body.size(),
                 hex_bytes(put_blob.body.cbytes(), std::min(10u, put_blob.body.size())), tid);

        auto b = _obj_inst->blob_manager()->put(put_shard_id, std::move(put_blob), tid).get();
        ASSERT_TRUE(b.hasError()) << "degraded pg on error member should return put blob fail, shard_id "
                                  << put_shard_id << " blob_id " << current_blob_id << " replica number "
                                  << g_helper->replica_num();

        blob_id_t delete_blob_id{0};
        auto delete_shard_id = pg_shard_id_vec[degrade_pg_id].back();
        auto delete_blob = build_blob(delete_blob_id);
        tid = generateRandomTraceId();
        LOGDEBUG("Delete blob pg={} shard {} blob {} size {} data {} trace_id={} on disk lost member", degrade_pg_id,
                 delete_shard_id, delete_blob_id, delete_blob.body.size(),
                 hex_bytes(delete_blob.body.cbytes(), std::min(10u, delete_blob.body.size())), tid);

        auto d = _obj_inst->blob_manager()->del(delete_shard_id, delete_blob_id, tid).get();
        ASSERT_TRUE(d.hasError()) << "degraded pg on error member should return delete blob fail, shard_id "
                                  << delete_shard_id << " blob_id " << delete_blob_id << " replica number "
                                  << g_helper->replica_num();
    } else {
        restart();
        sleep(10);
    }
    g_helper->sync();
}

TEST_F(HomeObjectFixture, DeleteNonExistBlob) {
    g_helper->sync();
    // create a pg with one shard
    const auto pg_id = 1;
    create_pg(pg_id);
    const auto shard_id = create_shard(1, 64 * Mi, "shard meta").id;
    verify_obj_count(1, 1, 0, false /* deleted */);

    // do not put any blob to exercise deleting a non-exist blob, everything should goes well
    del_blob(1, shard_id, 1);
}

#ifdef _PRERELEASE
TEST_F(HomeObjectFixture, BasicPutGetBlobWithPushDataDisabled) {
    // disable leader push data. As a result, followers have to fetch data to exercise the fetch_data implementation of
    // statemachine
    set_basic_flip("disable_leader_push_data", std::numeric_limits< int >::max());

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
            auto shard = create_shard(i, 64 * Mi, "shard meta");
            pg_shard_id_vec[i].emplace_back(shard.id);
            LOGINFO("pg={} shard {}", i, shard.id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blobs(pg_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    // Verify all get blobs
    verify_get_blob(pg_shard_id_vec, num_blobs_per_shard);

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    remove_flip("disable_leader_push_data");
}

// TODO:: add a test for no_space_left without flip.

#endif

// TODO:: add more test cases to verify the push data disabled scenario after we have gc
