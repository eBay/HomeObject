#include <chrono>
#include <cstdint>
#include <limits>
#include <mutex>
#include <utility>
#include <vector>

#include "homeobj_fixture.hpp"
#include "lib/homestore_backend/index_kv.hpp"

namespace {
using Clock = std::chrono::steady_clock;

int64_t elapsed_us(Clock::time_point begin, Clock::time_point end) {
    return std::chrono::duration_cast< std::chrono::microseconds >(end - begin).count();
}
} // namespace

// --num_pgs is the PG count; --num_blobs is the number of index entries per PG.
TEST_F(HomeObjectFixture, DISABLED_MultiPgTombstoneScanBenchmark) {
    ASSERT_EQ(SISL_OPTIONS["replicas"].as< uint8_t >(), 1);
    ASSERT_FALSE(HS_BACKEND_DYNAMIC_CONFIG(enable_gc))
        << "Disable GC with --override_config hs_backend_config.enable_gc=false; index PBAs are synthetic";
    const auto pg_count = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    const auto entries_per_pg = SISL_OPTIONS["num_blobs"].as< uint64_t >();
    ASSERT_GT(pg_count, 0);
    ASSERT_LT(pg_count, std::numeric_limits< pg_id_t >::max());
    ASSERT_GT(entries_per_pg, 0);
    ASSERT_LT(entries_per_pg, std::numeric_limits< uint32_t >::max());

    constexpr uint64_t shard_count{16};
    constexpr uint32_t rounds{2};
    const uint64_t expected_tombstones = (entries_per_pg + 3) / 4;
    const homestore::MultiBlkId synthetic_pbas{0, 1, 1};
    ASSERT_NE(synthetic_pbas, HSHomeObject::tombstone_pbas);
    uint64_t total_node_bytes{0};
    const uint64_t node_size = homestore::hs()->index_service().node_size();

    LOGINFO("multi_pg_gc_scan config pgs={} entries_per_pg={} rounds={} cache=uncontrolled",
            pg_count, entries_per_pg, rounds);
    for (uint64_t pg_number = 1; pg_number <= pg_count; ++pg_number) {
        const auto pg_id = static_cast< pg_id_t >(pg_number);
        ASSERT_NO_FATAL_FAILURE(create_pg(pg_id));
        auto pg = _obj_inst->get_hs_pg(pg_id);
        ASSERT_NE(pg, nullptr);
        ASSERT_NE(pg->index_table_, nullptr);
        auto index = pg->index_table_;

        const auto load_begin = Clock::now();
        for (uint64_t i = 0; i < entries_per_pg; ++i) {
            const auto shard = make_new_shard_id(pg_id, i % shard_count + 1);
            BlobRouteKey key{BlobRoute{shard, i}};
            BlobRouteValue value{i % 4 == 0 ? HSHomeObject::tombstone_pbas : synthetic_pbas};
            BlobRouteValue existing_value;
            homestore::BtreeSinglePutRequest req{&key, &value, homestore::btree_put_type::INSERT, &existing_value};
            ASSERT_EQ(index->put(req), homestore::btree_status_t::success) << "pg=" << pg_number << " entry=" << i;
            if ((i + 1) % 1000000 == 0) {
                LOGINFO("multi_pg_gc_scan load_progress pg={} entries={}", pg_number, i + 1);
            }
        }
        const auto load_end = Clock::now();

        uint64_t visited{0};
        uint64_t tombstones{0};
        auto start_key = BlobRouteKey{BlobRoute{uint64_t(pg_id) << shard_width, 0}};
        auto end_key = BlobRouteKey{BlobRoute{uint64_t(pg_id + 1) << shard_width, 0}};
        homestore::BtreeQueryRequest< BlobRouteKey > query{
            homestore::BtreeKeyRange< BlobRouteKey >{std::move(start_key), true, std::move(end_key), false},
            homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY,
            std::numeric_limits< uint32_t >::max(),
            [&visited, &tombstones](homestore::BtreeKey const&, homestore::BtreeValue const& raw) -> bool {
                ++visited;
                if (BlobRouteValue{raw}.pbas() == HSHomeObject::tombstone_pbas) { ++tombstones; }
                return false;
            }};
        std::vector< std::pair< BlobRouteKey, BlobRouteValue > > matches;
        const auto status = index->query(query, matches);
        ASSERT_TRUE(status == homestore::btree_status_t::success || status == homestore::btree_status_t::has_more);
        ASSERT_TRUE(matches.empty());
        ASSERT_EQ(visited, entries_per_pg);
        ASSERT_EQ(tombstones, expected_tombstones);
        const auto [interior_nodes, leaf_nodes] = index->get_num_nodes();
        const auto node_bytes = (interior_nodes + leaf_nodes) * node_size;
        total_node_bytes += node_bytes;
        LOGINFO("multi_pg_gc_scan loaded pg={} entries={} tombstones={} load_us={} interior_nodes={} leaf_nodes={} "
                "node_size_bytes={} node_bytes={} total_node_bytes={}",
                pg_number, visited, tombstones, elapsed_us(load_begin, load_end), interior_nodes, leaf_nodes, node_size,
                node_bytes, total_node_bytes);

        if (pg_number == 1) {
            std::unique_lock lock(_obj_inst->_pg_lock);
            const auto start = Clock::now();
            const auto count = _obj_inst->get_pg_tombstone_blob_count(pg_id);
            const auto end = Clock::now();
            lock.unlock();
            ASSERT_EQ(count, expected_tombstones);
            LOGINFO("multi_pg_gc_scan baseline pg={} entries={} scan_us={} tombstones={}",
                    pg_number, entries_per_pg, elapsed_us(start, end), count);
        }
    }

    LOGINFO("multi_pg_gc_scan dataset pgs={} entries_per_pg={} total_node_bytes={} cache=uncontrolled",
            pg_count, entries_per_pg, total_node_bytes);
    for (uint32_t round = 0; round < rounds; ++round) {
        LOGINFO("multi_pg_gc_scan metrics_before round={} metrics={}", round,
                sisl::MetricsFarm::getInstance().get_result_in_json().dump());
        int64_t max_scan_us{-1};
        uint64_t max_pg{0};
        for (uint64_t pg_number = 1; pg_number <= pg_count; ++pg_number) {
            const auto pg_id = static_cast< pg_id_t >(pg_number);
            const auto lock_begin = Clock::now();
            std::unique_lock lock(_obj_inst->_pg_lock);
            const auto acquired = Clock::now();
            const auto scan_begin = Clock::now();
            const auto count = _obj_inst->get_pg_tombstone_blob_count(pg_id);
            const auto scan_end = Clock::now();
            lock.unlock();
            const auto released = Clock::now();
            ASSERT_EQ(count, expected_tombstones) << "pg=" << pg_number << " round=" << round;
            const auto scan_us = elapsed_us(scan_begin, scan_end);
            if (scan_us > max_scan_us) {
                max_scan_us = scan_us;
                max_pg = pg_number;
            }
            LOGINFO("multi_pg_gc_scan sample round={} pg={} entries={} tombstones={} lock_wait_us={} scan_us={} "
                    "test_lock_hold_us={}",
                    round, pg_number, entries_per_pg, count, elapsed_us(lock_begin, acquired), scan_us,
                    elapsed_us(acquired, released));
        }
        LOGINFO("multi_pg_gc_scan round_summary round={} pgs={} max_scan_us={} max_pg={}",
                round, pg_count, max_scan_us, max_pg);
        LOGINFO("multi_pg_gc_scan metrics_after round={} metrics={}", round,
                sisl::MetricsFarm::getInstance().get_result_in_json().dump());
    }
}
