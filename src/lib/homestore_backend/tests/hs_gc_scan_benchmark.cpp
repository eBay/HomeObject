#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <mutex>
#include <thread>
#include <vector>

#include "homeobj_fixture.hpp"
#include "lib/homestore_backend/index_kv.hpp"

namespace {
using Clock = std::chrono::steady_clock;

int64_t elapsed_us(Clock::time_point begin, Clock::time_point end) {
    return std::chrono::duration_cast< std::chrono::microseconds >(end - begin).count();
}
} // namespace

// Opt in with --gtest_also_run_disabled_tests; --num_blobs sets the maximum number of index entries.
TEST_F(HomeObjectFixture, DISABLED_PgTombstoneScanBenchmark) {
    ASSERT_EQ(SISL_OPTIONS["replicas"].as< uint8_t >(), 1);
    const auto max_entries = SISL_OPTIONS["num_blobs"].as< uint64_t >();
    ASSERT_GT(max_entries, 0);
    ASSERT_LT(max_entries, std::numeric_limits< uint32_t >::max());

    constexpr pg_id_t scan_pg{1};
    constexpr pg_id_t other_pg{2};
    constexpr uint64_t shard_count{16};
    constexpr uint32_t repetitions{5};
    create_pg(scan_pg);
    create_pg(other_pg);
    auto pg = _obj_inst->get_hs_pg(scan_pg);
    ASSERT_NE(pg, nullptr);
    ASSERT_NE(pg->index_table_, nullptr);
    ASSERT_NE(_obj_inst->get_hs_pg(other_pg), nullptr);
    auto index = pg->index_table_;

    std::vector< uint64_t > sizes;
    for (const uint64_t n : {10000ULL, 100000ULL, 1000000ULL, 10000000ULL}) {
        if (n < max_entries) { sizes.push_back(n); }
    }
    sizes.push_back(max_entries);

    uint64_t inserted{0};
    uint64_t expected_tombstones{0};
    const homestore::MultiBlkId synthetic_pbas{0, 1, 1};
    ASSERT_NE(synthetic_pbas, HSHomeObject::tombstone_pbas);
    for (const auto target : sizes) {
        const auto load_begin = Clock::now();
        for (; inserted < target; ++inserted) {
            const auto shard = make_new_shard_id(scan_pg, inserted % shard_count + 1);
            BlobRouteKey key{BlobRoute{shard, inserted}};
            const bool is_tombstone = inserted % 4 == 0;
            BlobRouteValue value{is_tombstone ? HSHomeObject::tombstone_pbas : synthetic_pbas};
            BlobRouteValue existing_value;
            homestore::BtreeSinglePutRequest req{&key, &value, homestore::btree_put_type::INSERT, &existing_value};
            const auto status = index->put(req);
            ASSERT_EQ(status, homestore::btree_status_t::success) << "entry=" << inserted;
            if (is_tombstone) { ++expected_tombstones; }
        }
        const auto load_end = Clock::now();

        uint64_t visited{0};
        uint64_t observed_tombstones{0};
        auto start_key = BlobRouteKey{BlobRoute{uint64_t(scan_pg) << shard_width, 0}};
        auto end_key = BlobRouteKey{BlobRoute{uint64_t(scan_pg + 1) << shard_width, 0}};
        homestore::BtreeQueryRequest< BlobRouteKey > query{
            homestore::BtreeKeyRange< BlobRouteKey >{std::move(start_key), true, std::move(end_key), false},
            homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY,
            std::numeric_limits< uint32_t >::max(),
            [&visited, &observed_tombstones](homestore::BtreeKey const&, homestore::BtreeValue const& raw) -> bool {
                ++visited;
                if (BlobRouteValue{raw}.pbas() == HSHomeObject::tombstone_pbas) { ++observed_tombstones; }
                return false;
            }};
        std::vector< std::pair< BlobRouteKey, BlobRouteValue > > matches;
        const auto status = index->query(query, matches);
        ASSERT_TRUE(status == homestore::btree_status_t::success || status == homestore::btree_status_t::has_more);
        ASSERT_TRUE(matches.empty());
        ASSERT_EQ(visited, target);
        ASSERT_EQ(observed_tombstones, expected_tombstones);
        LOGINFO("gc_scan_benchmark load entries={} tombstones={} batch_load_us={} shards={} cache=uncontrolled",
                target, expected_tombstones, elapsed_us(load_begin, load_end), shard_count);

        for (uint32_t iteration = 0; iteration < repetitions; ++iteration) {
            constexpr uint32_t baseline_samples{128};
            std::promise< void > reader_ready;
            auto ready = reader_ready.get_future();
            std::atomic< bool > stop_reader{false};
            uint32_t failed_lookups{0};
            uint64_t baseline_max_us{0};
            uint64_t probe_count{0};
            int64_t probe_max_us{-1};
            Clock::time_point probe_max_begin;
            Clock::time_point probe_max_end;
            std::thread reader([&] {
                for (uint32_t i = 0; i < baseline_samples; ++i) {
                    const auto begin = Clock::now();
                    const auto result = _obj_inst->get_hs_pg(other_pg);
                    const auto end = Clock::now();
                    if (result == nullptr) { ++failed_lookups; }
                    const auto duration = static_cast< uint64_t >(elapsed_us(begin, end));
                    if (duration > baseline_max_us) { baseline_max_us = duration; }
                }
                reader_ready.set_value();
                while (!stop_reader.load(std::memory_order_acquire)) {
                    const auto begin = Clock::now();
                    const auto result = _obj_inst->get_hs_pg(other_pg);
                    const auto end = Clock::now();
                    if (result == nullptr) { ++failed_lookups; }
                    ++probe_count;
                    const auto duration = elapsed_us(begin, end);
                    if (duration > probe_max_us) {
                        probe_max_us = duration;
                        probe_max_begin = begin;
                        probe_max_end = end;
                    }
                    std::this_thread::yield();
                }
            });
            ready.wait();
            const auto lock_begin = Clock::now();
            std::unique_lock lock(_obj_inst->_pg_lock);
            const auto acquired = Clock::now();
            const auto scan_begin = Clock::now();
            const auto count = _obj_inst->get_pg_tombstone_blob_count(scan_pg);
            const auto scan_end = Clock::now();
            lock.unlock();
            const auto released = Clock::now();
            stop_reader.store(true, std::memory_order_release);
            reader.join();
            ASSERT_EQ(count, expected_tombstones);
            ASSERT_EQ(failed_lookups, 0);
            LOGINFO("gc_scan_benchmark scan entries={} iteration={} lock_wait_us={} scan_us={} "
                    "test_lock_hold_us={} tombstones={}",
                    target, iteration, elapsed_us(lock_begin, acquired), elapsed_us(scan_begin, scan_end),
                    elapsed_us(acquired, released), count);
            LOGINFO("gc_scan_benchmark reader entries={} iteration={} baseline_samples={} baseline_max_us={} "
                    "probe_count={} probe_max_us={} max_overlaps_scan={} max_start_from_scan_start_us={} "
                    "max_end_from_scan_end_us={}",
                    target, iteration, baseline_samples, baseline_max_us, probe_count, probe_max_us,
                    probe_count > 0 && probe_max_begin < scan_end && probe_max_end > scan_begin,
                    probe_count > 0 ? elapsed_us(scan_begin, probe_max_begin) : 0,
                    probe_count > 0 ? elapsed_us(scan_end, probe_max_end) : 0);
        }

        // The probe starts while the writer owns the lock; timing still includes reader scheduling.
        Clock::time_point reader_begin;
        Clock::time_point reader_end;
        bool try_shared_succeeded{false};
        const HSHomeObject::HS_PG* other_result{nullptr};
        std::promise< void > reader_ready;
        auto ready = reader_ready.get_future();
        std::unique_lock lock(_obj_inst->_pg_lock);
        std::thread reader([&] {
            try_shared_succeeded = _obj_inst->_pg_lock.try_lock_shared();
            if (try_shared_succeeded) { _obj_inst->_pg_lock.unlock_shared(); }
            reader_begin = Clock::now();
            reader_ready.set_value();
            other_result = _obj_inst->get_hs_pg(other_pg);
            reader_end = Clock::now();
        });
        ready.wait();
        const auto scan_begin = Clock::now();
        const auto count = _obj_inst->get_pg_tombstone_blob_count(scan_pg);
        const auto scan_end = Clock::now();
        lock.unlock();
        const auto released = Clock::now();
        reader.join();
        ASSERT_FALSE(try_shared_succeeded);
        ASSERT_NE(other_result, nullptr);
        ASSERT_EQ(count, expected_tombstones);
        LOGINFO("gc_scan_benchmark other_pg entries={} scan_us={} lookup_us={} reader_start_to_scan_start_us={} "
                "unlock_sample_to_reader_end_us={}",
                target, elapsed_us(scan_begin, scan_end), elapsed_us(reader_begin, reader_end),
                elapsed_us(reader_begin, scan_begin), elapsed_us(released, reader_end));
    }
}
