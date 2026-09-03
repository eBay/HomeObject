#pragma once
#include <array>
#include <string>

#include <fmt/format.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <folly/concurrency/ConcurrentHashMap.h>
#pragma GCC diagnostic pop
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/MPMCQueue.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <folly/futures/Future.h>
#pragma GCC diagnostic pop

#include <sisl/utility/enum.hpp>
#include <sisl/logging/logging.h>
#include <iomgr/iomgr.hpp>

#include <homestore/homestore.hpp>
#include <homestore/blk.h>
#include <homestore/index/index_table.hpp>

#include "heap_chunk_selector.h"
#include "index_kv.hpp"
#include "hs_backend_config.hpp"

namespace homeobject {

class HSHomeObject;

ENUM(task_priority, uint8_t, emergent = 0, normal, priority_count);

using chunk_id_t = homestore::chunk_num_t;
using GCBlobIndexTable = homestore::IndexTable< BlobRouteByChunkKey, BlobRouteValue >;

class GCManager {
public:
    GCManager(HSHomeObject* homeobject);
    ~GCManager();

    // Disallow copy and move
    GCManager(const GCManager&) = delete;
    GCManager(GCManager&&) = delete;
    GCManager& operator=(const GCManager&) = delete;
    GCManager& operator=(GCManager&&) = delete;

public:
    inline static auto const gc_actor_meta_name = std::string("GCActor");
    inline static auto const gc_task_meta_name = std::string("GCTask");
    inline static auto const gc_reserved_chunk_meta_name = std::string("GCReservedChunk");
    inline static atomic_uint64_t _gc_task_id{1}; // 0 is used for crash recovery

#pragma pack(1)
    struct gc_actor_superblk {
        uint32_t pdev_id;
        homestore::uuid_t index_table_uuid;
        uint64_t success_gc_task_count{0ull};
        uint64_t success_egc_task_count{0ull};
        uint64_t failed_gc_task_count{0ull};
        uint64_t failed_egc_task_count{0ull};
        uint64_t total_reclaimed_blk_count_by_gc{0ull};
        uint64_t total_reclaimed_blk_count_by_egc{0ull};
        static std::string name() { return gc_actor_meta_name; }
    };

    struct gc_task_superblk {
        chunk_id_t move_from_chunk;
        chunk_id_t move_to_chunk;
        chunk_id_t vchunk_id;
        pg_id_t pg_id;
        uint8_t priority;
        static std::string name() { return gc_task_meta_name; }
    };

    struct gc_reserved_chunk_superblk {
        chunk_id_t chunk_id;
        static std::string name() { return gc_reserved_chunk_meta_name; }
    };
#pragma pack()

public:
    // TODO: refine the rate limiter, currently it is a simple token bucket implementation.
    class RateLimiter {
        // TODO::make ratelimiter perceptive to client io, so gc can take more io resource if the io traffic from cline
        // is not heavy.this is an optimization.
    public:
        // refillRate means how many tokens can be refilled per second
        RateLimiter(uint64_t refill_count_per_second);
        ~RateLimiter() = default;
        // Disallow copy and move
        RateLimiter(RateLimiter&&) = delete;
        RateLimiter& operator=(RateLimiter&&) = delete;

    public:
        bool allowRequest(uint64_t count);

    private:
        void refillTokens();
        uint64_t tokens_;
        uint64_t refillRate_; // tokens per second
        std::chrono::steady_clock::time_point lastRefillTime_;
        std::mutex mutex_;
    };

public:
    class pdev_gc_actor {
    public:
        pdev_gc_actor(const homestore::superblk< GCManager::gc_actor_superblk >& gc_actor_sb,
                      std::shared_ptr< HeapChunkSelector > chunk_selector, HSHomeObject* homeobject);

        ~pdev_gc_actor();

        // Disallow copy and move
        pdev_gc_actor(const pdev_gc_actor&) = delete;
        pdev_gc_actor(pdev_gc_actor&&) = delete;
        pdev_gc_actor& operator=(const pdev_gc_actor&) = delete;
        pdev_gc_actor& operator=(pdev_gc_actor&&) = delete;

    public:
        struct pdev_gc_metrics : public sisl::MetricsGroup {
        public:
            pdev_gc_metrics(pdev_gc_actor const& gc_actor) :
                    sisl::MetricsGroup{"pdev_GC", std::to_string(gc_actor.get_pdev_id())},
                    gc_actor_(gc_actor),
                    blk_size_{homestore::data_service().get_blk_size()} {
                // We use replica_set_uuid instead of pg_id for metrics to make it globally unique to allow aggregating
                // across multiple nodes
                REGISTER_GAUGE(success_gc_task_count, "Number of successful gc tasks");
                REGISTER_GAUGE(success_egc_task_count, "Number of successful emergent gc tasks");
                REGISTER_GAUGE(failed_gc_task_count, "Number of failed gc tasks");
                REGISTER_GAUGE(failed_egc_task_count, "Number of failed emergent gc tasks");
                REGISTER_GAUGE(total_reclaimed_space_by_gc, "Total reclaimed space by gc task");
                REGISTER_GAUGE(total_reclaimed_space_by_egc, "Total reclaimed space by emergent gc task");
                REGISTER_COUNTER(gc_read_blk_count, "Total read blk count by gc in this pdev");
                REGISTER_COUNTER(gc_write_blk_count, "Total written blk count by gc in this pdev");

                // gc task level histogram metrics
                REGISTER_HISTOGRAM(
                    reclaim_ratio_gc, "the ratio of reclaimed blks to total blks in a gc task",
                    HistogramBucketsType(PercentileBuckets)); // 0% to 100% in 10 buckets (10% increments)
                REGISTER_HISTOGRAM(
                    gc_time_duration_s_gc, "how long a successful gc task takes by second",
                    HistogramBucketsType(LinearUpto64Buckets)); // 17 buckets covering 0-64 seconds in 4s increments

                REGISTER_HISTOGRAM(
                    reclaim_ratio_egc, "the ratio of reclaimed blks to total blks in an egc task",
                    HistogramBucketsType(PercentileBuckets)); // 0% to 100% in 10 buckets (10% increments)
                REGISTER_HISTOGRAM(
                    gc_time_duration_s_egc, "how long a successful egc task takes by second",
                    HistogramBucketsType(LinearUpto64Buckets)); // 17 buckets covering 0-64 seconds in 4s increments

                // Backlog / pressure snapshot gauges. Values refreshed once per scan cycle by
                // GCManager::scan_chunks_for_gc; worst-case staleness = gc_scan_interval_sec.
                REGISTER_GAUGE(pending_gc_bytes,
                               "Total reclaimable garbage bytes in PG-owned chunks on this pdev");
                REGISTER_GAUGE(eligible_gc_bytes,
                               "Reclaimable bytes currently eligible for normal GC on this pdev");
                REGISTER_GAUGE(eligible_gc_chunk_count,
                               "Chunks currently eligible for normal GC on this pdev");
                REGISTER_GAUGE(pending_normal_gc_task_count,
                               "Normal-priority GC tasks queued or running on this pdev");

                // Distribution of the pending backlog by garbage-ratio bucket. We register 10
                // gauges under a single Prometheus metric name (`pending_gc_chunks_ratio`),
                // differentiated by a `bucket` label. The REGISTER_GAUGE macro uses a compile-
                // time singleton per name and cannot express this shape (all 10 would collapse
                // to one gauge index), so we call the impl directly and store the returned
                // indices ourselves. Each label reads as "(lo, hi]% ratio"; the first bucket
                // is (0, 10]% because a chunk contributes only if it has garbage.
                //
                // Descriptions embed the bucket label because sisl's JSON dump keys entries by
                // description text (see MetricsGroupImpl::get_result_in_json in sisl); without
                // per-bucket descriptions the 10 registrations collapse to one JSON entry
                // (last-wins) and the test-log dumps become useless for verifying distribution
                // shape. Prometheus text output is unaffected — it uses HELP (unchanged across
                // registrations) and disambiguates series by labels.
                static constexpr std::array< const char*, 10 > kRatioBucketLabels = {
                    "00-10", "10-20", "20-30", "30-40", "40-50",
                    "50-60", "60-70", "70-80", "80-90", "90-100"};
                for (size_t i = 0; i < kRatioBucketLabels.size(); ++i) {
                    const auto lo = i * 10;
                    const auto hi = (i + 1) * 10;
                    const auto desc = fmt::format(
                        "Snapshot count of pending chunks with garbage ratio in ({}, {}]% "
                        "(bucket={})",
                        lo, hi, kRatioBucketLabels[i]);
                    ratio_bucket_indices_[i] = m_impl_ptr->register_gauge(
                        "pending_gc_chunks_ratio", desc, "" /* report_name */,
                        sisl::metric_label{"bucket", kRatioBucketLabels[i]});
                }

                register_me_to_farm();
                attach_gather_cb(std::bind(&pdev_gc_metrics::on_gather, this));
            }

            ~pdev_gc_metrics() { deregister_me_from_farm(); }
            pdev_gc_metrics(const pdev_gc_metrics&) = delete;
            pdev_gc_metrics(pdev_gc_metrics&&) noexcept = delete;
            pdev_gc_metrics& operator=(const pdev_gc_metrics&) = delete;
            pdev_gc_metrics& operator=(pdev_gc_metrics&&) noexcept = delete;

            void on_gather() {
                GAUGE_UPDATE(*this, success_gc_task_count,
                             gc_actor_.durable_entities().success_gc_task_count.load(std::memory_order_relaxed));
                GAUGE_UPDATE(*this, success_egc_task_count,
                             gc_actor_.durable_entities().success_egc_task_count.load(std::memory_order_relaxed));
                GAUGE_UPDATE(*this, failed_gc_task_count,
                             gc_actor_.durable_entities().failed_gc_task_count.load(std::memory_order_relaxed));
                GAUGE_UPDATE(*this, failed_egc_task_count,
                             gc_actor_.durable_entities().failed_egc_task_count.load(std::memory_order_relaxed));
                GAUGE_UPDATE(
                    *this, total_reclaimed_space_by_gc,
                    gc_actor_.durable_entities().total_reclaimed_blk_count_by_gc.load(std::memory_order_relaxed) *
                        blk_size_);
                GAUGE_UPDATE(
                    *this, total_reclaimed_space_by_egc,
                    gc_actor_.durable_entities().total_reclaimed_blk_count_by_egc.load(std::memory_order_relaxed) *
                        blk_size_);

                // Backlog / pressure snapshot gauges. Read the last-published scan accumulators.
                GAUGE_UPDATE(*this, pending_gc_bytes, gc_actor_.get_pending_gc_bytes());
                GAUGE_UPDATE(*this, eligible_gc_bytes, gc_actor_.get_eligible_gc_bytes());
                GAUGE_UPDATE(*this, eligible_gc_chunk_count, gc_actor_.get_eligible_gc_chunk_count());
                GAUGE_UPDATE(*this, pending_normal_gc_task_count, gc_actor_.get_pending_normal_task_count());

                // Bypass GAUGE_UPDATE for the same reason as bucket registration: we need to
                // address 10 distinct gauge indices that share one metric name.
                for (size_t i = 0; i < ratio_bucket_indices_.size(); ++i) {
                    m_impl_ptr->gauge_update(
                        ratio_bucket_indices_[i],
                        static_cast< int64_t >(gc_actor_.get_pending_ratio_bucket(i)));
                }
            }

        private:
            pdev_gc_actor const& gc_actor_;
            uint32_t blk_size_;
            // Indices returned by m_impl_ptr->register_gauge, one per ratio bucket. Populated
            // during construction and consumed by on_gather (see above). Kept as a member so we
            // can address each bucket by index — the compile-time-name macro cannot.
            std::array< uint64_t, 10 > ratio_bucket_indices_{};
        };

    public:
        struct DurableEntities {
            std::atomic< uint64_t > success_gc_task_count{0ull};
            std::atomic< uint64_t > success_egc_task_count{0ull};
            std::atomic< uint64_t > failed_gc_task_count{0ull};
            std::atomic< uint64_t > failed_egc_task_count{0ull};
            std::atomic< uint64_t > total_reclaimed_blk_count_by_gc{0ull};
            std::atomic< uint64_t > total_reclaimed_blk_count_by_egc{0ull};
        };

        std::atomic< bool > is_dirty_{false};

        void durable_entities_update(auto&& cb, bool dirty = true) {
            cb(durable_entities_);
            if (dirty) { is_dirty_.store(true, std::memory_order_relaxed); }
        }

        DurableEntities const& durable_entities() const { return durable_entities_; }

    public:
        struct gc_task_guard {
        public:
            gc_task_guard(uint8_t priority, pg_id_t pg_id, chunk_id_t move_from_chunk, chunk_id_t move_to_chunk,
                          chunk_id_t vchunk_id, uint64_t task_id, folly::Promise< bool >& task,
                          pdev_gc_actor* gc_actor) :
                    priority(priority),
                    pg_id(pg_id),
                    move_from_chunk(move_from_chunk),
                    move_to_chunk(move_to_chunk),
                    vchunk_id(vchunk_id),
                    task_id(task_id),
                    task(task),
                    m_gc_actor(gc_actor) {}

            ~gc_task_guard();

            // Disallow copy and move
            gc_task_guard(const gc_task_guard&) = delete;
            gc_task_guard(gc_task_guard&&) = delete;
            gc_task_guard& operator=(const gc_task_guard&) = delete;
            gc_task_guard& operator=(gc_task_guard&&) = delete;

        public:
            uint8_t priority;
            bool success{false};
            pg_id_t pg_id;
            chunk_id_t move_from_chunk;
            chunk_id_t move_to_chunk;
            chunk_id_t vchunk_id;
            uint64_t task_id;
            folly::Promise< bool >& task;
            pdev_gc_actor* m_gc_actor;
        };

    public:
        void add_reserved_chunk(homestore::superblk< GCManager::gc_reserved_chunk_superblk > reserved_chunk_sb);
        folly::SemiFuture< bool > add_gc_task(uint8_t priority, chunk_id_t move_from_chunk);
        void handle_recovered_gc_task(homestore::superblk< GCManager::gc_task_superblk >& gc_task_sb);
        void start();
        void stop();
        uint32_t get_pdev_id() const { return m_pdev_id; }

        // Returns the number of normal-priority GC tasks that are currently queued or running in
        // m_gc_executor. Used by scan_chunks_for_gc to enforce a cross-scan quota cap.
        uint32_t get_pending_normal_task_count() const {
            return m_pending_normal_gc_task_count.load(std::memory_order_relaxed);
        }

        // Snapshot readers used by pdev_gc_metrics::on_gather. Return the last value published
        // by GCManager::scan_chunks_for_gc for this pdev; 0 before the first scan completes.
        uint64_t get_pending_gc_bytes() const {
            return m_pending_gc_bytes.load(std::memory_order_relaxed);
        }
        uint64_t get_eligible_gc_bytes() const {
            return m_eligible_gc_bytes.load(std::memory_order_relaxed);
        }
        uint32_t get_eligible_gc_chunk_count() const {
            return m_eligible_gc_chunk_count.load(std::memory_order_relaxed);
        }
        uint32_t get_pending_ratio_bucket(size_t bucket_idx) const {
            return m_pending_ratio_buckets[bucket_idx].load(std::memory_order_relaxed);
        }

        // Publishes a full backlog snapshot atomically-per-field. Callers (the scanner) compute
        // the totals locally over all chunks on this pdev, then hand them in via one call so the
        // metrics stay internally consistent within a scan cycle. Between-gauge drift is bounded
        // by one scan interval; individual scalars are aligned and therefore torn-read safe.
        void publish_scan_snapshot(uint64_t pending_bytes, uint64_t eligible_bytes,
                                   uint32_t eligible_chunks,
                                   const std::array< uint32_t, 10 >& ratio_buckets) {
            m_pending_gc_bytes.store(pending_bytes, std::memory_order_relaxed);
            m_eligible_gc_bytes.store(eligible_bytes, std::memory_order_relaxed);
            m_eligible_gc_chunk_count.store(eligible_chunks, std::memory_order_relaxed);
            for (size_t i = 0; i < ratio_buckets.size(); ++i) {
                m_pending_ratio_buckets[i].store(ratio_buckets[i], std::memory_order_relaxed);
            }
        }

    private:
        void process_gc_task(chunk_id_t move_from_chunk, uint8_t priority, folly::Promise< bool > task,
                             const uint64_t task_id);

        // this should be called only after gc_task meta blk is persisted. it will update the pg index table according
        // to the gc index table. return the move_to_chunk to chunkselector and put move_from_chunk to reserved chunk
        // queue.
        bool
        replace_blob_index(chunk_id_t move_from_chunk, chunk_id_t move_to_chunk,
                           const std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > >& valid_blob_indexes,
                           const uint64_t task_id);

        // copy all the valid data from the move_from_chunk to move_to_chunk. valid data means those blobs that are not
        // tombstone in the pg index table
        // return true if the data copy is successful, false otherwise.
        bool copy_valid_data(chunk_id_t move_from_chunk, chunk_id_t move_to_chunk,
                             folly::ConcurrentHashMap< BlobRouteByChunk, BlobRouteValue >& copied_blobs,
                             const uint8_t priority, const uint64_t task_id);

        // before we select a reserved chunk and start gc, we need:
        //  1 clear all the entries of this chunk in the gc index table
        //  2 reset this chunk to make sure it is empty.
        bool purge_reserved_chunk(chunk_id_t move_to_chunk, const uint64_t task_id, const pg_id_t pg_id);

        bool get_blobs_to_replace(chunk_id_t move_to_chunk,
                                  std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > >& valid_blob_indexes,
                                  const uint64_t task_id, const pg_id_t pg_id);

        // this function aims to execute the logic after gc_meta_blk has been persisted, which will shared by normal gc
        // case and recvoery case
        bool process_after_gc_metablk_persisted(
            homestore::superblk< GCManager::gc_task_superblk >& gc_task_sb,
            const std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > >& valid_blob_indexes,
            const uint64_t task_id);

        bool check_blob_consistency(
            folly::ConcurrentHashMap< BlobRouteByChunk, BlobRouteValue > const& copied_blobs,
            std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > > const& valid_blob_indexes,
            const uint64_t task_id, const pg_id_t pg_id);

        void on_gc_task_completed(uint8_t priority, pg_id_t pg_id, chunk_id_t move_from_chunk, chunk_id_t move_to_chunk,
                                  uint64_t vchunk_id, bool success, const uint64_t task_id);

        pdev_gc_metrics& metrics() { return metrics_; }

    private:
        friend class gc_task_guard;
        uint32_t m_pdev_id;
        std::shared_ptr< HeapChunkSelector > m_chunk_selector;
        folly::MPMCQueue< chunk_id_t > m_reserved_chunk_queue;
        std::shared_ptr< GCBlobIndexTable > m_index_table;
        HSHomeObject* m_hs_home_object{nullptr};
        bool m_enable_read_verify;

        // limit the io resource that gc thread can take, so that it will not impact the client io.
        // assuming the throughput of a HDD is 300M/s(including read and write) and gc can take 10% of the io resource,
        // which is 30M/s. A block is 4K, so gc can read/write 30M/s / 4K = 7680 blocks per second.
        RateLimiter m_rate_limiter{HS_BACKEND_DYNAMIC_CONFIG(max_read_write_block_count_per_second)};

        std::shared_ptr< folly::IOThreadPoolExecutor > m_gc_executor;
        std::shared_ptr< folly::IOThreadPoolExecutor > m_egc_executor;
        std::atomic_bool m_is_stopped{true};
        // Tracks normal-priority GC tasks that are queued or actively running in m_gc_executor.
        // Incremented in add_gc_task after a task is enqueued; decremented in on_gc_task_completed.
        // Used by scan_chunks_for_gc to enforce a true cross-scan quota cap.
        std::atomic< uint32_t > m_pending_normal_gc_task_count{0};

        // Snapshot accumulators for the backlog / pressure gauges (see publish_scan_snapshot).
        // Refreshed once per pdev iteration in GCManager::scan_chunks_for_gc; consumed by
        // pdev_gc_metrics::on_gather via the get_* accessors above. Worst-case staleness =
        // gc_scan_interval_sec. Relaxed ordering is sufficient because each metric is an aligned
        // scalar and the values are advisory snapshots, not synchronization state.
        std::atomic< uint64_t > m_pending_gc_bytes{0};
        std::atomic< uint64_t > m_eligible_gc_bytes{0};
        std::atomic< uint32_t > m_eligible_gc_chunk_count{0};
        std::array< std::atomic< uint32_t >, 10 > m_pending_ratio_buckets{};
        // since we have a very small number of reserved chunks, a vector is enough
        // TODO:: use a map if we have a large number of reserved chunks
        std::vector< homestore::superblk< GCManager::gc_reserved_chunk_superblk > > m_reserved_chunks;

        // metrics
        pdev_gc_metrics metrics_;
        DurableEntities durable_entities_;
    };

public:
    /**
     * sumbit a gc task
     * @param chunk_id ID of the chunk
     * @param priority the priority of this task
     *
     * @return the future to wait for the task to be completed. false means gc task fails.
     * TODO:: add error code as the returned value to indicate the reason of failure.
     */
    folly::SemiFuture< bool > submit_gc_task(task_priority priority, chunk_id_t chunk_id);

    /**
     * try to create a new gc actor for a pdev
     * @param pdev_id ID of the pdev.
     *
     * @return the created or existing(if already exists) gc actor
     */
    std::shared_ptr< pdev_gc_actor >
    try_create_pdev_gc_actor(uint32_t pdev_id, const homestore::superblk< GCManager::gc_actor_superblk >& gc_actor_sb);

    // Returns the garbage ratio percentage [0.0, 100.0] for the given chunk if it is a valid GC candidate,
    // or 0.0 if the chunk is not eligible (wrong state, no defrag blks, no pg, or pg not gc-able).
    // Uses floating-point arithmetic to avoid truncation for chunks with very few defrag blocks.
    float get_chunk_gc_ratio(chunk_id_t chunk_id);

    // One-shot snapshot of chunk state relevant to GC decisions. Populated with a single
    // ExtendedVChunk lookup so scan_chunks_for_gc can compute both submission decisions AND
    // backlog metrics without a second hash+lock roundtrip. `gc_thresh_low` is the low-watermark
    // percentage used to set the `eligible` flag; pass 0 if you only care about is_gc_candidate.
    struct ChunkGCSnapshot {
        uint32_t defrag_blks = 0;
        uint32_t total_blks = 0;
        float ratio_pct = 0.0f; // 100.0 * defrag_blks / total_blks; 0 if defrag_blks == 0
        bool has_pg = false;
        bool is_gc_candidate = false; // AVAILABLE && has_pg && pg_alive && defrag_blks > 0
        bool eligible = false;        // is_gc_candidate && ratio_pct > gc_thresh_low
    };
    ChunkGCSnapshot get_chunk_gc_snapshot(chunk_id_t chunk_id, uint8_t gc_thresh_low);

    void handle_all_recovered_gc_tasks();

    void start();
    void stop();

    // the following two functions should not be called concurrently. if we need to call them concurrently, we need to
    // add lock to protect
    void start_gc_scan_timer();
    void stop_gc_scan_timer();

    void scan_chunks_for_gc();
    void drain_pg_pending_gc_task(const pg_id_t pg_id);
    void decr_pg_pending_gc_task(const pg_id_t pg_id);
    void incr_pg_pending_gc_task(const pg_id_t pg_id);
    auto& get_gc_actor_superblks() { return m_gc_actor_sbs; }
    std::shared_ptr< pdev_gc_actor > get_pdev_gc_actor(uint32_t pdev_id);

private:
    void on_gc_task_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie);
    void on_gc_actor_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie);
    void on_reserved_chunk_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie);

private:
    std::shared_ptr< HeapChunkSelector > m_chunk_selector;
    folly::ConcurrentHashMap< uint32_t, std::shared_ptr< pdev_gc_actor > > m_pdev_gc_actors;
    iomgr::timer_handle_t m_gc_timer_hdl{iomgr::null_timer_handle};
    iomgr::io_fiber_t m_gc_timer_fiber{nullptr};
    HSHomeObject* m_hs_home_object{nullptr};
    std::list< homestore::superblk< GCManager::gc_task_superblk > > m_recovered_gc_tasks;
    std::unordered_map< pg_id_t, atomic_uint64_t > m_pending_gc_task_num_per_pg;
    std::mutex m_pending_gc_task_mtx;
    std::vector< homestore::superblk< GCManager::gc_actor_superblk > > m_gc_actor_sbs;
};

} // namespace homeobject
