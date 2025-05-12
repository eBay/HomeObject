#pragma once
#include <string>

#include <folly/concurrency/PriorityUnboundedQueueSet.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/MPMCQueue.h>
#include <folly/futures/Future.h>

#include <sisl/utility/enum.hpp>
#include <sisl/logging/logging.h>
#include <iomgr/iomgr.hpp>

#include <homestore/homestore.hpp>
#include <homestore/blk.h>
#include <homestore/index/index_table.hpp>

#include "heap_chunk_selector.h"
#include "index_kv.hpp"

namespace homeobject {

class HSHomeObject;
// TODO:: make those #define below configurable

// Default number of chunks reserved for GC per pdev
#define RESERVED_CHUNK_NUM_PER_PDEV 4
// GC interval in seconds, this is used to schedule the GC timer
#define GC_SCAN_INTERVAL_SEC 10llu
// Garbage rate threshold, bigger than which, a gc task will be scheduled for a chunk
#define GC_GARBAGE_RATE_THRESHOLD 80
// limit the io resource that gc thread can take, so that it will not impact the client io.
// assuming the throughput of a HDD is 300M/s(including read and write) and gc can take 10% of the io resource, which is
// 30M/s. A block is 4K, so gc can read/write 30M/s / 4K = 7680 blocks per second.
#define MAX_READ_WRITE_BLOCK_COUNT_PER_SECOND 7680

ENUM(task_priority, uint8_t, emergent = 0, normal, priority_count);

using chunk_id_t = homestore::chunk_num_t;
using GCBlobIndexTable = homestore::IndexTable< BlobRouteByChunkKey, BlobRouteValue >;

class GCManager {
public:
    GCManager(std::shared_ptr< HeapChunkSelector > chunk_selector, HSHomeObject* homeobject);
    ~GCManager();

    // Disallow copy and move
    GCManager(const GCManager&) = delete;
    GCManager(GCManager&&) = delete;
    GCManager& operator=(const GCManager&) = delete;
    GCManager& operator=(GCManager&&) = delete;

public:
    inline static auto const _gc_actor_meta_name = std::string("GCActor");
    inline static auto const _gc_task_meta_name = std::string("GCTask");
    inline static auto const _gc_reserved_chunk_meta_name = std::string("GCReservedChunk");

#pragma pack(1)
    struct gc_actor_superblk {
        uint32_t pdev_id;
        homestore::uuid_t index_table_uuid;
        static std::string name() { return _gc_actor_meta_name; }
    };

    struct gc_task_superblk {
        chunk_id_t move_from_chunk;
        chunk_id_t move_to_chunk;
        uint8_t priority;
        static std::string name() { return _gc_task_meta_name; }
    };

    struct gc_reserved_chunk_superblk {
        chunk_id_t chunk_id;
        static std::string name() { return _gc_actor_meta_name; }
    };
#pragma pack()

public:
    class gc_task {
    public:
        gc_task(chunk_id_t chunk_id, uint8_t priority, folly::Promise< bool > promise) :
                move_from_chunk_id_{chunk_id}, priority_{priority}, promise_{std ::move(promise)} {};

        gc_task() : move_from_chunk_id_{0}, priority_{0}, promise_{folly::Promise< bool >()} {};

        gc_task(gc_task&& other) noexcept :
                move_from_chunk_id_(other.move_from_chunk_id_),
                priority_(other.priority_),
                promise_(std::move(other.promise_)) {}

        gc_task& operator=(gc_task&& other) noexcept {
            if (this != &other) {
                move_from_chunk_id_ = other.move_from_chunk_id_;
                priority_ = other.priority_;
                promise_ = std::move(other.promise_);
            }
            return *this;
        }

        // disallow copy
        gc_task(const gc_task&) = delete;
        gc_task& operator=(const gc_task&) = delete;

    public:
        chunk_id_t get_move_from_chunk_id() const { return move_from_chunk_id_; }
        uint8_t get_priority() const { return priority_; }
        void complete(bool success) { promise_.setValue(success); }

    private:
        chunk_id_t move_from_chunk_id_;
        uint8_t priority_;
        folly::Promise< bool > promise_;
    };

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
        pdev_gc_actor(uint32_t pdev_id, std::shared_ptr< HeapChunkSelector > chunk_selector,
                      std::shared_ptr< GCBlobIndexTable > index_table, HSHomeObject* homeobject);
        ~pdev_gc_actor();

        // Disallow copy and move
        pdev_gc_actor(const pdev_gc_actor&) = delete;
        pdev_gc_actor(pdev_gc_actor&&) = delete;
        pdev_gc_actor& operator=(const pdev_gc_actor&) = delete;
        pdev_gc_actor& operator=(pdev_gc_actor&&) = delete;

    public:
        void add_reserved_chunk(chunk_id_t chunk_id);
        void add_gc_task(task_priority priority, gc_task task);
        void handle_recovered_gc_task(const GCManager::gc_task_superblk* gc_task);
        void start();
        void stop();

    private:
        void process_gc_task(gc_task& task);

        // this should be called only after gc_task meta blk is persisted. it will update the pg index table according
        // to the gc index table. return the move_to_chunk to chunkselector and put move_from_chunk to reserved chunk
        // queue.
        void replace_blob_index(chunk_id_t move_from_chunk, chunk_id_t move_to_chunk, uint8_t priority);

        // copy all the valid data from the move_from_chunk to move_to_chunk. valid data means those blobs that are not
        // tombstone in the pg index table
        // return true if the data copy is successful, false otherwise.
        bool copy_valid_data(chunk_id_t move_from_chunk, chunk_id_t move_to_chunk, bool is_emergent = false);

        // before we select a reserved chunk and start gc, we need:
        //  1 clear all the entries of this chunk in the gc index table
        //  2 reset this chunk to make sure it is empty.
        void purge_reserved_chunk(chunk_id_t move_to_chunk);

    private:
        // utils
        sisl::sg_list generate_shard_super_blk_sg_list(shard_id_t shard_id);

    private:
        uint32_t m_pdev_id;
        std::shared_ptr< HeapChunkSelector > m_chunk_selector;
        // multi producer , multi consumer , no-blocking priority task queue
        folly::PriorityUMPMCQueueSet< gc_task, false > m_gc_task_queue;
        folly::MPMCQueue< chunk_id_t > m_reserved_chunk_queue;
        std::shared_ptr< GCBlobIndexTable > m_index_table;
        HSHomeObject* m_hs_home_object{nullptr};
        RateLimiter m_rate_limiter{MAX_READ_WRITE_BLOCK_COUNT_PER_SECOND};
        std::shared_ptr< folly::IOThreadPoolExecutor > m_gc_executor;
        std::atomic_bool m_is_stopped{true};
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
    std::shared_ptr< pdev_gc_actor > try_create_pdev_gc_actor(uint32_t pdev_id,
                                                              std::shared_ptr< GCBlobIndexTable > index_table);
    std::shared_ptr< pdev_gc_actor > get_pdev_gc_actor(uint32_t pdev_id);

    static bool is_eligible_for_gc(std::shared_ptr< HeapChunkSelector::ExtendedVChunk > chunk);

    void start();
    void stop();

private:
    void scan_chunks_for_gc();

private:
    std::shared_ptr< HeapChunkSelector > m_chunk_selector;
    folly::ConcurrentHashMap< uint32_t, std::shared_ptr< pdev_gc_actor > > m_pdev_gc_actors;
    iomgr::timer_handle_t m_gc_timer_hdl{iomgr::null_timer_handle};
    HSHomeObject* m_hs_home_object{nullptr};
};

} // namespace homeobject