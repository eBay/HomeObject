#pragma once
#include <string>

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
#include "hs_backend_config.hpp"

namespace homeobject {

class HSHomeObject;

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
    inline static atomic_uint64_t _gc_task_id{1}; // 0 is used for crash recovery

#pragma pack(1)
    struct gc_actor_superblk {
        uint32_t pdev_id;
        homestore::uuid_t index_table_uuid;
        static std::string name() { return _gc_actor_meta_name; }
    };

    struct gc_task_superblk {
        chunk_id_t move_from_chunk;
        chunk_id_t move_to_chunk;
        chunk_id_t vchunk_id;
        pg_id_t pg_id;
        uint8_t priority;
        static std::string name() { return _gc_task_meta_name; }
    };

    struct gc_reserved_chunk_superblk {
        chunk_id_t chunk_id;
        static std::string name() { return _gc_reserved_chunk_meta_name; }
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
        pdev_gc_actor(uint32_t pdev_id, std::shared_ptr< HeapChunkSelector > chunk_selector,
                      std::shared_ptr< GCBlobIndexTable > index_table, HSHomeObject* homeobject);
        ~pdev_gc_actor();

        // Disallow copy and move
        pdev_gc_actor(const pdev_gc_actor&) = delete;
        pdev_gc_actor(pdev_gc_actor&&) = delete;
        pdev_gc_actor& operator=(const pdev_gc_actor&) = delete;
        pdev_gc_actor& operator=(pdev_gc_actor&&) = delete;

    public:
        void add_reserved_chunk(homestore::superblk< GCManager::gc_reserved_chunk_superblk > reserved_chunk_sb);
        folly::SemiFuture< bool > add_gc_task(uint8_t priority, chunk_id_t move_from_chunk);
        void handle_recovered_gc_task(homestore::superblk< GCManager::gc_task_superblk >& gc_task_sb);
        void start();
        void stop();

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
        bool copy_valid_data(chunk_id_t move_from_chunk, chunk_id_t move_to_chunk, const uint64_t task_id,
                             bool is_emergent = false);

        // before we select a reserved chunk and start gc, we need:
        //  1 clear all the entries of this chunk in the gc index table
        //  2 reset this chunk to make sure it is empty.
        bool purge_reserved_chunk(chunk_id_t move_to_chunk);

        bool get_blobs_to_replace(chunk_id_t move_to_chunk,
                                  std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > >& valid_blob_indexes);

        // this function aims to execute the logic after gc_meta_blk has been persisted, which will shared by normal gc
        // case and recvoery case
        bool process_after_gc_metablk_persisted(
            homestore::superblk< GCManager::gc_task_superblk >& gc_task_sb,
            const std::vector< std::pair< BlobRouteByChunkKey, BlobRouteValue > >& valid_blob_indexes,
            const uint64_t task_id);

        void handle_error_before_persisting_gc_metablk(chunk_id_t move_from_chunk, chunk_id_t move_to_chunk,
                                                       folly::Promise< bool > task, const uint64_t task_id,
                                                       uint8_t priority);

    private:
        // utils
        sisl::sg_list generate_shard_super_blk_sg_list(shard_id_t shard_id);

    private:
        uint32_t m_pdev_id;
        std::shared_ptr< HeapChunkSelector > m_chunk_selector;
        folly::MPMCQueue< chunk_id_t > m_reserved_chunk_queue;
        std::shared_ptr< GCBlobIndexTable > m_index_table;
        HSHomeObject* m_hs_home_object{nullptr};

        // limit the io resource that gc thread can take, so that it will not impact the client io.
        // assuming the throughput of a HDD is 300M/s(including read and write) and gc can take 10% of the io resource,
        // which is 30M/s. A block is 4K, so gc can read/write 30M/s / 4K = 7680 blocks per second.
        RateLimiter m_rate_limiter{HS_BACKEND_DYNAMIC_CONFIG(max_read_write_block_count_per_second)};

        std::shared_ptr< folly::IOThreadPoolExecutor > m_gc_executor;
        std::shared_ptr< folly::IOThreadPoolExecutor > m_egc_executor;
        std::atomic_bool m_is_stopped{true};
        // since we have a very small number of reserved chunks, a vector is enough
        // TODO:: use a map if we have a large number of reserved chunks
        std::vector< homestore::superblk< GCManager::gc_reserved_chunk_superblk > > m_reserved_chunks;
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

    bool is_eligible_for_gc(chunk_id_t chunk_id);

    void start();
    void stop();
    bool is_started();

    void scan_chunks_for_gc();
    void handle_all_recovered_gc_tasks();
    void drain_pg_pending_gc_task(const pg_id_t pg_id);
    void decr_pg_pending_gc_task(const pg_id_t pg_id);
    void incr_pg_pending_gc_task(const pg_id_t pg_id);

private:
    void on_gc_task_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie);
    void on_gc_actor_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie);
    void on_reserved_chunk_meta_blk_found(sisl::byte_view const& buf, void* meta_cookie);
    std::shared_ptr< pdev_gc_actor > get_pdev_gc_actor(uint32_t pdev_id);

private:
    std::shared_ptr< HeapChunkSelector > m_chunk_selector;
    folly::ConcurrentHashMap< uint32_t, std::shared_ptr< pdev_gc_actor > > m_pdev_gc_actors;
    iomgr::timer_handle_t m_gc_timer_hdl{iomgr::null_timer_handle};
    HSHomeObject* m_hs_home_object{nullptr};
    std::list< homestore::superblk< GCManager::gc_task_superblk > > m_recovered_gc_tasks;
    std::unordered_map< pg_id_t, atomic_uint64_t > m_pending_gc_task_num_per_pg;
    std::mutex m_pending_gc_task_mtx;
};

} // namespace homeobject