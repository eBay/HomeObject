#pragma once

#include "homeobject/common.hpp"

#include <homestore/chunk_selector.h>
#include <homestore/vchunk.h>
#include <homestore/homestore_decl.hpp>
#include <homestore/blk.h>
#include <sisl/utility/enum.hpp>

#include <queue>
#include <vector>
#include <mutex>
#include <functional>
#include <atomic>

namespace homeobject {

ENUM(ChunkState, uint8_t, AVAILABLE = 0, INUSE);

using csharedChunk = homestore::cshared< homestore::Chunk >;

class HeapChunkSelector : public homestore::ChunkSelector {
public:
    HeapChunkSelector() = default;
    ~HeapChunkSelector() = default;

    using VChunk = homestore::VChunk;
    using chunk_num_t = homestore::chunk_num_t;

    class ExtendedVChunk : public VChunk {
    public:
        ExtendedVChunk(csharedChunk const& chunk) :
                VChunk(chunk), m_state(ChunkState::AVAILABLE), m_pg_id(), m_v_chunk_id() {}
        ~ExtendedVChunk() = default;
        ChunkState m_state;
        std::optional< pg_id_t > m_pg_id;
        std::optional< chunk_num_t > m_v_chunk_id;
        bool available() const { return m_state == ChunkState::AVAILABLE; }
    };

    class ExtendedVChunkComparator {
    public:
        bool operator()(std::shared_ptr< ExtendedVChunk >& lhs, std::shared_ptr< ExtendedVChunk >& rhs) {
            return lhs->available_blks() < rhs->available_blks();
        }
    };
    using ExtendedVChunkHeap =
        std::priority_queue< std::shared_ptr< ExtendedVChunk >, std::vector< std::shared_ptr< ExtendedVChunk > >,
                             ExtendedVChunkComparator >;

    struct ChunkHeap {
        std::mutex mtx;
        ExtendedVChunkHeap m_heap;
        std::atomic_size_t available_blk_count;
        uint64_t m_total_blks{0}; // initlized during boot, and will not change during runtime;
        uint32_t size() const { return m_heap.size(); }
    };

    struct PGChunkCollection {
        std::mutex mtx;
        std::vector< std::shared_ptr< ExtendedVChunk > > m_pg_chunks;
        std::atomic_size_t available_num_chunks;
        std::atomic_size_t available_blk_count;
        uint64_t m_total_blks{0}; // initlized during boot, and will not change during runtime;
    };

    void add_chunk(csharedChunk&) override;

    void foreach_chunks(std::function< void(csharedChunk&) >&& cb) override;

    csharedChunk select_chunk([[maybe_unused]] homestore::blk_count_t nblks, const homestore::blk_alloc_hints& hints);

    // this function will be used by create shard or recovery flow to mark one specific chunk to be busy, caller should
    // be responsible to use release_chunk() interface to release it when no longer to use the chunk anymore.
    csharedChunk select_specific_chunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id);

    // This function returns a chunk back to ChunkSelector.
    // It is used in two scenarios: 1. seal shard  2. create shard rollback
    bool release_chunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id);

    /**
     * select chunks for pg, chunks need to be in same pdev.
     *
     * @param pg_id The ID of the pg.
     * @param pg_size The fix pg size.
     * @return An optional uint32_t value representing num_chunk, or std::nullopt if no space left.
     */
    std::optional< uint32_t > select_chunks_for_pg(pg_id_t pg_id, uint64_t pg_size);

    // this function is used for pg info superblk persist v_chunk_id <-> p_chunk_id
    std::shared_ptr< const std::vector< chunk_num_t > > get_pg_chunks(pg_id_t pg_id) const;

    /**
     * pop pg top chunk
     *
     * @param pg_id The ID of the pg.
     * @return An optional chunk_num_t value representing v_chunk_id, or std::nullopt if no space left.
     */
    std::optional< chunk_num_t > get_most_available_blk_chunk(pg_id_t pg_id) const;

    // this should be called on each pg meta blk found
    bool recover_pg_chunks(pg_id_t pg_id, std::vector< chunk_num_t >&& p_chunk_ids);

    // this should be called after all pg meta blk recovered
    void recover_per_dev_chunk_heap();

    // this should be called after ShardManager is initialized and get all the open shards
    bool recover_pg_chunks_states(pg_id_t pg_id, const std::unordered_set< chunk_num_t >& excluding_v_chunk_ids);

    /**
     * Returns the number of available blocks of the given device id.
     *
     * @param dev_id (optional) The device ID. if nullopt, it returns the maximum available blocks among all devices.
     * @return The number of available blocks.
     */
    uint64_t avail_blks(pg_id_t pg_id) const;

    /**
     * Returns the total number of blocks of the given device;
     *
     * @param dev_id The device ID.
     * @return The total number of blocks.
     */
    uint64_t total_blks(uint32_t dev_id) const;

    /**
     * Returns the maximum number of chunks on pdev that are currently available for allocation.
     * Caller is not interested with the pdev id;
     *
     * @return The number of available chunks.
     */
    uint32_t most_avail_num_chunks() const;

    /**
     * Returns the number of available chunks for a given pg id.
     *
     * @param pg_id The pg id.
     * @return The number of available chunks.
     */
    uint32_t avail_num_chunks(pg_id_t pg_id) const;

    /**
     * @brief Returns the total number of chunks.
     *
     * This function returns the total number of chunks in the heap chunk selector.
     *
     * @return The total number of chunks.
     */
    uint32_t total_chunks() const;

    uint32_t get_chunk_size() const;

private:
    void add_chunk_internal(const chunk_num_t, bool add_to_heap = true);

private:
    std::unordered_map< uint32_t, std::shared_ptr< ChunkHeap > > m_per_dev_heap;

    std::unordered_map< pg_id_t, std::shared_ptr< PGChunkCollection > > m_per_pg_chunks;
    // hold all the chunks , selected or not
    std::unordered_map< chunk_num_t, homestore::cshared< ExtendedVChunk > > m_chunks;

    mutable std::shared_mutex m_chunk_selector_mtx;
};
} // namespace homeobject
