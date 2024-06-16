#pragma once

#include "homeobject/common.hpp"

#include <homestore/chunk_selector.h>
#include <homestore/vchunk.h>
#include <homestore/homestore_decl.hpp>
#include <homestore/blk.h>

#include <queue>
#include <vector>
#include <mutex>
#include <functional>
#include <atomic>
#include <set>

namespace homeobject {

using csharedChunk = homestore::cshared< homestore::Chunk >;
using sharedChunk = homestore::shared< homestore::Chunk >;
using VChunk = homestore::VChunk;

class HeapChunkSelector : public homestore::ChunkSelector {
public:
    HeapChunkSelector() = default;
    ~HeapChunkSelector() = default;

    class VChunkComparator {
    public:
        bool operator()(VChunk& lhs, VChunk& rhs) { return lhs.available_blks() < rhs.available_blks(); }
    };
    using VChunkHeap = std::priority_queue< VChunk, std::vector< VChunk >, VChunkComparator >;
    using chunk_num_t = homestore::chunk_num_t;

    struct PerDevHeap {
        std::mutex mtx;
        VChunkHeap m_heap;
        std::atomic_size_t available_blk_count;
        uint64_t m_total_blks{0}; // initlized during boot, and will not change during runtime;
        uint32_t size() const { return m_heap.size(); }
    };

    void add_chunk(csharedChunk&) override;
    void foreach_chunks(std::function< void(csharedChunk&) >&& cb) override;
    csharedChunk select_chunk([[maybe_unused]] homestore::blk_count_t nblks, const homestore::blk_alloc_hints& hints);

    // this function will be used by GC flow or recovery flow to mark one specific chunk to be busy, caller should be
    // responsible to use release_chunk() interface to release it when no longer to use the chunk anymore.
    csharedChunk select_specific_chunk(const chunk_num_t);

    // this function is used to return a chunk back to ChunkSelector when sealing a shard, and will only be used by
    // Homeobject.
    void release_chunk(const chunk_num_t);

    // this should be called after ShardManager is initialized and get all the open shards
    void build_per_dev_chunk_heap(const std::unordered_set< chunk_num_t >& excludingChunks);

    /**
     * Retrieves the block allocation hints for a given chunk.
     *
     * @param chunk_id The ID of the chunk.
     * @return The block allocation hints for the specified chunk.
     */
    homestore::blk_alloc_hints chunk_to_hints(chunk_num_t chunk_id) const;

    /**
     * Returns the number of available blocks of the given device id.
     *
     * @param dev_id (optional) The device ID. if nullopt, it returns the maximum available blocks among all devices.
     * @return The number of available blocks.
     */
    uint64_t avail_blks(std::optional< uint32_t > dev_id) const;

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
     * Returns the number of available chunks for a given device ID.
     *
     * @param dev_id The device ID.
     * @return The number of available chunks.
     */
    uint32_t avail_num_chunks(uint32_t dev_id) const;

    /**
     * @brief Returns the total number of chunks.
     *
     * This function returns the total number of chunks in the heap chunk selector.
     *
     * @return The total number of chunks.
     */
    uint32_t total_chunks() const;

    std::set< uint32_t > get_all_pdev_ids() const;

    const std::vector< VChunk >& get_all_chunks(uint32_t pdev_id);

private:
    std::unordered_map< uint32_t, std::shared_ptr< PerDevHeap > > m_per_dev_heap;

    // hold all the chunks , selected or not
    std::unordered_map< chunk_num_t, csharedChunk > m_chunks;

    // hold all the chunks in each pdev
    std::unordered_map< uint32_t, std::vector< VChunk > > m_pdev_chunks;

    void add_chunk_internal(const chunk_num_t, bool add_to_heap = true);
};
} // namespace homeobject
