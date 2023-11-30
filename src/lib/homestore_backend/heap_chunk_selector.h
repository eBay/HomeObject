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

namespace homeobject {

using csharedChunk = homestore::cshared< homestore::Chunk >;

class HeapChunkSelector : public homestore::ChunkSelector {
public:
    HeapChunkSelector() = default;
    ~HeapChunkSelector() = default;

    using VChunk = homestore::VChunk;
    class VChunkComparator {
    public:
        bool operator()(VChunk& lhs, VChunk& rhs) { return lhs.available_blks() < rhs.available_blks(); }
    };

    class VChunkDefragComparator {
    public:
        bool operator()(VChunk& lhs, VChunk& rhs) { return lhs.get_defrag_nblks() < rhs.get_defrag_nblks(); }
    };

    using VChunkHeap = std::priority_queue< VChunk, std::vector< VChunk >, VChunkComparator >;
    using VChunkDefragHeap = std::priority_queue< VChunk, std::vector< VChunk >, VChunkDefragComparator >;
    using chunk_num_t = homestore::chunk_num_t;

    struct PerDevHeap {
        std::mutex mtx;
        VChunkHeap m_heap;
        std::atomic_size_t available_blk_count;
    };

    void add_chunk(csharedChunk&) override;
    void foreach_chunks(std::function< void(csharedChunk&) >&& cb) override;
    csharedChunk select_chunk([[maybe_unused]] homestore::blk_count_t nblks, const homestore::blk_alloc_hints& hints);

    // this function will be used by GC flow or recovery flow to mark one specific chunk to be busy, caller should be
    // responsible to use release_chunk() interface to release it when no longer to use the chunk anymore.
    csharedChunk select_specific_chunk(const chunk_num_t);

    // this function will be used by GC flow to select a chunk for GC
    csharedChunk most_defrag_chunk();

    // this function is used to return a chunk back to ChunkSelector when sealing a shard, and will only be used by
    // Homeobject.
    void release_chunk(const chunk_num_t);

    // this should be called after ShardManager is initialized and get all the open shards
    void build_per_dev_chunk_heap(const std::unordered_set< chunk_num_t >& excludingChunks);

    homestore::blk_alloc_hints chunk_to_hints(chunk_num_t chunk_id) const;

private:
    std::unordered_map< uint32_t, std::shared_ptr< PerDevHeap > > m_per_dev_heap;

    // hold all the chunks , selected or not
    std::unordered_map< chunk_num_t, csharedChunk > m_chunks;

    VChunkDefragHeap m_defrag_heap;
    std::mutex m_defrag_mtx;

    void add_chunk_internal(const chunk_num_t);
    void remove_chunk_from_defrag_heap(const chunk_num_t);
};
} // namespace homeobject
