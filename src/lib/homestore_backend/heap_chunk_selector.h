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

    using VChunkHeap = std::priority_queue< VChunk, std::vector< VChunk >, VChunkComparator >;
    using chunk_num_t = homestore::chunk_num_t;

    struct PerDevHeap {
        std::mutex mtx;
        VChunkHeap m_heap;
        std::atomic_size_t available_blk_count;
    };

    void add_chunk(csharedChunk&) override;
    void foreach_chunks(std::function< void(csharedChunk&) >&& cb) override;
    csharedChunk select_chunk([[maybe_unused]] homestore::blk_count_t nblks, const homestore::blk_alloc_hints& hints);

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

    void add_chunk_internal(const chunk_num_t);
};
} // namespace homeobject
