#pragma once

#include <homestore/chunk_selector.h>
#include <homestore/vchunk.h>
#include <homestore/homestore_decl.hpp>

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
    void release_chunk(const uint16_t);

    // homestore will initialize HeapChunkSelector by adding all the chunks. but some of them are already
    // selected by open shards. so after homeobject restarts, we need to mark all these chunks as selected
    void mark_chunk_selected(const uint16_t);

private:
    std::unordered_map< uint32_t, std::shared_ptr< PerDevHeap > > m_per_dev_heap;

    // hold all the chunks , selected or not
    std::unordered_map< uint16_t, csharedChunk > m_chunks;
};
} // namespace homeobject