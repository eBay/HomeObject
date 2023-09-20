#pragma once

#include <homestore/chunk_selector.h>
#include <homestore/vchunk.h>
#include <homestore/homestore_decl.hpp>

#include <queue>
#include <vector>
#include <mutex>
#include <utility>
#include <memory>
#include <functional>
#include <atomic>

#include <folly/AtomicHashMap.h>

namespace homeobject {

using csharedChunk = homestore::cshared< homestore::Chunk >;

class HeapChunkSelector : public homestore::ChunkSelector {
public:
    HeapChunkSelector();
    HeapChunkSelector(const uint32_t&, const uint32_t&, const uint32_t&);
    ~HeapChunkSelector() = default;

    using VChunk = homestore::VChunk;
    class VChunkComparator {
    public:
        bool operator()(VChunk& lhs, VChunk& rhs) { return lhs.available_blks() < rhs.available_blks(); }
    };

    using VChunkHeap = std::priority_queue< VChunk, std::vector< VChunk >, VChunkComparator >;

    void add_chunk(csharedChunk&) override;
    void foreach_chunks(std::function< void(csharedChunk&) >&& cb) override;
    csharedChunk select_chunk([[maybe_unused]] homestore::blk_count_t nblks, const homestore::blk_alloc_hints& hints);

    // this function is used to return a chunk back to ChunkSelector when sealing a shard, and will only be used by
    // Homeobject.
    void release_chunk(const uint32_t);

    // we use 64K for the initial size
    static constexpr uint32_t chunk_atomicmap_init_size = 1 << 16;

    // we suppose our max pdev number is 64(64 physical disks)
    static constexpr uint32_t pdev_atomicmap_init_size = 1 << 6;

    using PdevHeapMap = folly::AtomicHashMap< uint32_t, std::shared_ptr< std::pair< std::mutex, VChunkHeap > > >;
    using PdevAvalableBlkMap = folly::AtomicHashMap< uint32_t, std::atomic_size_t >;
    using ChunkMap = folly::AtomicHashMap< uint32_t, csharedChunk >;

private:
    // this holds all the unselected chunks for each pdev.
    PdevHeapMap m_pdev_heap_map;

    PdevAvalableBlkMap m_pdev_avalable_blk_map;

    // hold all the chunks , selected or not
    ChunkMap m_chunks;
};
} // namespace homeobject