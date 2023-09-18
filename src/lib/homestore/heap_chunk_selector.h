#pragma once

#include <homestore/chunk_selector.h>
#include <homestore/vchunk.h>
#include <homestore/homestore_decl.hpp>

#include <queue>
#include <vector>
#include <mutex>
#include <utility>
#include <memory>
#include <map>
#include <functional>
#include <atomic>

#include <folly/AtomicHashMap.h>

using VChunk = homestore::VChunk;

template <>
struct std::hash< VChunk > {
    std::size_t operator()(VChunk const& v) const noexcept { return std::hash< uint16_t >()(v.get_chunk_id()); }
};

namespace homeobject {

using csharedChunk = homestore::cshared<homestore::Chunk>;  

class HeapChunkSelector : public homestore::ChunkSelector {
public:
    HeapChunkSelector();
    HeapChunkSelector(const uint32_t&, const uint32_t&, const uint32_t&);
    ~HeapChunkSelector() = default;

    class VChunkComparator {
    public:
        bool operator()(VChunk& lhs, VChunk& rhs) {
            return lhs.available_blks() < rhs.available_blks();
        }
    };

    using VChunkHeap = std::priority_queue<VChunk, std::vector<VChunk>, VChunkComparator>;

    void add_chunk(csharedChunk&) override;
    void foreach_chunks(std::function< void(csharedChunk&) >&& cb) override;
    csharedChunk select_chunk([[maybe_unused]]homestore::blk_count_t nblks, const homestore::blk_alloc_hints& hints);

    using PdevHeapMap = folly::AtomicHashMap< uint32_t, std::shared_ptr< std::pair< std::mutex, VChunkHeap > > >;
    using PdevAvalableBlkMap = folly::AtomicHashMap< uint32_t, std::atomic_uint32_t >;
    using ChunkMap = folly::AtomicHashMap< uint32_t, VChunk >;

private:
    //this holds all the chunks for each pdev, they are all unselected.
    PdevHeapMap m_pdev_heap_map;

    //for now, uint32_t is enough for the sum of all the available blocks of a pdev.
    //if necessary , we can change this to uint64_t to hold a larger sum.
    PdevAvalableBlkMap m_pdev_avalable_blk_map;

    //hold all the chunks , selected or not
    ChunkMap m_chunks;
};
} // namespace homeobject