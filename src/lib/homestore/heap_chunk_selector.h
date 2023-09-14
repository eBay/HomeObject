#pragma once

#include <homestore/chunk_selector.h>
#include <homestore/vchunk.h>
#include <homestore/homestore_decl.hpp>

#include <queue>
#include <vector>
#include <mutex>
#include <utility>
#include <memory>

#include <folly/concurrency/ConcurrentHashMap.h>

namespace homeobject {

using csharedChunk = homestore::cshared<homestore::Chunk>;  

class HeapChunkSelector : public homestore::ChunkSelector {
public:
    HeapChunkSelector() = default;
    HeapChunkSelector(const HeapChunkSelector&) = delete;
    HeapChunkSelector(HeapChunkSelector&&) noexcept = delete;
    HeapChunkSelector& operator=(const HeapChunkSelector&) = delete;
    HeapChunkSelector& operator=(HeapChunkSelector&&) noexcept = delete;
    ~HeapChunkSelector() = default;

    using VChunk = homestore::VChunk;

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

private:
    folly::ConcurrentHashMap< uint32_t, std::shared_ptr<std::pair< std::mutex, VChunkHeap > > >_pdev_heap_map;
};
} // namespace homeobject