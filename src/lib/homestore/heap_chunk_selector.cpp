#include "heap_chunk_selector.h"

#include <execution>
#include <algorithm>
#include <sisl/logging/logging.h>

namespace homeobject {

// https://github.com/facebook/folly/blob/61c11d77eb9a8bdc60f673017fccfbe900125cb6/folly/AtomicHashMap.h#L42
// AtomichashMap has a max size limit of ~18x initial size.

HeapChunkSelector::HeapChunkSelector() :
        m_pdev_heap_map(PdevHeapMap(pdev_atomicmap_init_size)),
        m_pdev_avalable_blk_map(pdev_atomicmap_init_size),
        m_chunks(chunk_atomicmap_init_size) {}

HeapChunkSelector::HeapChunkSelector(const uint32_t& pdev_heap_map_initial_size,
                                     const uint32_t& pdev_avalable_blk_map_initial_size,
                                     const uint32_t& chunk_initial_num) :
        m_pdev_heap_map(PdevHeapMap(pdev_heap_map_initial_size)),
        m_pdev_avalable_blk_map(PdevAvalableBlkMap(pdev_avalable_blk_map_initial_size)),
        m_chunks(ChunkMap(chunk_initial_num)) {}

// add_chunk may be called in Homeobject(when sealing a shard) or in Homestore(when adding chunks to vdev) concurently
void HeapChunkSelector::add_chunk(csharedChunk& chunk) {
    VChunk vchunk(chunk);
    auto&& pdevID = vchunk.get_pdev_id();
    auto&& [it, _] = m_pdev_heap_map.emplace(pdevID, std::make_shared< std::pair< std::mutex, VChunkHeap > >());
    const auto& available_blk_num = vchunk.available_blks();
    auto&& [ignore, happened] = m_pdev_avalable_blk_map.emplace(pdevID, available_blk_num);

    if (!happened) {
        auto& avalableBlkCounter = m_pdev_avalable_blk_map.find(pdevID)->second;
        avalableBlkCounter.fetch_add(available_blk_num);
    }

    m_chunks.emplace(vchunk.get_chunk_id(), chunk);

    auto& heapLock = it->second->first;
    auto& heap = it->second->second;

    std::lock_guard< std::mutex > l(heapLock);
    heap.emplace(chunk);
}

// select_chunk will only be called in homestore when creating a shard.
csharedChunk HeapChunkSelector::select_chunk(homestore::blk_count_t count, const homestore::blk_alloc_hints& hint) {
    auto& chunkIdHint = hint.chunk_id_hint;
    if (chunkIdHint.has_value()) {
        LOGINFO("should not allocated a chunk with exiting chunk_id {} in hint!", chunkIdHint.value());
        return nullptr;
    }

    // shardid -> chunkid map is maintained by ShardManager
    // pg_id->pdev_id map is maintained by PgManager
    // chunselector will not take care of the two maps for now.
    uint32_t pdevID = 0;
    auto& pdevIdHint = hint.pdev_id_hint;
    if (!pdevIdHint.has_value()) {
        // this is the first shard of this pg, select a pdev with the most available blocks for it
        auto&& it =
            std::max_element(m_pdev_avalable_blk_map.begin(), m_pdev_avalable_blk_map.end(),
                             [](const std::pair< uint32_t, uint32_t >& lhs,
                                const std::pair< uint32_t, uint32_t >& rhs) { return lhs.second < rhs.second; });
        if (it == m_pdev_avalable_blk_map.end()) {
            LOGINFO("No pdev found for new pg");
            return nullptr;
        }
        pdevID = it->first;
    } else {
        pdevID = pdevIdHint.value();
    }

    auto it = m_pdev_heap_map.find(pdevID);
    if (it == m_pdev_heap_map.end()) {
        LOGINFO("No pdev found for pdev {}", pdevID);
        return nullptr;
    }

    const auto& vchunk = [it = std::move(it), pdevID]() {
        auto& heapLock = it->second->first;
        auto& heap = it->second->second;
        std::lock_guard< std::mutex > l(heapLock);
        if (heap.empty()) return VChunk(nullptr);
        VChunk vchunk(heap.top().get_internal_chunk());
        heap.pop();
        return vchunk;
    }();

    if (vchunk.get_internal_chunk()) {
        auto&& avalableBlkCounter = m_pdev_avalable_blk_map.find(pdevID)->second;
        avalableBlkCounter.fetch_sub(vchunk.available_blks());
    } else {
        LOGINFO("No pdev found for pdev {}", pdevID);
    }

    return vchunk.get_internal_chunk();
}

void HeapChunkSelector::foreach_chunks(std::function< void(csharedChunk&) >&& cb) {
    // we should call `cb` on all the chunks, selected or not
    std::for_each(std::execution::par_unseq, m_chunks.begin(), m_chunks.end(),
                  [cb = std::move(cb)](auto& p) { cb(p.second); });
}

void HeapChunkSelector::release_chunk(const uint32_t chunkID) {
    const auto& it = m_chunks.find(chunkID);
    if (it == m_chunks.end()) {
        LOGWARN("No chunk found for ChunkID {}", chunkID);
    } else {
        add_chunk(it->second);
    }
}
} // namespace homeobject