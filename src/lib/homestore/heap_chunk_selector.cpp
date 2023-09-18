#include "heap_chunk_selector.h"

#include <execution>
#include <algorithm>
#include <sisl/logging/logging.h>

namespace homeobject {

//https://github.com/facebook/folly/blob/61c11d77eb9a8bdc60f673017fccfbe900125cb6/folly/AtomicHashMap.h#L42
//AtomichashMap has a max size limit of ~18x initial size.
//we support max of 64k chunks for now, so 64*1024/18 + 1 = 3641, we use 4000 for sufficiently initial size 
#define DEFAULT_ATOMICHASHMAP_INITIAL_SIZE 4000

HeapChunkSelector::HeapChunkSelector() : 
m_pdev_heap_map(PdevHeapMap(DEFAULT_ATOMICHASHMAP_INITIAL_SIZE)), 
m_pdev_avalable_blk_map(PdevAvalableBlkMap(DEFAULT_ATOMICHASHMAP_INITIAL_SIZE)), 
m_chunks(DEFAULT_ATOMICHASHMAP_INITIAL_SIZE) {}

HeapChunkSelector::HeapChunkSelector(const uint32_t& pdev_heap_map_initial_size, 
const uint32_t& pdev_avalable_blk_map_initial_size, const uint32_t& chunk_initial_num) : 
m_pdev_heap_map(PdevHeapMap(pdev_heap_map_initial_size)), 
m_pdev_avalable_blk_map(PdevAvalableBlkMap(pdev_avalable_blk_map_initial_size)),
m_chunks(ChunkMap(chunk_initial_num)) {}


//add_chunk may be called in Homeobject(when sealing a shard) or in Homestore(when adding chunks to vdev) concurently
void HeapChunkSelector::add_chunk(csharedChunk& chunk) {
    VChunk vchunk(chunk);
    auto&& pdevID = vchunk.get_pdev_id();
    auto&& [it, _] = m_pdev_heap_map.emplace(pdevID, std::make_shared<std::pair<std::mutex, VChunkHeap>>());
    const auto& available_blk_num = vchunk.available_blks();
    auto&& [ignore, ok] = m_pdev_avalable_blk_map.emplace(pdevID, available_blk_num);

    if(!ok) {
        auto&& avalableBlkCounter = m_pdev_avalable_blk_map.find(pdevID)->second;
        avalableBlkCounter.fetch_add(available_blk_num);
    }
    
    m_chunks.emplace(vchunk.get_chunk_id(), chunk);

    auto& heapLock = it->second->first;
    auto& heap = it->second->second;

    std::lock_guard<std::mutex> l(heapLock); 
    heap.emplace(chunk);
}

//select_chunk will only be called in homestore when allocating a block
csharedChunk HeapChunkSelector::select_chunk(homestore::blk_count_t count, const homestore::blk_alloc_hints& hint) {
    auto& chunkID = hint.chunk_id_hint;
    if(chunkID != homestore::INVALID_CHUNK_ID) {
        auto it = m_chunks.find(chunkID);
        if(it == m_chunks.end()) {
            // whether we should terminate if this happen?
            LOGERROR("No chunk found for pg");
            return nullptr;
        }
        auto& vchunk = it->second;
        return vchunk.get_internal_chunk();
    }

    auto pdevID = hint.dev_id_hint;
    if(pdevID == homestore::INVALID_DEV_ID) {
    // this is the first shard of this pg, select a pdev with the most available blocks for it
        //std::lock_guard<std::mutex> l(lock);
        auto&& it = std::max_element(m_pdev_avalable_blk_map.begin(), m_pdev_avalable_blk_map.end(),
        [](const std::pair<uint32_t, uint32_t>& lhs, const std::pair<uint32_t, uint32_t>& rhs) { 
            return lhs.second < rhs.second; }
        );
        if(it == m_pdev_avalable_blk_map.end()) {
            LOGINFO("No pdev found for pg");
            return nullptr;
        }
        pdevID = it->first;
    }

    auto it = m_pdev_heap_map.find(pdevID);
    if (it == m_pdev_heap_map.end()) {
        LOGINFO("No pdev found for pdev {}", pdevID);
        return nullptr;
    }

    const auto& vchunk = [it = std::move(it), pdevID]() {
        auto& heapLock = it->second->first;
        auto& heap = it->second->second;
        std::lock_guard<std::mutex> l(heapLock);
        if(heap.empty()) return VChunk(nullptr);
        const auto& vchunk = heap.top();
        heap.pop();
        return vchunk;
    }();

    if(vchunk.get_internal_chunk()){
        auto&& avalableBlkCounter = m_pdev_avalable_blk_map.find(pdevID)->second;
        avalableBlkCounter.fetch_sub(vchunk.available_blks());
    } else {
        LOGINFO("No pdev found for pdev {}", pdevID);
    }

    return vchunk.get_internal_chunk();
}

void HeapChunkSelector::foreach_chunks(std::function< void(csharedChunk&) >&& cb) {
    //we should call `cb` on all the chunks, selected or not
    //actually, after vdev is initialized, no more new chunks will be added again, 
    //so we can make this assumption that when this is called
    std::for_each(std::execution::par_unseq, m_chunks.begin(), m_chunks.end(), 
    [cb = std::move(cb)](const std::pair< uint32_t, VChunk >& p){ cb(p.second.get_internal_chunk()); });
}
}   // namespace homeobject