#include "heap_chunk_selector.h"

#include <execution>
#include <algorithm>
#include <sisl/logging/logging.h>

namespace homeobject {

//add_chunk may be called in Homeobject(when sealing a shard) or in Homestore(when adding chunks to vdev) concurently
void HeapChunkSelector::add_chunk(csharedChunk& chunk) {
    VChunk vchunk(chunk);
    auto&& pdevId = vchunk.get_pdev_id();
    auto&& [it, ok] = m_pdev_heap_map.try_emplace(pdevId, std::make_shared<std::pair<std::mutex, VChunkHeap>>());
    
    {
        std::lock_guard<std::mutex> l(lock); 
        if(ok) m_pdev_avalable_blk_map.emplace(pdevId, vchunk.available_blks());
        else m_pdev_avalable_blk_map[pdevId] += vchunk.available_blks();
        m_chunks.emplace(chunk);
    }

    std::lock_guard<std::mutex> l(it->second->first); 
    it->second->second.emplace(chunk);
}

//select_chunk will only be called in homestore when allocating a block
csharedChunk HeapChunkSelector::select_chunk(homestore::blk_count_t count, const homestore::blk_alloc_hints& hint) {
    auto pdevID = hint.dev_id_hint;
    if(pdevID == homestore::INVALID_DEV_ID) {
    // this is the first shard of this pg, select a pdev with the most available blocks for it
        std::lock_guard<std::mutex> l(lock);
        auto it = std::max_element(m_pdev_avalable_blk_map.begin(), m_pdev_avalable_blk_map.end(),
        [](const std::pair<uint32_t, uint32_t>& lhs, const std::pair<uint32_t, uint32_t>& rhs) { 
            return lhs.second < rhs.second; }
        );
        pdevID = it->first;
    }

    auto it = m_pdev_heap_map.find(pdevID);
    if (it == m_pdev_heap_map.end()) {
        LOGINFO("No pdev found for pdev {}", pdevID);
        return nullptr;
    }

    
    it->second->first.lock();
    if(it->second->second.empty()) {
        LOGINFO("No more available chunks found for pdev {}", pdevID);
        return nullptr;
    }
    auto& vchunk = it->second->second.top();
    it->second->second.pop();
    it->second->first.unlock();
    
    {
        std::lock_guard<std::mutex> l(lock);
        m_pdev_avalable_blk_map[pdevID] -= vchunk.available_blks();
    }

    return vchunk.get_internal_chunk();
}

void HeapChunkSelector::foreach_chunks(std::function< void(csharedChunk&) >&& cb) {
    //we should call `cb` on all the chunks, selected or not

    //actually, after vdev is initialized, no more new chunks will be added again, 
    //so we can make this assumption and disable this lock.

    //if we support dynamiclly add new chunks to vdev in the future, we need to enable this lock
    //std::lock_guard<std::mutex> l(lock); 
    std::for_each(std::execution::par_unseq, m_chunks.begin(), m_chunks.end(), 
    [cb = std::move(cb)](const VChunk& p){ cb(p.get_internal_chunk()); });
}
}   // namespace homeobject