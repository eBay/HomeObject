#include "heap_chunk_selector.h"

namespace homeobject {

void HeapChunkSelector::add_chunk(csharedChunk& chunk) {
    uint32_t pdevId = VChunk(chunk).get_pdev_id();
    auto it = _pdev_heap_map.find(pdevId);
    if (it == _pdev_heap_map.end()) it = _pdev_heap_map.try_emplace(pdevId, std::make_shared<std::pair<std::mutex, VChunkHeap>>()).first;
    std::unique_lock<std::mutex> l(it->second->first);
    it->second->second.emplace(chunk);
}

csharedChunk HeapChunkSelector::select_chunk(homestore::blk_count_t count, const homestore::blk_alloc_hints& hint) {
    auto it = _pdev_heap_map.find(hint.dev_id_hint);
    if (it == _pdev_heap_map.end()) return nullptr;
    std::unique_lock<std::mutex> l(it->second->first);
    if(it->second->second.empty()) return nullptr;
    auto& vchunk = it->second->second.top();
    it->second->second.pop();
    return vchunk.get_internal_chunk();
}

void HeapChunkSelector::foreach_chunks(std::function< void(csharedChunk&) >&& cb) {
    VChunkHeap tempHeap;
    for(auto& p : _pdev_heap_map) {
        std::unique_lock<std::mutex> l(p.second->first);
        auto& heap = p.second->second;
        while(!heap.empty()) {
            auto& vchunk = heap.top();
            heap.pop();
            cb(vchunk.get_internal_chunk());
            tempHeap.emplace(std::move(vchunk));
        }        
        heap.swap(tempHeap);
    }
}

}   // namespace homeobject