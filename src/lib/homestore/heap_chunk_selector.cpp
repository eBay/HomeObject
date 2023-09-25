#include "heap_chunk_selector.h"

#include <execution>
#include <algorithm>
#include <utility>

#include <sisl/logging/logging.h>

namespace homeobject {
// https://github.com/eBay/HomeObject/pull/30#discussion_r1331112743
// we make the following assumptions
// 1 homestore will initialize HeapChunkSelector by adding all the chunks single threaded
// 2 we do not need dynamic chunk requirements

// it means after the single thread initialization,
// 1 the key collection of m_per_dev_heap will never change.
// 2 the key collection of m_chunks will never change

// this should only be called when initializing HeapChunkSelector in Homestore
void HeapChunkSelector::add_chunk(csharedChunk& chunk) { m_chunks.emplace(VChunk(chunk).get_chunk_id(), chunk); }

void HeapChunkSelector::add_chunk_internal(const chunk_num_t chunkID) {
    if (m_chunks.find(chunkID) == m_chunks.end()) {
        // sanity check
        LOGWARN("No chunk found for ChunkID {}", chunkID);
        return;
    }
    const auto& chunk = m_chunks[chunkID];
    VChunk vchunk(chunk);
    auto pdevID = vchunk.get_pdev_id();
    // add this find here, since we don`t want to call make_shared in try_emplace every time.
    auto it = m_per_dev_heap.find(pdevID);
    if (it == m_per_dev_heap.end()) it = m_per_dev_heap.emplace(pdevID, std::make_shared< PerDevHeap >()).first;
    auto& avalableBlkCounter = it->second->available_blk_count;
    avalableBlkCounter.fetch_add(vchunk.available_blks());

    auto& heapLock = it->second->mtx;
    auto& heap = it->second->m_heap;
    std::lock_guard< std::mutex > l(heapLock);
    heap.emplace(chunk);
}

// select_chunk will only be called in homestore when creating a shard.
csharedChunk HeapChunkSelector::select_chunk(homestore::blk_count_t count, const homestore::blk_alloc_hints& hint) {
    auto& chunkIdHint = hint.chunk_id_hint;
    if (chunkIdHint.has_value()) {
        LOGWARN("should not allocated a chunk with exiting chunk_id {} in hint!", chunkIdHint.value());
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
            std::max_element(m_per_dev_heap.begin(), m_per_dev_heap.end(),
                             [](const std::pair< const uint32_t, std::shared_ptr< PerDevHeap > >& lhs,
                                const std::pair< const uint32_t, std::shared_ptr< PerDevHeap > >& rhs) {
                                 return lhs.second->available_blk_count.load() < rhs.second->available_blk_count.load();
                             });
        if (it == m_per_dev_heap.end()) {
            LOGWARN("No pdev found for new pg");
            return nullptr;
        }
        pdevID = it->first;
    } else {
        pdevID = pdevIdHint.value();
    }

    auto it = m_per_dev_heap.find(pdevID);
    if (it == m_per_dev_heap.end()) {
        LOGWARN("No pdev found for pdev {}", pdevID);
        return nullptr;
    }

    const auto& vchunk = [it = std::move(it)]() {
        auto& heapLock = it->second->mtx;
        auto& heap = it->second->m_heap;
        std::lock_guard< std::mutex > l(heapLock);
        if (heap.empty()) return VChunk(nullptr);
        VChunk vchunk = heap.top();
        heap.pop();
        return vchunk;
    }();

    if (vchunk.get_internal_chunk()) {
        auto& avalableBlkCounter = it->second->available_blk_count;
        avalableBlkCounter.fetch_sub(vchunk.available_blks());
    } else {
        LOGWARN("No pdev found for pdev {}", pdevID);
    }

    return vchunk.get_internal_chunk();
}

void HeapChunkSelector::foreach_chunks(std::function< void(csharedChunk&) >&& cb) {
    // we should call `cb` on all the chunks, selected or not
    std::for_each(std::execution::par_unseq, m_chunks.begin(), m_chunks.end(),
                  [cb = std::move(cb)](auto& p) { cb(p.second); });
}

void HeapChunkSelector::release_chunk(const chunk_num_t chunkID) {
    const auto& it = m_chunks.find(chunkID);
    if (it == m_chunks.end()) {
        // sanity check
        LOGWARN("No chunk found for ChunkID {}", chunkID);
    } else {
        add_chunk_internal(chunkID);
    }
}

void HeapChunkSelector::build_per_dev_chunk_heap(const std::unordered_set< chunk_num_t >& excludingChunks) {
    for (const auto& p : m_chunks) {
        if (excludingChunks.find(p.first) == excludingChunks.end()) { add_chunk_internal(p.first); }
    };
}
} // namespace homeobject
