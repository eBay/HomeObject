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

void HeapChunkSelector::add_chunk_internal(const chunk_num_t chunkID, bool add_to_heap) {
    if (m_chunks.find(chunkID) == m_chunks.end()) {
        // sanity check
        LOGWARNMOD(homeobject, "No chunk found for ChunkID {}", chunkID);
        return;
    }

    const auto& chunk = m_chunks[chunkID];
    VChunk vchunk(chunk);
    auto pdevID = vchunk.get_pdev_id();
    // add this find here, since we don`t want to call make_shared in try_emplace every time.
    auto it = m_per_dev_heap.find(pdevID);
    if (it == m_per_dev_heap.end()) { it = m_per_dev_heap.emplace(pdevID, std::make_shared< ChunkHeap >()).first; }

    // build total blks for every chunk on this device;
    it->second->m_total_blks += vchunk.get_total_blks();

    if (add_to_heap) {
        auto& avalableBlkCounter = it->second->available_blk_count;
        avalableBlkCounter.fetch_add(vchunk.available_blks());

        auto& heapLock = it->second->mtx;
        auto& heap = it->second->m_heap;
        {
            std::lock_guard< std::mutex > l(m_defrag_mtx);
            m_defrag_heap.emplace(chunk);
        }
        std::lock_guard< std::mutex > l(heapLock);
        heap.emplace(chunk);
    }
}

// select_chunk will only be called in homestore when creating a shard.
csharedChunk HeapChunkSelector::select_chunk(homestore::blk_count_t count, const homestore::blk_alloc_hints& hint) {
    auto& chunkIdHint = hint.chunk_id_hint;
    if (chunkIdHint.has_value()) {
        LOGWARNMOD(homeobject, "should not allocated a chunk with exiting chunk_id {} in hint!", chunkIdHint.value());
        return nullptr;
    }

    std::shared_lock lock_guard(m_chunk_selector_mtx);
    // FIXME @Hooper: Temporary bypass using pdev_id_hint to represent pg_id_hint, "identical layout" will change it
    pg_id_t pg_id = 0;
    auto& pg_id_hint = hint.pdev_id_hint;
    if (!pg_id_hint.has_value()) {
        LOGWARNMOD(homeobject, "should not allocated a chunk without exiting pg_id in hint!");
        return nullptr;
    } else {
        pg_id = pg_id_hint.value();
    }

    auto it = m_per_pg_heap.find(pg_id);
    if (it == m_per_pg_heap.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg_id {}", pg_id);
        return nullptr;
    }

    auto vchunk = VChunk(nullptr);
    auto& heap = it->second->m_heap;
    if (auto lock_guard = std::lock_guard< std::mutex >(it->second->mtx); !heap.empty()) {
        vchunk = heap.top();
        heap.pop();
    }

    if (vchunk.get_internal_chunk()) {
        auto& avalableBlkCounter = it->second->available_blk_count;
        avalableBlkCounter.fetch_sub(vchunk.available_blks());
        remove_chunk_from_defrag_heap(vchunk.get_chunk_id());
    } else {
        LOGWARNMOD(homeobject, "no available chunks left for pg {}", pg_id);
    }

    return vchunk.get_internal_chunk();
}

csharedChunk HeapChunkSelector::select_specific_chunk(const pg_id_t pg_id, const chunk_num_t chunkID) {
    if (m_chunks.find(chunkID) == m_chunks.end()) {
        // sanity check
        LOGWARNMOD(homeobject, "No chunk found for ChunkID {}", chunkID);
        return nullptr;
    }

    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_heap.find(pg_id);
    if (pg_it == m_per_pg_heap.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg_id {}", pg_id);
        return nullptr;
    }

    VChunk vchunk(nullptr);
    auto& heap = pg_it->second->m_heap;
    if (auto lock_guard = std::lock_guard< std::mutex >(pg_it->second->mtx); !heap.empty()) {
        std::vector< VChunk > chunks;
        chunks.reserve(heap.size());
        while (!heap.empty()) {
            auto c = heap.top();
            heap.pop();
            if (c.get_chunk_id() == chunkID) {
                vchunk = c;
                break;
            }
            chunks.push_back(std::move(c));
        }
        for (auto& c : chunks) {
            heap.emplace(c);
        }
    }

    if (vchunk.get_internal_chunk()) {
        auto& avalableBlkCounter = pg_it->second->available_blk_count;
        avalableBlkCounter.fetch_sub(vchunk.available_blks());
        remove_chunk_from_defrag_heap(vchunk.get_chunk_id());
    }

    return vchunk.get_internal_chunk();
}

// Temporarily commented out, the subsequent GC implementation needs to be adapted to fix pg size
// most_defrag_chunk will only be called when GC is triggered, and will return the chunk with the most
// defrag blocks
csharedChunk HeapChunkSelector::most_defrag_chunk() {
    // chunk_num_t chunkID{0};
    // the chunk might be seleted for creating shard. if this happens, we need to select another chunk
    // for (;;) {
    //     {
    //         std::lock_guard< std::mutex > lg(m_defrag_mtx);
    //         if (m_defrag_heap.empty()) break;
    //         chunkID = m_defrag_heap.top().get_chunk_id();
    //     }
    //     auto chunk = select_specific_chunk(chunkID);
    //     if (chunk) return chunk;
    // }
    return nullptr;
}

void HeapChunkSelector::remove_chunk_from_defrag_heap(const chunk_num_t chunkID) {
    std::vector< VChunk > chunks;
    std::lock_guard< std::mutex > lg(m_defrag_mtx);
    chunks.reserve(m_defrag_heap.size());
    while (!m_defrag_heap.empty()) {
        auto c = m_defrag_heap.top();
        m_defrag_heap.pop();
        if (c.get_chunk_id() == chunkID) break;
        chunks.emplace_back(std::move(c));
    }
    for (auto& c : chunks) {
        m_defrag_heap.emplace(c);
    }
}

void HeapChunkSelector::foreach_chunks(std::function< void(csharedChunk&) >&& cb) {
    // we should call `cb` on all the chunks, selected or not
    std::for_each(std::execution::par_unseq, m_chunks.begin(), m_chunks.end(),
                  [cb = std::move(cb)](auto& p) { cb(p.second); });
}

void HeapChunkSelector::release_chunk(const pg_id_t pg_id, const chunk_num_t chunkID) {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    if (m_chunks.find(chunkID) == m_chunks.end()) {
        // sanity check
        LOGWARNMOD(homeobject, "No chunk found for ChunkID {}", chunkID);
        return;
    }

    auto pg_it = m_per_pg_heap.find(pg_id);
    if (pg_it == m_per_pg_heap.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg_id {}", pg_id);
        return;
    }

    const auto& chunk = m_chunks[chunkID];
    VChunk vchunk(chunk);
    {
        std::lock_guard< std::mutex > l(pg_it->second->mtx);
        auto& pg_heap = pg_it->second->m_heap;
        pg_heap.emplace(chunk);
    }
    auto& avalableBlkCounter = pg_it->second->available_blk_count;
    avalableBlkCounter += vchunk.available_blks();

}

uint32_t HeapChunkSelector::get_chunk_size() const {
    const auto& chunk = m_chunks.begin()->second;
    auto vchunk = VChunk(chunk);
    return vchunk.size();
}

std::optional< uint32_t > HeapChunkSelector::select_chunks_for_pg(pg_id_t pg_id, u_int64_t pg_size) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    if (m_per_pg_heap.find(pg_id) != m_per_pg_heap.end()) {
        LOGWARNMOD(homeobject, "PG had already created, pg_id {}", pg_id);
        return std::nullopt;
    }

    const auto chunk_size = get_chunk_size();
    const uint32_t num_chunk = sisl::round_down(pg_size, chunk_size) / chunk_size;

    //Select a pdev with the most available num chunk
    auto &&most_avail_dev_it =
        std::max_element(m_per_dev_heap.begin(), m_per_dev_heap.end(),
                            [](const std::pair< const uint32_t, std::shared_ptr< ChunkHeap > >& lhs,
                            const std::pair< const uint32_t, std::shared_ptr< ChunkHeap > >& rhs) {
                                return lhs.second->size() < rhs.second->size();
                            });
    auto& pdev_heap = most_avail_dev_it->second;
    if (num_chunk > pdev_heap->size()) {
        LOGWARNMOD(homeobject, "Pdev has no enough space to create pg {} with num_chunk {}", pg_id, num_chunk);
        return std::nullopt;
    }
    auto vchunk = VChunk(nullptr);
    auto it = m_per_pg_heap.emplace(pg_id, std::make_shared< ChunkHeap >()).first;
    auto v2r_vector = m_v2r_chunk_map.emplace(pg_id, std::make_shared< std::vector < chunk_num_t > >()).first->second;
    auto r2v_map = m_r2v_chunk_map.emplace(pg_id, std::make_shared< ChunkIdMap >()).first->second;

    auto& pg_heap = it->second;
    std::scoped_lock lock(pdev_heap->mtx, pg_heap->mtx);
    v2r_vector->reserve(num_chunk);
    for (chunk_num_t i = 0; i < num_chunk; ++i) {
        vchunk = pdev_heap->m_heap.top();
        //sanity check
        RELEASE_ASSERT(vchunk.get_total_blks() == vchunk.available_blks(), "vchunk should be empty");
        pdev_heap->m_heap.pop();
        pdev_heap->available_blk_count -= vchunk.available_blks();

        pg_heap->m_heap.emplace(vchunk);
        pg_heap->m_total_blks += vchunk.get_total_blks();
        pg_heap->available_blk_count += vchunk.available_blks();
        // v_chunk_id start from 0.
        chunk_num_t v_chunk_id = i;
        chunk_num_t r_chunk_id = vchunk.get_chunk_id();
        v2r_vector->emplace_back(r_chunk_id);
        r2v_map->emplace(r_chunk_id, v_chunk_id);
    }

    return num_chunk;
}

void HeapChunkSelector::set_pg_chunks(pg_id_t pg_id, std::vector<chunk_num_t>&& chunk_ids) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    if (m_v2r_chunk_map.find(pg_id) != m_v2r_chunk_map.end()) {
        LOGWARNMOD(homeobject, "PG {} had been recovered", pg_id);
        return;
    }

    auto v2r_vector = m_v2r_chunk_map.emplace(pg_id, std::make_shared< std::vector < chunk_num_t > >(std::move(chunk_ids))).first->second;
    auto r2v_map = m_r2v_chunk_map.emplace(pg_id, std::make_shared< ChunkIdMap >()).first->second;

    for (chunk_num_t i = 0; i < v2r_vector->size(); ++i) {
        // v_chunk_id start from 0.
        chunk_num_t v_chunk_id = i;
        chunk_num_t r_chunk_id = (*v2r_vector)[i];
        r2v_map->emplace(r_chunk_id, v_chunk_id);
    }
}

void HeapChunkSelector::recover_per_dev_chunk_heap() {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    for (const auto& [chunk_id, _] : m_chunks) {
        bool add_to_heap = true;
        for (const auto& [_, chunk_map] : m_r2v_chunk_map) {
            if (chunk_map->find(chunk_id) != chunk_map->end()) {
                add_to_heap = false;
                break;
            }
        }
        add_chunk_internal(chunk_id, add_to_heap);

    }
}

void HeapChunkSelector::recover_pg_chunk_heap(pg_id_t pg_id, const std::unordered_set< chunk_num_t >& excludingChunks)
{
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    if (m_per_pg_heap.find(pg_id) != m_per_pg_heap.end()) {
        LOGWARNMOD(homeobject, "Pg_heap {} had been recovered", pg_id);
        return;
    }
    auto it = m_v2r_chunk_map.find(pg_id);
    if (it == m_v2r_chunk_map.end()) {
        LOGWARNMOD(homeobject, "Pg_chunk_map {} had never been recovered", pg_id);
        return;
    }
    const auto& chunk_ids = it->second;
    auto& pg_heap = m_per_pg_heap.emplace(pg_id, std::make_shared< ChunkHeap >()).first->second;
    for (const auto& chunk_id : *chunk_ids) {
        if (excludingChunks.find(chunk_id) == excludingChunks.end()) {
            const auto& chunk = m_chunks[chunk_id];
            auto vchunk = VChunk(chunk);
            pg_heap->m_heap.emplace(vchunk);
            pg_heap->m_total_blks += vchunk.get_total_blks();
            pg_heap->available_blk_count += vchunk.available_blks();
        }
    }
}

std::shared_ptr< const std::vector <homestore::chunk_num_t> > HeapChunkSelector::get_pg_chunks(pg_id_t pg_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto it = m_v2r_chunk_map.find(pg_id);
    if (it != m_v2r_chunk_map.end()) {
        return it->second;
    } else {
        LOGWARNMOD(homeobject, "PG {} had never been created", pg_id);
        return nullptr;
    }
}

homestore::blk_alloc_hints HeapChunkSelector::chunk_to_hints(chunk_num_t chunk_id) const {
    auto iter = m_chunks.find(chunk_id);
    if (iter == m_chunks.end()) {
        LOGWARNMOD(homeobject, "No chunk found for chunk_id {}, will return default blk alloc hints", chunk_id);
        return homestore::blk_alloc_hints();
    }
    homestore::blk_alloc_hints hints;
    hints.pdev_id_hint = VChunk(iter->second).get_pdev_id();
    return hints;
}

// return the maximum number of chunks that can be allocated on pdev
uint32_t HeapChunkSelector::most_avail_num_chunks() const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    uint32_t max_avail_num_chunks = 0ul;
    for (auto const& [_, pdev_heap] : m_per_dev_heap) {
        max_avail_num_chunks = std::max(max_avail_num_chunks, pdev_heap->size());
    }

    return max_avail_num_chunks;
}

uint32_t HeapChunkSelector::avail_num_chunks(uint32_t dev_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto it = m_per_dev_heap.find(dev_id);
    if (it == m_per_dev_heap.end()) {
        LOGWARNMOD(homeobject, "No pdev found for pdev {}", dev_id);
        return 0;
    }

    return it->second->size();
}

uint32_t HeapChunkSelector::total_chunks() const { return m_chunks.size(); }

uint64_t HeapChunkSelector::avail_blks(std::optional< uint32_t > dev_it) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    if (!dev_it.has_value()) {
        uint64_t max_avail_blks = 0ull;
        for (auto const& [_, heap] : m_per_dev_heap) {
            std::scoped_lock lock(heap->mtx);
            max_avail_blks = std::max(max_avail_blks, static_cast< uint64_t >(heap->available_blk_count.load()));
        }
        return max_avail_blks;
    } else {
        auto it = m_per_dev_heap.find(dev_it.value());
        std::scoped_lock lock(it->second->mtx);
        if (it == m_per_dev_heap.end()) {
            LOGWARNMOD(homeobject, "No pdev found for pdev {}", dev_it.value());
            return 0;
        }
        return it->second->available_blk_count.load();
    }
}

uint64_t HeapChunkSelector::total_blks(uint32_t dev_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto it = m_per_dev_heap.find(dev_id);
    if (it == m_per_dev_heap.end()) {
        LOGWARNMOD(homeobject, "No pdev found for pdev {}", dev_id);
        return 0;
    }

    return it->second->m_total_blks;
}

} // namespace homeobject
