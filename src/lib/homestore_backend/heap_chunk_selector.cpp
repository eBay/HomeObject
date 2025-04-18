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
void HeapChunkSelector::add_chunk(csharedChunk& chunk) {
    m_chunks.emplace(VChunk(chunk).get_chunk_id(), std::make_shared< ExtendedVChunk >(chunk));
}

void HeapChunkSelector::add_chunk_internal(const chunk_num_t p_chunk_id, bool add_to_heap) {
    // private function p_chunk_id must belong to m_chunks

    auto chunk = m_chunks[p_chunk_id];
    auto pdevID = chunk->get_pdev_id();
    // add this find here, since we don`t want to call make_shared in try_emplace every time.
    auto it = m_per_dev_heap.find(pdevID);
    if (it == m_per_dev_heap.end()) { it = m_per_dev_heap.emplace(pdevID, std::make_shared< ChunkHeap >()).first; }

    // build total blks for every chunk on this device;
    it->second->m_total_blks += chunk->get_total_blks();

    if (add_to_heap) {
        std::lock_guard< std::mutex > l(it->second->mtx);
        auto& heap = it->second->m_heap;
        heap.emplace(chunk);
        it->second->available_blk_count += chunk->available_blks();
    }
}

// select_chunk will only be called in homestore when creating a shard.
csharedChunk HeapChunkSelector::select_chunk(homestore::blk_count_t count, const homestore::blk_alloc_hints& hint) {
    auto& chunkIdHint = hint.chunk_id_hint;
    if (chunkIdHint.has_value()) {
        LOGWARNMOD(homeobject, "should not allocated a chunk with exiting chunkIdHint={} in hint!",
                   chunkIdHint.value());
        return nullptr;
    }

    if (!hint.application_hint.has_value()) {
        LOGWARNMOD(homeobject, "should not allocated a chunk without exiting application_hint in hint!");
        return nullptr;
    } else {
        // Both chunk_num_t and pg_id_t are of type uint16_t.
        static_assert(std::is_same< pg_id_t, uint16_t >::value, "pg_id_t is not uint16_t");
        static_assert(std::is_same< homestore::chunk_num_t, uint16_t >::value, "chunk_num_t is not uint16_t");
        auto application_hint = hint.application_hint.value();
        pg_id_t pg_id = (uint16_t)(application_hint >> 16 & 0xFFFF);
        homestore::chunk_num_t v_chunk_id = (uint16_t)(application_hint & 0xFFFF);
        return select_specific_chunk(pg_id, v_chunk_id);
    }
}

csharedChunk HeapChunkSelector::select_specific_chunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id) {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return nullptr;
    }

    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    std::scoped_lock lock(pg_chunk_collection->mtx);
    if (v_chunk_id >= pg_chunks.size()) {
        LOGWARNMOD(homeobject, "No chunk found for v_chunk_id={}", v_chunk_id);
        return nullptr;
    }
    auto chunk = pg_chunks[v_chunk_id];
    if (chunk->m_state == ChunkState::AVAILABLE) {
        chunk->m_state = ChunkState::INUSE;
        --pg_chunk_collection->available_num_chunks;
        pg_chunk_collection->available_blk_count -= chunk->available_blks();
    }

    return chunk->get_internal_chunk();
}

void HeapChunkSelector::foreach_chunks(std::function< void(csharedChunk&) >&& cb) {
    // we should call `cb` on all the chunks, selected or not
    std::for_each(std::execution::par_unseq, m_chunks.begin(), m_chunks.end(),
                  [cb = std::move(cb)](auto& p) { cb(p.second->get_internal_chunk()); });
}

bool HeapChunkSelector::release_chunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id) {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return false;
    }

    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    if (v_chunk_id >= pg_chunks.size()) {
        LOGWARNMOD(homeobject, "No chunk found for v_chunk_id={}", v_chunk_id);
        return false;
    }
    std::scoped_lock lock(pg_chunk_collection->mtx);
    auto chunk = pg_chunks[v_chunk_id];
    if (chunk->m_state == ChunkState::INUSE) {
        chunk->m_state = ChunkState::AVAILABLE;
        ++pg_chunk_collection->available_num_chunks;
        pg_chunk_collection->available_blk_count += chunk->available_blks();
    }
    return true;
}

bool HeapChunkSelector::reset_pg_chunks(pg_id_t pg_id) {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return false;
    }
    {
        auto pg_chunk_collection = pg_it->second;
        std::scoped_lock lock(pg_chunk_collection->mtx);
        for (auto& chunk : pg_chunk_collection->m_pg_chunks) {
            chunk->reset();
        }
    }
    return true;
}

bool HeapChunkSelector::return_pg_chunks_to_dev_heap(const pg_id_t pg_id) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return false;
    }

    auto pg_chunk_collection = pg_it->second;
    auto pdev_id = pg_chunk_collection->m_pg_chunks[0]->get_pdev_id();
    auto pdev_it = m_per_dev_heap.find(pdev_id);
    RELEASE_ASSERT(pdev_it != m_per_dev_heap.end(), "pdev_id={} should in per dev heap", pdev_id);
    auto pdev_heap = pdev_it->second;

    {
        std::scoped_lock lock(pdev_heap->mtx, pg_chunk_collection->mtx);
        for (auto& chunk : pg_chunk_collection->m_pg_chunks) {
            if (chunk->m_state == ChunkState::INUSE) {
                chunk->m_state = ChunkState::AVAILABLE;
            } // with shard which should be first
            chunk->m_pg_id = std::nullopt;
            chunk->m_v_chunk_id = std::nullopt;

            pdev_heap->m_heap.emplace(chunk);
            pdev_heap->available_blk_count += chunk->available_blks();
        }
    }
    m_per_pg_chunks.erase(pg_it);
    return true;
}

uint32_t HeapChunkSelector::get_chunk_size() const {
    const auto chunk = m_chunks.begin()->second;
    return chunk->size();
}

bool HeapChunkSelector::is_chunk_available(const pg_id_t pg_id, const chunk_num_t v_chunk_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return false;
    }

    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    if (v_chunk_id >= pg_chunks.size()) {
        LOGWARNMOD(homeobject, "No chunk found for v_chunk_id={}", v_chunk_id);
        return false;
    }
    std::scoped_lock lock(pg_chunk_collection->mtx);
    auto chunk = pg_chunks[v_chunk_id];
    return chunk->available();
}

std::optional< uint32_t > HeapChunkSelector::select_chunks_for_pg(pg_id_t pg_id, uint64_t pg_size) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    const auto chunk_size = get_chunk_size();
    if (pg_size < chunk_size) {
        LOGWARNMOD(homeobject, "pg_size={} is less than chunk_size={}", pg_size, chunk_size);
        return std::nullopt;
    }
    const uint32_t num_chunk = sisl::round_down(pg_size, chunk_size) / chunk_size;

    if (m_per_pg_chunks.find(pg_id) != m_per_pg_chunks.end()) {
        // leader may call select_chunks_for_pg multiple times
        RELEASE_ASSERT(num_chunk == m_per_pg_chunks[pg_id]->m_pg_chunks.size(), "num_chunk should be same");
        LOGWARNMOD(homeobject, "PG had already created, pg={}", pg_id);
        return num_chunk;
    }

    // Select a pdev with the most available num chunk
    auto most_avail_dev_it = std::max_element(m_per_dev_heap.begin(), m_per_dev_heap.end(),
                                              [](const std::pair< const uint32_t, std::shared_ptr< ChunkHeap > >& lhs,
                                                 const std::pair< const uint32_t, std::shared_ptr< ChunkHeap > >& rhs) {
                                                  return lhs.second->size() < rhs.second->size();
                                              });
    auto& pdev_heap = most_avail_dev_it->second;
    if (num_chunk > pdev_heap->size()) {
        LOGWARNMOD(homeobject, "Pdev has no enough space to create pg={} with num_chunk={}", pg_id, num_chunk);
        return std::nullopt;
    }

    auto pg_it = m_per_pg_chunks.emplace(pg_id, std::make_shared< PGChunkCollection >()).first;
    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    std::scoped_lock lock(pdev_heap->mtx, pg_chunk_collection->mtx);
    pg_chunks.reserve(num_chunk);

    // v_chunk_id start from 0.
    for (chunk_num_t v_chunk_id = 0; v_chunk_id < num_chunk; ++v_chunk_id) {
        auto chunk = pdev_heap->m_heap.top();
        // sanity check
        RELEASE_ASSERT(chunk->get_total_blks() == chunk->available_blks(), "chunk should be empty");
        RELEASE_ASSERT(chunk->available(), "chunk state should be available");
        pdev_heap->m_heap.pop();
        pdev_heap->available_blk_count -= chunk->available_blks();

        chunk->m_pg_id = pg_id;
        chunk->m_v_chunk_id = v_chunk_id;
        pg_chunks.emplace_back(chunk);
        ++pg_chunk_collection->available_num_chunks;
        pg_chunk_collection->m_total_blks += chunk->get_total_blks();
        pg_chunk_collection->available_blk_count += chunk->available_blks();
    }

    return num_chunk;
}

bool HeapChunkSelector::recover_pg_chunks(pg_id_t pg_id, std::vector< chunk_num_t >&& p_chunk_ids) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    // check pg exist
    if (m_per_pg_chunks.find(pg_id) != m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "pg={} had been recovered", pg_id);
        return false;
    }
    if (p_chunk_ids.size() == 0) {
        LOGWARNMOD(homeobject, "Unexpected empty pg={}", pg_id);
        return false;
    }

    // check chunks valid, must belong to m_chunks and have same pdev_id
    std::optional< uint32_t > last_pdev_id;
    for (auto p_chunk_id : p_chunk_ids) {
        auto it = m_chunks.find(p_chunk_id);
        if (it == m_chunks.end()) {
            LOGWARNMOD(homeobject, "No chunk found for p_chunk_id={}", p_chunk_id);
            return false;
        }
        auto chunk = it->second;
        if (last_pdev_id.has_value() && last_pdev_id.value() != chunk->get_pdev_id()) {
            LOGWARNMOD(homeobject, "The pdev value is different, last_pdev_id={}, pdev_id={}", last_pdev_id.value(),
                       chunk->get_pdev_id());
            return false;
        } else {
            last_pdev_id = chunk->get_pdev_id();
        }
    }

    auto pg_it = m_per_pg_chunks.emplace(pg_id, std::make_shared< PGChunkCollection >()).first;
    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    std::scoped_lock lock(pg_chunk_collection->mtx);
    pg_chunks.reserve(p_chunk_ids.size());

    // v_chunk_id start from 0.
    for (chunk_num_t v_chunk_id = 0; v_chunk_id < p_chunk_ids.size(); ++v_chunk_id) {
        chunk_num_t p_chunk_id = p_chunk_ids[v_chunk_id];
        auto chunk = m_chunks[p_chunk_id];
        chunk->m_pg_id = pg_id;
        chunk->m_v_chunk_id = v_chunk_id;
        pg_chunks.emplace_back(chunk);
    }
    return true;
}

void HeapChunkSelector::recover_per_dev_chunk_heap() {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    for (auto [p_chunk_id, chunk] : m_chunks) {
        // if selected for pg, not add to pdev.
        bool add_to_heap = !chunk->m_pg_id.has_value();
        add_chunk_internal(p_chunk_id, add_to_heap);
    }
}

bool HeapChunkSelector::recover_pg_chunks_states(pg_id_t pg_id,
                                                 const std::unordered_set< chunk_num_t >& excluding_v_chunk_ids) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "PG chunks should be recovered beforhand, pg={}", pg_id);
        return false;
    }

    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    std::scoped_lock lock(pg_chunk_collection->mtx);

    for (size_t v_chunk_id = 0; v_chunk_id < pg_chunks.size(); ++v_chunk_id) {
        auto chunk = pg_chunks[v_chunk_id];
        pg_chunk_collection->m_total_blks += chunk->get_total_blks();
        if (excluding_v_chunk_ids.find(v_chunk_id) == excluding_v_chunk_ids.end()) {
            chunk->m_state = ChunkState::AVAILABLE;
            ++pg_chunk_collection->available_num_chunks;
            pg_chunk_collection->available_blk_count += chunk->available_blks();

        } else {
            chunk->m_state = ChunkState::INUSE;
        }
    }
    return true;
}

std::shared_ptr< const std::vector< homestore::chunk_num_t > > HeapChunkSelector::get_pg_chunks(pg_id_t pg_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "pg={} had never been created", pg_id);
        return nullptr;
    }

    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    std::scoped_lock lock(pg_chunk_collection->mtx);
    auto p_chunk_ids = std::make_shared< std::vector< homestore::chunk_num_t > >();
    p_chunk_ids->reserve(pg_chunks.size());
    for (auto chunk : pg_chunks) {
        p_chunk_ids->emplace_back(chunk->get_chunk_id());
    }
    return p_chunk_ids;
}

std::optional< homestore::chunk_num_t > HeapChunkSelector::get_most_available_blk_chunk(uint64_t ctx, pg_id_t pg_id) {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return std::nullopt;
    }
    std::scoped_lock lock(pg_it->second->mtx);
    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    auto max_it =
        std::max_element(pg_chunks.begin(), pg_chunks.end(),
                         [](const std::shared_ptr< ExtendedVChunk >& a, const std::shared_ptr< ExtendedVChunk >& b) {
                             return !a->available() || (b->available() && a->available_blks() < b->available_blks());
                         });
    if (!(*max_it)->available()) {
        LOGWARNMOD(homeobject, "No available chunk for pg={}, ctx=0x{:x}", pg_id, ctx);
        return std::nullopt;
    }
    auto v_chunk_id = std::distance(pg_chunks.begin(), max_it);
    LOGDEBUGMOD(homeobject, "Picked v_chunk_id={} : [p_chunk_id={}, avail={}], ctx=0x{:x}", v_chunk_id,
                pg_chunks[v_chunk_id]->get_chunk_id(), pg_chunks[v_chunk_id]->available_blks(), ctx);
    pg_chunks[v_chunk_id]->m_state = ChunkState::INUSE;
    --pg_chunk_collection->available_num_chunks;
    pg_chunk_collection->available_blk_count -= pg_chunks[v_chunk_id]->available_blks();
    return v_chunk_id;
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

uint32_t HeapChunkSelector::avail_num_chunks(pg_id_t pg_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return 0;
    }
    return pg_it->second->available_num_chunks.load();
}

uint32_t HeapChunkSelector::total_chunks() const { return m_chunks.size(); }

uint64_t HeapChunkSelector::avail_blks(pg_id_t pg_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return 0;
    }
    return pg_it->second->available_blk_count.load();
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

std::list< uint32_t > HeapChunkSelector::get_pdev_ids() const {
    std::list< uint32_t > pdev_ids;
    for (const auto& [pdev_id, _] : m_per_dev_heap) {
        pdev_ids.emplace_back(pdev_id);
    }
    return pdev_ids;
}

std::shared_ptr< HeapChunkSelector::ExtendedVChunk > HeapChunkSelector::select_chunk_from_pdev(uint32_t pdev_id) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    auto it = m_per_dev_heap.find(pdev_id);
    if (it == m_per_dev_heap.end()) {
        LOGWARNMOD(homeobject, "No pdev heap found for pdev {}", pdev_id);
        return nullptr;
    }
    auto pdev_heap = it->second;
    std::unique_lock lock(pdev_heap->mtx);
    if (pdev_heap->m_heap.empty()) {
        LOGWARNMOD(homeobject, "No available chunk found for pdev {}", pdev_id);
        return nullptr;
    }
    auto chunk = pdev_heap->m_heap.top();
    pdev_heap->m_heap.pop();
    pdev_heap->available_blk_count -= chunk->available_blks();

    RELEASE_ASSERT(!chunk->m_pg_id.has_value(), "chunk {} is selected from pdev heap, but it has a pg_id {}!",
                   chunk->get_chunk_id(), chunk->m_pg_id.value());

    return chunk;
}

std::shared_ptr< HeapChunkSelector::ExtendedVChunk >
HeapChunkSelector::select_specific_chunk_from_pdev(uint32_t pdev_id, const chunk_num_t chunk_id) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    auto it = m_per_dev_heap.find(pdev_id);
    if (it == m_per_dev_heap.end()) {
        LOGWARNMOD(homeobject, "No pdev heap found for pdev {}", pdev_id);
        return nullptr;
    }
    auto pdev_heap = it->second;
    std::shared_ptr< HeapChunkSelector::ExtendedVChunk > EVchunk;
    std::list< std::shared_ptr< HeapChunkSelector::ExtendedVChunk > > EVchunks;

    std::unique_lock lock(pdev_heap->mtx);
    for (; pdev_heap->m_heap.empty();) {
        EVchunk = pdev_heap->m_heap.top();
        pdev_heap->m_heap.pop();
        if (EVchunk->get_chunk_id() == chunk_id) {
            break;
        } else {
            EVchunks.emplace_back(EVchunk);
        }
    }

    for (const auto& chunk : EVchunks) {
        pdev_heap->m_heap.emplace(chunk);
    }

    if (!EVchunk) {
        LOGWARNMOD(homeobject, "No available chunk found for pdev {}, chunk_id {}", pdev_id, chunk_id);
        return nullptr;
    }

    pdev_heap->available_blk_count -= EVchunk->available_blks();

    RELEASE_ASSERT(!EVchunk->m_pg_id.has_value(), "chunk {} is selected from pdev heap, but it has a pg_id {}!",
                   EVchunk->get_chunk_id(), EVchunk->m_pg_id.value());

    return EVchunk;
}

const std::unordered_map< homestore::chunk_num_t, homestore::cshared< HeapChunkSelector::ExtendedVChunk > >&
HeapChunkSelector::get_all_chunks() const {
    return m_chunks;
}

homestore::cshared< HeapChunkSelector::ExtendedVChunk >
HeapChunkSelector::get_extend_vchunk(const chunk_num_t chunk_id) const {
    auto it = m_chunks.find(chunk_id);
    if (it != m_chunks.end()) { return it->second; }
    return nullptr;
}

} // namespace homeobject
