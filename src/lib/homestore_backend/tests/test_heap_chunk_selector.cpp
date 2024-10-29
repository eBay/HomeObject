#include <gtest/gtest.h>

#include <sisl/options/options.h>
#include <sisl/logging/logging.h>
#include <folly/init/Init.h>

#include <memory>

#include "homeobject/common.hpp"
#define protected public
#define private public
#include "lib/homestore_backend/heap_chunk_selector.h"

SISL_LOGGING_DEF(HOMEOBJECT_LOG_MODS)
SISL_LOGGING_INIT(HOMEOBJECT_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

namespace homestore {
// This is a fake implementation of Chunk/VChunk to avoid linking with homestore instance.

// if redefinition error was seen while building this file, any api being added in homestore::Chunk/VChunk, also needs
// to add one here to avoid redefinition error.
// Compiler will get confused if symbol can't be resolved locally(e.g. in this file), and will try to find it in
// homestore library which will cause redefine error.
class Chunk : public std::enable_shared_from_this< Chunk > {
public:
    uint32_t available_blks() const { return m_available_blks; }

    void set_available_blks(uint32_t available_blks) { m_available_blks = available_blks; }

    uint32_t get_defrag_nblks() const { return m_defrag_nblks; }

    void set_defrag_nblks(uint32_t defrag_nblks) { m_defrag_nblks = defrag_nblks; }

    uint32_t get_pdev_id() const { return m_pdev_id; }

    void set_pdev_id(uint32_t pdev_id) { m_pdev_id = pdev_id; }

    uint16_t get_chunk_id() const { return m_chunk_id; }

    blk_num_t get_total_blks() const { return m_available_blks; }
    void set_chunk_id(uint16_t chunk_id) { m_chunk_id = chunk_id; }
    const std::shared_ptr< Chunk > get_internal_chunk() { return shared_from_this(); }
    uint64_t size() const { return 1 * Mi; }

    Chunk(uint32_t pdev_id, uint16_t chunk_id, uint32_t available_blks, uint32_t defrag_nblks) {
        m_available_blks = available_blks;
        m_pdev_id = pdev_id;
        m_chunk_id = chunk_id;
        m_defrag_nblks = defrag_nblks;
    }

private:
    uint32_t m_available_blks;
    uint32_t m_pdev_id;
    uint16_t m_chunk_id;
    uint32_t m_defrag_nblks;
};

VChunk::VChunk(cshared< Chunk >& chunk) : m_internal_chunk(chunk) {}

void VChunk::set_user_private(const sisl::blob& data) {}

const uint8_t* VChunk::get_user_private() const { return nullptr; };

blk_num_t VChunk::available_blks() const { return m_internal_chunk->available_blks(); }

blk_num_t VChunk::get_defrag_nblks() const { return m_internal_chunk->get_defrag_nblks(); }

uint32_t VChunk::get_pdev_id() const { return m_internal_chunk->get_pdev_id(); }

uint16_t VChunk::get_chunk_id() const { return m_internal_chunk->get_chunk_id(); }

blk_num_t VChunk::get_total_blks() const { return m_internal_chunk->get_total_blks(); }

uint64_t VChunk::size() const { return m_internal_chunk->size(); }

cshared< Chunk > VChunk::get_internal_chunk() const { return m_internal_chunk->get_internal_chunk(); }

} // namespace homestore

using homeobject::csharedChunk;
using homeobject::HeapChunkSelector;
using homestore::Chunk;
using homestore::chunk_num_t;

class HeapChunkSelectorTest : public ::testing::Test {
protected:
    void SetUp() override {
        HCS.add_chunk(std::make_shared< Chunk >(1, 1, 1, 9));
        HCS.add_chunk(std::make_shared< Chunk >(1, 2, 2, 8));
        HCS.add_chunk(std::make_shared< Chunk >(1, 3, 3, 7));
        HCS.add_chunk(std::make_shared< Chunk >(2, 4, 1, 6));
        HCS.add_chunk(std::make_shared< Chunk >(2, 5, 2, 5));
        HCS.add_chunk(std::make_shared< Chunk >(2, 6, 3, 4));
        HCS.add_chunk(std::make_shared< Chunk >(3, 7, 1, 3));
        HCS.add_chunk(std::make_shared< Chunk >(3, 8, 2, 2));
        HCS.add_chunk(std::make_shared< Chunk >(3, 9, 3, 1));
        HCS.recover_per_dev_chunk_heap();
        prepare_pg();
    };

    void prepare_pg() {
        const uint32_t chunk_size = HCS.get_chunk_size(); // may problem
        const u_int64_t pg_size = chunk_size * 3;
        for (uint16_t pg_id = 1; pg_id < 4; ++pg_id) {
            HCS.select_chunks_for_pg(pg_id, pg_size);
            uint32_t last_pdev_id = 0;
            // test pg heap
            auto pg_heap_it = HCS.m_per_pg_heap.find(pg_id);
            ASSERT_NE(pg_heap_it, HCS.m_per_pg_heap.end());
            ASSERT_EQ(pg_heap_it->second->size(), 3);

            // test chunk_map
            auto v2r_chunk_map_it = HCS.m_v2r_chunk_map.find(pg_id);
            ASSERT_NE(v2r_chunk_map_it, HCS.m_v2r_chunk_map.end());
            ASSERT_EQ(v2r_chunk_map_it->second->size(), 3);

            auto r2v_chunk_map_it = HCS.m_r2v_chunk_map.find(pg_id);
            ASSERT_NE(r2v_chunk_map_it, HCS.m_r2v_chunk_map.end());
            ASSERT_EQ(r2v_chunk_map_it->second->size(), 3);
            for (int i = 0; i < 3; ++i) {
                auto r_chunk_id = v2r_chunk_map_it->second->at(i);
                ASSERT_EQ(i, r2v_chunk_map_it->second->at(r_chunk_id));
                auto pdev_id = HCS.m_chunks[r_chunk_id]->get_pdev_id();
                if (last_pdev_id != 0) {
                    ASSERT_EQ(last_pdev_id, pdev_id);
                } else {
                    last_pdev_id = pdev_id;
                }

                auto pdev_it = HCS.m_per_dev_heap.find(pdev_id);
                ASSERT_NE(pdev_it, HCS.m_per_dev_heap.end());
                ASSERT_EQ(pdev_it->second->size(), 0);
            }
        }
    }


public:
    HeapChunkSelector HCS;
};

TEST_F(HeapChunkSelectorTest, test_for_each_chunk) {
    std::atomic_uint32_t size;
    HCS.foreach_chunks([&size](csharedChunk& chunk) { size.fetch_add(chunk->available_blks()); });
    ASSERT_EQ(size.load(), 18);
}

TEST_F(HeapChunkSelectorTest, test_select_chunk) {
    homestore::blk_count_t count = 1;
    homestore::blk_alloc_hints hints;
    auto chunk = HCS.select_chunk(count, hints);
    ASSERT_EQ(chunk, nullptr);

    for (uint16_t pg_id = 1; pg_id < 4; ++pg_id) {
        hints.pdev_id_hint = pg_id; // tmp bypass using pdev_id_hint present pg_id
        for (int j = 3; j > 0; --j) {
            auto chunk = HCS.select_chunk(count, hints);
            ASSERT_NE(chunk, nullptr);
            ASSERT_EQ(chunk->get_pdev_id(), pg_id);
            ASSERT_EQ(chunk->available_blks(), j);
        }
    }
}


TEST_F(HeapChunkSelectorTest, test_select_specific_chunk) {
    const uint16_t pg_id = 1;
    auto chunk_ids = HCS.get_pg_chunks(pg_id);
    ASSERT_NE(chunk_ids, nullptr);
    const chunk_num_t chunk_id = chunk_ids->at(0);

    auto chunk = HCS.select_specific_chunk(pg_id, chunk_id);
    ASSERT_EQ(chunk->get_chunk_id(), chunk_id);
    auto pdev_id = chunk->get_pdev_id();

    // make sure pg chunk map
    auto pg_heap_it = HCS.m_per_pg_heap.find(pg_id);
    ASSERT_NE(pg_heap_it, HCS.m_per_pg_heap.end());
    ASSERT_EQ(pg_heap_it->second->size(), 2);

    // test chunk_map stable
    auto v2r_chunk_map_it = HCS.m_v2r_chunk_map.find(pg_id);
    ASSERT_NE(v2r_chunk_map_it, HCS.m_v2r_chunk_map.end());
    ASSERT_EQ(v2r_chunk_map_it->second->size(), 3);

    auto r2v_chunk_map_it = HCS.m_r2v_chunk_map.find(pg_id);
    ASSERT_NE(r2v_chunk_map_it, HCS.m_r2v_chunk_map.end());
    ASSERT_EQ(r2v_chunk_map_it->second->size(), 3);

    // select the rest chunks to make sure specific chunk does not exist in HeapChunkSelector anymore.
    homestore::blk_count_t count = 1;
    homestore::blk_alloc_hints hints;
    hints.pdev_id_hint = pg_id;
    for (int j = 2; j > 0; --j) {
        auto chunk = HCS.select_chunk(count, hints);
        ASSERT_EQ(chunk->get_pdev_id(), pdev_id);
    }

    // release this chunk to HeapChunkSelector
    HCS.release_chunk(pg_id, chunk_id);
    chunk = HCS.select_chunk(1, hints);
    ASSERT_EQ(1, chunk->get_pdev_id());
    ASSERT_EQ(chunk_id, chunk->get_chunk_id());

}


TEST_F(HeapChunkSelectorTest, test_release_chunk) {
    homestore::blk_count_t count = 1;
    homestore::blk_alloc_hints hints;
    const uint16_t pg_id = 1;
    hints.pdev_id_hint = pg_id;
    auto chunk1 = HCS.select_chunk(count, hints);
    auto pdev_id = chunk1->get_pdev_id();

    ASSERT_EQ(chunk1->get_pdev_id(), pdev_id);
    ASSERT_EQ(chunk1->available_blks(), 3);

    auto chunk2 = HCS.select_chunk(count, hints);
    ASSERT_EQ(chunk2->get_pdev_id(), pdev_id);
    ASSERT_EQ(chunk2->available_blks(), 2);

    HCS.release_chunk(pg_id, chunk1->get_chunk_id());
    HCS.release_chunk(pg_id, chunk2->get_chunk_id());

    chunk1 = HCS.select_chunk(count, hints);
    ASSERT_EQ(chunk1->get_pdev_id(), pdev_id);
    ASSERT_EQ(chunk1->available_blks(), 3);

    chunk2 = HCS.select_chunk(count, hints);
    ASSERT_EQ(chunk2->get_pdev_id(), pdev_id);
    ASSERT_EQ(chunk2->available_blks(), 2);
}

TEST_F(HeapChunkSelectorTest, test_recovery) {
    HeapChunkSelector HCS_recovery;
    HCS_recovery.add_chunk(std::make_shared< Chunk >(1, 1, 1, 9));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(1, 2, 2, 8));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(1, 3, 3, 7));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(2, 4, 1, 6));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(2, 5, 2, 5));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(2, 6, 3, 4));

    std::vector<chunk_num_t> chunk_ids {1,2,3};
    const uint16_t pg_id = 1;
    // test recover chunk map
    HCS_recovery.set_pg_chunks(pg_id, std::move(chunk_ids));
    auto v2r_chunk_map_it = HCS_recovery.m_v2r_chunk_map.find(pg_id);
    ASSERT_NE(v2r_chunk_map_it, HCS_recovery.m_v2r_chunk_map.end());
    ASSERT_EQ(v2r_chunk_map_it->second->size(), 3);

    auto r2v_chunk_map_it = HCS_recovery.m_r2v_chunk_map.find(pg_id);
    ASSERT_NE(r2v_chunk_map_it, HCS_recovery.m_r2v_chunk_map.end());
    ASSERT_EQ(r2v_chunk_map_it->second->size(), 3);
    // test recover pdev map
    HCS_recovery.recover_per_dev_chunk_heap();
    auto pdev_it = HCS_recovery.m_per_dev_heap.find(1);
    ASSERT_NE(pdev_it, HCS_recovery.m_per_dev_heap.end());
    ASSERT_EQ(pdev_it->second->size(), 0);

    pdev_it = HCS_recovery.m_per_dev_heap.find(2);
    ASSERT_NE(pdev_it, HCS_recovery.m_per_dev_heap.end());
    ASSERT_EQ(pdev_it->second->size(), 3);
    auto &pdev_heap = pdev_it->second->m_heap;
    auto vchunk = homestore::VChunk(nullptr);
    for (int i = 6; i > 3; --i) {
        vchunk = pdev_heap.top();
        pdev_heap.pop();
        ASSERT_EQ(vchunk.get_chunk_id(), i);
    }

    // test recover pg heap
    std::unordered_set< homestore::chunk_num_t > excluding_chunks;
    excluding_chunks.emplace(1);
    HCS_recovery.recover_pg_chunk_heap(pg_id, excluding_chunks);
    auto pg_heap_it = HCS_recovery.m_per_pg_heap.find(pg_id);
    ASSERT_NE(pg_heap_it, HCS_recovery.m_per_pg_heap.end());
    ASSERT_EQ(pg_heap_it->second->size(), 2);

    homestore::blk_alloc_hints hints;
    hints.pdev_id_hint = pg_id;
    for (int j = 3; j > 1; --j) {
        auto chunk = HCS_recovery.select_chunk(1, hints);
        ASSERT_EQ(chunk->get_pdev_id(), 1);
        ASSERT_EQ(chunk->available_blks(), j);
    }
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);
    return RUN_ALL_TESTS();
}
