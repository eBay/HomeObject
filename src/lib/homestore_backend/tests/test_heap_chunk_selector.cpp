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

cshared< Chunk > VChunk::get_internal_chunk() const { return m_internal_chunk; }

} // namespace homestore

using homeobject::ChunkState;
using homeobject::csharedChunk;
using homeobject::HeapChunkSelector;
using homeobject::pg_id_t;
using homestore::Chunk;
using homestore::chunk_num_t;

const pg_id_t FAKE_PG_ID = UINT16_MAX;
const chunk_num_t FAKE_CHUNK_ID = UINT16_MAX;

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
        const uint32_t chunk_size = HCS.get_chunk_size();
        const u_int64_t pg_size = chunk_size * 3;
        for (uint16_t pg_id = 1; pg_id < 4; ++pg_id) {
            ASSERT_EQ(HCS.select_chunks_for_pg(pg_id, pg_size), 3);
            uint32_t last_pdev_id = 0;
            // test pg heap
            auto pg_it = HCS.m_per_pg_chunks.find(pg_id);
            ASSERT_NE(pg_it, HCS.m_per_pg_chunks.end());
            auto pg_chunk_collection = pg_it->second;
            auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
            ASSERT_EQ(pg_chunk_collection->available_num_chunks, 3);
            ASSERT_EQ(pg_chunk_collection->available_blk_count, 1 + 2 + 3);
            ASSERT_EQ(pg_chunk_collection->m_total_blks, 1 + 2 + 3);

            for (int i = 0; i < 3; ++i) {
                // test chunk information
                auto p_chunk_id = pg_chunks[i]->get_chunk_id();
                ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_pg_id.value(), pg_id);
                ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_v_chunk_id.value(), i);
                ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_state, ChunkState::AVAILABLE);
                // test pg chunks must belong to same pdev
                auto pdev_id = HCS.m_chunks[p_chunk_id]->get_pdev_id();
                if (last_pdev_id != 0) {
                    ASSERT_EQ(last_pdev_id, pdev_id);
                } else {
                    last_pdev_id = pdev_id;
                }
                // pdev heap should be empty at this point because all chunks have already been given to pg.
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

TEST_F(HeapChunkSelectorTest, test_identical_layout) {
    const homestore::blk_count_t count = 1;
    homestore::blk_alloc_hints hints;
    for (uint16_t pg_id = 1; pg_id < 4; ++pg_id) {
        chunk_num_t p_chunk_id = 0;
        auto pg_chunk_collection = HCS.m_per_pg_chunks[pg_id];
        auto start_available_blk_count = 1 + 2 + 3;
        for (int j = 3; j > 0; --j) {
            ASSERT_EQ(pg_chunk_collection->available_blk_count, start_available_blk_count);

            const auto v_chunkID = HCS.get_most_available_blk_chunk(pg_id);
            ASSERT_TRUE(v_chunkID.has_value());
            p_chunk_id = pg_chunk_collection->m_pg_chunks[v_chunkID.value()]->get_chunk_id();
            ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_state, ChunkState::INUSE);
            ASSERT_EQ(pg_chunk_collection->available_num_chunks, j - 1);
            ASSERT_EQ(pg_chunk_collection->available_blk_count, start_available_blk_count - j);

            const auto v_chunkID2 = HCS.m_chunks[p_chunk_id]->m_v_chunk_id;
            ASSERT_TRUE(v_chunkID2.has_value());
            ASSERT_EQ(v_chunkID.value(), v_chunkID2.value());
            hints.application_hint = ((uint64_t)pg_id << 16) | v_chunkID.value();

            // mock leader on_commit
            ASSERT_NE(HCS.select_chunk(count, hints), nullptr);
            ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_state, ChunkState::INUSE);
            ASSERT_EQ(pg_chunk_collection->available_num_chunks, j - 1);
            ASSERT_EQ(pg_chunk_collection->available_blk_count, start_available_blk_count - j);

            // mock leader rollback or on_error
            ASSERT_TRUE(HCS.release_chunk(pg_id, v_chunkID.value()));
            ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_state, ChunkState::AVAILABLE);
            ASSERT_EQ(pg_chunk_collection->available_num_chunks, j);
            ASSERT_EQ(pg_chunk_collection->available_blk_count, start_available_blk_count);

            // mock follower rollback or on_error
            ASSERT_TRUE(HCS.release_chunk(pg_id, v_chunkID.value()));
            ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_state, ChunkState::AVAILABLE);
            ASSERT_EQ(pg_chunk_collection->available_num_chunks, j);
            ASSERT_EQ(pg_chunk_collection->available_blk_count, start_available_blk_count);

            // mock follower on_commit
            ASSERT_NE(HCS.select_chunk(count, hints), nullptr); // leader select
            ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_state, ChunkState::INUSE);
            ASSERT_EQ(pg_chunk_collection->available_num_chunks, j - 1);
            ASSERT_EQ(pg_chunk_collection->available_blk_count, start_available_blk_count - j);

            start_available_blk_count -= j;
        }
        // all chunks have been given out
        ASSERT_FALSE(HCS.get_most_available_blk_chunk(pg_id).has_value());
    }
}

TEST_F(HeapChunkSelectorTest, test_select_chunk) {
    homestore::blk_count_t count = 1;
    homestore::blk_alloc_hints hints;
    auto chunk = HCS.select_chunk(count, hints);
    ASSERT_EQ(chunk, nullptr);

    for (uint16_t pg_id = 1; pg_id < 4; ++pg_id) {
        for (int j = 3; j > 0; --j) {
            chunk_num_t v_chunk_id = 3 - j;
            hints.application_hint = ((uint64_t)pg_id << 16) | v_chunk_id;
            auto chunk = HCS.select_chunk(count, hints);
            ASSERT_NE(chunk, nullptr);
            ASSERT_EQ(chunk->get_pdev_id(), pg_id); // in this ut, pg_id is same as pdev id
            ASSERT_EQ(chunk->available_blks(), j);
        }
    }
}

TEST_F(HeapChunkSelectorTest, test_select_specific_chunk_and_release_chunk) {
    for (uint16_t pg_id = 1; pg_id < 4; ++pg_id) {
        // test fake
        ASSERT_FALSE(HCS.release_chunk(FAKE_PG_ID, FAKE_CHUNK_ID));
        ASSERT_FALSE(HCS.release_chunk(pg_id, FAKE_CHUNK_ID));
        ASSERT_EQ(nullptr, HCS.select_specific_chunk(FAKE_PG_ID, FAKE_CHUNK_ID));
        ASSERT_EQ(nullptr, HCS.select_specific_chunk(pg_id, FAKE_CHUNK_ID));

        auto chunk_ids = HCS.get_pg_chunks(pg_id);
        ASSERT_NE(chunk_ids, nullptr);
        const chunk_num_t v_chunk_id = 0;
        const chunk_num_t p_chunk_id = chunk_ids->at(v_chunk_id);

        auto pg_chunk_collection = HCS.m_per_pg_chunks[pg_id];
        auto chunk = HCS.select_specific_chunk(pg_id, v_chunk_id);
        ASSERT_NE(nullptr, chunk);
        ASSERT_EQ(chunk->get_chunk_id(), p_chunk_id);
        ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_state, ChunkState::INUSE);
        ASSERT_EQ(pg_chunk_collection->available_num_chunks, 2);
        ASSERT_EQ(pg_chunk_collection->available_blk_count, 1 + 2);

        // test select an INUSE chunk
        chunk = HCS.select_specific_chunk(pg_id, v_chunk_id);
        ASSERT_NE(nullptr, chunk);
        ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_state, ChunkState::INUSE);
        ASSERT_EQ(pg_chunk_collection->available_num_chunks, 2);
        ASSERT_EQ(pg_chunk_collection->available_blk_count, 1 + 2);

        // release this chunk to HeapChunkSelector
        ASSERT_TRUE(HCS.release_chunk(pg_id, v_chunk_id));
        ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_state, ChunkState::AVAILABLE);
        ASSERT_EQ(pg_chunk_collection->available_num_chunks, 3);
        ASSERT_EQ(pg_chunk_collection->available_blk_count, 1 + 2 + 3);

        // test release an AVAILABLE chunk
        ASSERT_TRUE(HCS.release_chunk(pg_id, v_chunk_id));
        ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_state, ChunkState::AVAILABLE);
        ASSERT_EQ(pg_chunk_collection->available_num_chunks, 3);
        ASSERT_EQ(pg_chunk_collection->available_blk_count, 1 + 2 + 3);

        // select again
        chunk = HCS.select_specific_chunk(pg_id, v_chunk_id);
        ASSERT_NE(nullptr, chunk);
        ASSERT_EQ(HCS.m_chunks[p_chunk_id]->m_state, ChunkState::INUSE);
        ASSERT_EQ(pg_chunk_collection->available_num_chunks, 2);
        ASSERT_EQ(pg_chunk_collection->available_blk_count, 1 + 2);
        ASSERT_EQ(pg_id, chunk->get_pdev_id()); // in this ut, pg_id is same as pdev id
        ASSERT_EQ(p_chunk_id, chunk->get_chunk_id());
    }
}

TEST_F(HeapChunkSelectorTest, test_recovery) {
    HeapChunkSelector HCS_recovery;
    HCS_recovery.add_chunk(std::make_shared< Chunk >(1, 1, 1, 9));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(1, 2, 2, 8));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(1, 3, 3, 7));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(2, 4, 1, 6));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(2, 5, 2, 5));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(2, 6, 3, 4));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(3, 7, 1, 3));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(3, 8, 2, 2));
    HCS_recovery.add_chunk(std::make_shared< Chunk >(3, 9, 3, 1));

    // on_pg_meta_blk_found
    for (uint16_t pg_id = 1; pg_id < 4; ++pg_id) {
        std::vector< chunk_num_t > chunk_ids{1, 2};
        std::vector< chunk_num_t > chunk_ids_for_twice{1, 2};
        std::vector< chunk_num_t > chunk_ids_not_valid{1, 20};
        std::vector< chunk_num_t > chunk_ids_not_same_pdev{1, 6};
        for (chunk_num_t j = 0; j < 2; ++j) {
            chunk_ids[j] += (pg_id - 1) * 3;
            chunk_ids_for_twice[j] += (pg_id - 1) * 3;
            chunk_ids_not_valid[j] += (pg_id - 1) * 3;
            chunk_ids_not_same_pdev[j] += ((pg_id - 1) * 3) % 9;
        }

        // test recover chunk map
        ASSERT_FALSE(HCS_recovery.recover_pg_chunks(pg_id, std::move(chunk_ids_not_valid)));
        ASSERT_FALSE(HCS_recovery.recover_pg_chunks(pg_id, std::move(chunk_ids_not_same_pdev)));

        ASSERT_TRUE(HCS_recovery.recover_pg_chunks(pg_id, std::move(chunk_ids)));
        // can't set pg chunks twice
        ASSERT_FALSE(HCS_recovery.recover_pg_chunks(pg_id, std::move(chunk_ids_for_twice)));

        auto pg_it = HCS_recovery.m_per_pg_chunks.find(pg_id);
        ASSERT_NE(pg_it, HCS_recovery.m_per_pg_chunks.end());
        auto pg_chunk_collection = pg_it->second;
        ASSERT_EQ(pg_chunk_collection->m_pg_chunks.size(), 2);
        for (chunk_num_t v_chunk_id = 0; v_chunk_id < 2; ++v_chunk_id) {
            ASSERT_EQ(pg_chunk_collection->m_pg_chunks[v_chunk_id]->m_pg_id, pg_id);
            ASSERT_EQ(pg_chunk_collection->m_pg_chunks[v_chunk_id]->m_v_chunk_id, v_chunk_id);
            ASSERT_EQ(pg_chunk_collection->m_pg_chunks[v_chunk_id]->get_chunk_id(), chunk_ids[v_chunk_id]);
        }
    }

    //  on_pg_meta_blk_recover_completed
    HCS_recovery.recover_per_dev_chunk_heap();
    for (uint16_t pg_id = 1; pg_id < 4; ++pg_id) {
        // test recover pdev map size
        auto pdev_it = HCS_recovery.m_per_dev_heap.find(pg_id);
        ASSERT_NE(pdev_it, HCS_recovery.m_per_dev_heap.end());
        ASSERT_EQ(pdev_it->second->size(), 1); // 1 = 3(all) - 2(pg)

        auto& pdev_heap = pdev_it->second->m_heap;
        auto chunk = pdev_heap.top();
        ASSERT_EQ(chunk->get_chunk_id(), 3 + (pg_id - 1) * 3);
    }

    // on_shard_meta_blk_recover_completed
    for (uint16_t pg_id = 1; pg_id < 4; ++pg_id) {
        // test recover pg heap
        std::unordered_set< homestore::chunk_num_t > excluding_chunks;
        excluding_chunks.emplace(0);

        ASSERT_FALSE(HCS_recovery.recover_pg_chunks_states(FAKE_PG_ID, excluding_chunks));
        ASSERT_TRUE(HCS_recovery.recover_pg_chunks_states(pg_id, excluding_chunks));

        auto pg_it = HCS_recovery.m_per_pg_chunks.find(pg_id);
        ASSERT_NE(pg_it, HCS_recovery.m_per_pg_chunks.end());
        auto pg_chunk_collection = pg_it->second;
        ASSERT_EQ(pg_chunk_collection->m_pg_chunks.size(), 2); // size wont change
        ASSERT_EQ(pg_chunk_collection->available_num_chunks, 1);
        ASSERT_EQ(pg_chunk_collection->available_blk_count, 2); // only left v_chunk_id=1

        ASSERT_EQ(pg_chunk_collection->m_pg_chunks[0]->m_state, ChunkState::INUSE);
        ASSERT_EQ(pg_chunk_collection->m_pg_chunks[1]->m_state, ChunkState::AVAILABLE);

        const auto v_chunkID = HCS_recovery.get_most_available_blk_chunk(pg_id);
        ASSERT_TRUE(v_chunkID.has_value());
        auto chunk = HCS_recovery.select_specific_chunk(pg_id, v_chunkID.value());
        ASSERT_NE(chunk, nullptr);
        ASSERT_EQ(chunk->get_pdev_id(), pg_id);
        ASSERT_EQ(chunk->available_blks(), 2);
        ASSERT_EQ(pg_chunk_collection->m_pg_chunks[1]->m_state, ChunkState::INUSE);
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
