#include "lib/homestore_backend/heap_chunk_selector.h"

#include <gtest/gtest.h>

#include <sisl/options/options.h>
#include <sisl/logging/logging.h>
#include <folly/init/Init.h>

#include <memory>

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

    blk_num_t get_total_blks() const { return 0; }
    void set_chunk_id(uint16_t chunk_id) { m_chunk_id = chunk_id; }
    const std::shared_ptr< Chunk > get_internal_chunk() { return shared_from_this(); }

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
        std::unordered_set< chunk_num_t > excludingChunks;
        HCS.build_per_dev_chunk_heap(excludingChunks);
    };

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
    for (uint32_t i = 1; i < 4; i++) {
        hints.pdev_id_hint = i;
        for (int j = 3; j > 0; j--) {
            auto chunk = HCS.select_chunk(count, hints);
            ASSERT_EQ(chunk->get_pdev_id(), i);
            ASSERT_EQ(chunk->available_blks(), j);
        }
    }
}

TEST_F(HeapChunkSelectorTest, test_select_specific_chunk) {
    const chunk_num_t chunk_id = 3;
    auto chunk = HCS.select_specific_chunk(chunk_id);
    ASSERT_EQ(chunk->get_pdev_id(), 1);
    ASSERT_EQ(chunk->get_chunk_id(), chunk_id);

    // select the rest chunks to make sure specific chunk does not exist in HeapChunkSelector anymore.
    homestore::blk_count_t count = 1;
    homestore::blk_alloc_hints hints;
    for (uint32_t i = 1; i < 4; i++) {
        hints.pdev_id_hint = i;
        auto chunk_num = 3;
        if (i == 1) { --chunk_num; }
        for (int j = chunk_num; j > 0; j--) {
            auto chunk = HCS.select_chunk(count, hints);
            ASSERT_EQ(chunk->get_pdev_id(), i);
            ASSERT_EQ(chunk->available_blks(), j);
        }
    }

    // release this chunk to HeapChunkSelector
    HCS.release_chunk(chunk_id);
    chunk = HCS.select_chunk(1, homestore::blk_alloc_hints());
    ASSERT_EQ(1, chunk->get_pdev_id());
    ASSERT_EQ(chunk_id, chunk->get_chunk_id());
}

TEST_F(HeapChunkSelectorTest, test_most_defrag_chunk) {
    for (uint32_t i = 1; i < 6; i++) {
        auto chunk = HCS.most_defrag_chunk();
        // should always select the chunk with the most defrag blocks
        ASSERT_EQ(chunk->get_chunk_id(), i);
    }

    // after release a chunk with the most defrag blocks, most_defrag_chunk should select this chunk.
    HCS.release_chunk(1);
    auto chunk = HCS.most_defrag_chunk();
    ASSERT_EQ(chunk->get_chunk_id(), 1);
}

TEST_F(HeapChunkSelectorTest, test_release_chunk) {
    homestore::blk_count_t count = 1;
    homestore::blk_alloc_hints hints;
    hints.pdev_id_hint = 1;
    auto chunk1 = HCS.select_chunk(count, hints);
    ASSERT_EQ(chunk1->get_pdev_id(), 1);
    ASSERT_EQ(chunk1->available_blks(), 3);

    auto chunk2 = HCS.select_chunk(count, hints);
    ASSERT_EQ(chunk2->get_pdev_id(), 1);
    ASSERT_EQ(chunk2->available_blks(), 2);

    HCS.release_chunk(chunk1->get_chunk_id());
    HCS.release_chunk(chunk2->get_chunk_id());

    chunk1 = HCS.select_chunk(count, hints);
    ASSERT_EQ(chunk1->get_pdev_id(), 1);
    ASSERT_EQ(chunk1->available_blks(), 3);

    chunk2 = HCS.select_chunk(count, hints);
    ASSERT_EQ(chunk2->get_pdev_id(), 1);
    ASSERT_EQ(chunk2->available_blks(), 2);
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
