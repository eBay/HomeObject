cmake_minimum_required(VERSION 3.13)

include_directories (BEFORE .)

list(APPEND TEST_SOURCES
    hs_shard_tests.cpp
    hs_blob_tests.cpp
    homeobj_cp_tests.cpp
    )

add_library(homestore_tests OBJECT)
target_sources(homestore_tests PRIVATE ${TEST_SOURCES})
target_link_libraries(homestore_tests
            homeobject_homestore
            ${COMMON_TEST_DEPS}
        )

add_executable (test_heap_chunk_selector)
target_sources(test_heap_chunk_selector PRIVATE test_heap_chunk_selector.cpp ../heap_chunk_selector.cpp)
target_link_libraries(test_heap_chunk_selector homestore::homestore ${COMMON_TEST_DEPS})
add_test(NAME HeapChunkSelectorTest COMMAND ${CMAKE_BINARY_DIR}/bin/test_heap_chunk_selector)
