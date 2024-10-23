cmake_minimum_required(VERSION 3.13)

include_directories (BEFORE .)

list(APPEND TEST_SOURCES
    hs_shard_tests.cpp
    hs_blob_tests.cpp
    hs_pg_tests.cpp
    homeobj_cp_tests.cpp
    test_homestore_backend.cpp
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
add_test(NAME HeapChunkSelectorTest COMMAND test_heap_chunk_selector)
