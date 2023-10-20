cmake_minimum_required(VERSION 3.13)

include_directories (BEFORE .)

add_executable(hs_blob_tests)
target_sources(hs_blob_tests PRIVATE hs_blob_tests.cpp)
target_link_libraries(hs_blob_tests
            homeobject_homestore
            ${COMMON_TEST_DEPS}
        )
add_test(NAME HSBlobTests COMMAND ${CMAKE_BINARY_DIR}/bin/hs_blob_tests)
set_property(TEST HSBlobTests PROPERTY RUN_SERIAL 1)

add_executable (hs_shard_tests)
target_sources(hs_shard_tests PRIVATE hs_shard_tests.cpp)
target_link_libraries(hs_shard_tests
            homeobject_homestore
            ${COMMON_TEST_DEPS}
)
add_test(NAME HSShardTests COMMAND ${CMAKE_BINARY_DIR}/bin/hs_shard_tests)
set_property(TEST HSShardTests PROPERTY RUN_SERIAL 1)

add_executable (test_heap_chunk_selector)
target_sources(test_heap_chunk_selector PRIVATE test_heap_chunk_selector.cpp ../heap_chunk_selector.cpp)
target_link_libraries(test_heap_chunk_selector homeobject_homestore ${COMMON_TEST_DEPS})
add_test(NAME HeapChunkSelectorTest COMMAND ${CMAKE_BINARY_DIR}/bin/test_heap_chunk_selector)

