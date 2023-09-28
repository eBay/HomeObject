cmake_minimum_required(VERSION 3.13)

include_directories (BEFORE .)

add_executable(test_home_object)
target_sources(test_home_object PRIVATE test_home_object.cpp)
target_link_libraries(test_home_object
            homeobject_homestore
            ${COMMON_TEST_DEPS}
        )
add_test(NAME HomeObject COMMAND ${CMAKE_BINARY_DIR}/bin/test_home_object)
set_property(TEST HomeObject PROPERTY RUN_SERIAL 1)

add_executable (test_shard_manager)
target_sources(test_shard_manager PRIVATE test_shard_manager.cpp)
target_link_libraries(test_shard_manager
            homeobject_homestore
            ${COMMON_TEST_DEPS}
)
add_test(NAME ShardManagerTest COMMAND ${CMAKE_BINARY_DIR}/bin/test_shard_manager)

add_executable (test_heap_chunk_selector)
target_sources(test_heap_chunk_selector PRIVATE test_heap_chunk_selector.cpp ../heap_chunk_selector.cpp)
target_link_libraries(test_heap_chunk_selector homeobject_homestore ${COMMON_TEST_DEPS})
add_test(NAME HeapChunkSelectorTest COMMAND ${CMAKE_BINARY_DIR}/bin/test_heap_chunk_selector)

