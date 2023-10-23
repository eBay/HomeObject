cmake_minimum_required(VERSION 3.13)

include_directories (BEFORE .)

list(APPEND TEST_SOURCES
    test_heap_chunk_selector.cpp
    hs_shard_tests.cpp
    )
if (NOT ${CMAKE_SYSTEM_PROCESSOR} STREQUAL "aarch64")
list(APPEND TEST_SOURCES
    hs_blob_tests.cpp
    )
endif()

add_library(homestore_tests OBJECT)
target_sources(homestore_tests PRIVATE ${TEST_SOURCES})
target_link_libraries(homestore_tests
            homeobject_homestore
            ${COMMON_TEST_DEPS}
        )
