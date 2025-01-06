cmake_minimum_required (VERSION 3.11)

list(APPEND COMMON_DEPS homestore::homestore)

# This is a work-around for not being able to specify the link
# order in a conan recipe. We link these explicitly and thus
# need to specify the LINK path. They should only be needed
# to build a DSO (test executable) however.
link_directories(${spdk_LIB_DIRS} ${dpdk_LIB_DIRS})
list(APPEND COMMON_TEST_DEPS
    ${spdk_LIBRARY_LIST}
    ${dpdk_LIBRARY_LIST}
)

if(NOT ${CMAKE_CURRENT_BINARY_DIR} STREQUAL ${CMAKE_CURRENT_SOURCE_DIR})
  include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR})
endif()

add_library ("${PROJECT_NAME}_homestore")
target_sources("${PROJECT_NAME}_homestore" PRIVATE
    hs_homeobject.cpp
    hs_blob_manager.cpp
    hs_shard_manager.cpp
    hs_pg_manager.cpp
    pg_blob_iterator.cpp
    snapshot_receive_handler.cpp
    index_kv.cpp
    heap_chunk_selector.cpp
    replication_state_machine.cpp
    hs_cp_callbacks.cpp
    hs_http_manager.cpp
    $<TARGET_OBJECTS:${PROJECT_NAME}_core>
)
target_link_libraries("${PROJECT_NAME}_homestore" PUBLIC
    homestore::homestore
    ${COMMON_DEPS}
)

set(FLATBUFFERS_FLATC_EXECUTABLE ${flatbuffers_INCLUDE_DIRS}/../bin/flatc)
settings_gen_cpp(
    ${FLATBUFFERS_FLATC_EXECUTABLE}
    ${CMAKE_CURRENT_BINARY_DIR}/generated/
    "${PROJECT_NAME}_homestore"
    hs_backend_config.fbs
    resync_pg_data.fbs
    resync_shard_data.fbs
    resync_blob_data.fbs
  )

add_subdirectory(tests)

add_executable (homestore_test)
target_sources(homestore_test PRIVATE
    $<TARGET_OBJECTS:homestore_tests>
)
target_link_libraries(homestore_test PUBLIC
    homeobject_homestore
    ${COMMON_TEST_DEPS}
)
add_test(NAME HomestoreTest COMMAND homestore_test -csv error --executor immediate --config_path ./ --override_config homestore_config.consensus.snapshot_freq_distance:0)
set_property(TEST HomestoreTest PROPERTY RUN_SERIAL 1)

add_executable (homestore_test_dynamic)
target_sources(homestore_test_dynamic PRIVATE
    $<TARGET_OBJECTS:homestore_tests_dynamic>
)
target_link_libraries(homestore_test_dynamic PUBLIC
    homeobject_homestore
    ${COMMON_TEST_DEPS}
)

add_test(NAME HomestoreTestDynamic
        COMMAND homestore_test_dynamic -csv error --executor immediate --config_path ./
        --override_config homestore_config.consensus.snapshot_freq_distance:0)

# To test both baseline & incremental resync functionality, we use 13 to minimize the likelihood of it being a divisor of the total LSN (currently 30)
add_test(NAME HomestoreTestDynamicWithResync
        COMMAND homestore_test_dynamic -csv error --executor immediate --config_path ./
        --override_config homestore_config.consensus.snapshot_freq_distance:13
        --override_config homestore_config.consensus.num_reserved_log_items=13)
