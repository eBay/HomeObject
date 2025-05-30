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
    gc_manager.cpp
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

# Unit test objects
add_subdirectory(tests)

# Basic tests
add_executable(homestore_test_pg)
target_sources(homestore_test_pg PRIVATE $<TARGET_OBJECTS:homestore_tests_pg>)
target_link_libraries(homestore_test_pg PUBLIC homeobject_homestore ${COMMON_TEST_DEPS})
add_test(NAME HomestoreTestPg COMMAND homestore_test_pg -csv error --executor immediate --config_path ./
        --override_config homestore_config.consensus.snapshot_freq_distance:0
        --override_config homestore_config.consensus.max_grpc_message_size:138412032)

add_executable(homestore_test_shard)
target_sources(homestore_test_shard PRIVATE $<TARGET_OBJECTS:homestore_tests_shard>)
target_link_libraries(homestore_test_shard PUBLIC homeobject_homestore ${COMMON_TEST_DEPS})
add_test(NAME HomestoreTestShard COMMAND homestore_test_shard -csv error --executor immediate --config_path ./
        --override_config homestore_config.consensus.snapshot_freq_distance:0
        --override_config homestore_config.consensus.max_grpc_message_size:138412032)

add_executable(homestore_test_blob)
target_sources(homestore_test_blob PRIVATE $<TARGET_OBJECTS:homestore_tests_blob>)
target_link_libraries(homestore_test_blob PUBLIC homeobject_homestore ${COMMON_TEST_DEPS})
add_test(NAME HomestoreTestBlob COMMAND homestore_test_blob -csv error --executor immediate --config_path ./
        --override_config homestore_config.consensus.snapshot_freq_distance:0
        --override_config homestore_config.consensus.max_grpc_message_size:138412032)

add_executable(homestore_test_misc)
target_sources(homestore_test_misc PRIVATE $<TARGET_OBJECTS:homestore_tests_misc>)
target_link_libraries(homestore_test_misc PUBLIC homeobject_homestore ${COMMON_TEST_DEPS})
add_test(NAME HomestoreTestMisc COMMAND homestore_test_misc -csv error --executor immediate --config_path ./
        --override_config homestore_config.consensus.snapshot_freq_distance:0
        --override_config homestore_config.consensus.max_grpc_message_size:138412032)

# Dynamic tests
add_executable(homestore_test_dynamic)
target_sources(homestore_test_dynamic PRIVATE $<TARGET_OBJECTS:homestore_tests_dynamic>)
target_link_libraries(homestore_test_dynamic PUBLIC homeobject_homestore ${COMMON_TEST_DEPS})
add_test(NAME HomestoreTestReplaceMember
        COMMAND homestore_test_dynamic -csv error --executor immediate --config_path ./
        --override_config homestore_config.consensus.snapshot_freq_distance:0
        --gtest_filter=HomeObjectFixture.ReplaceMember
        --override_config homestore_config.generic.repl_dev_cleanup_interval_sec=5
        --override_config homestore_config.consensus.max_grpc_message_size:138412032
        --override_config homestore_config.consensus.replace_member_sync_check_interval_ms=1000
        --override_config homestore_config.consensus.laggy_threshold=0)
# To test both baseline & incremental resync functionality, we use 13 to minimize the likelihood of it being a divisor of the total LSN (currently 30)
add_test(NAME HomestoreTestReplaceMemberWithBaselineResync
        COMMAND homestore_test_dynamic -csv error --executor immediate --config_path ./
        --override_config homestore_config.consensus.snapshot_freq_distance:13
        --override_config homestore_config.consensus.num_reserved_log_items=13
        --override_config homestore_config.consensus.snapshot_sync_ctx_timeout_ms=5000
        --override_config homestore_config.generic.repl_dev_cleanup_interval_sec=5
        --override_config homestore_config.consensus.max_grpc_message_size:138412032
        --override_config homestore_config.consensus.replace_member_sync_check_interval_ms=1000
        --override_config homestore_config.consensus.laggy_threshold=0
        --gtest_filter=HomeObjectFixture.ReplaceMember)
add_test(NAME HomestoreResyncTestWithFollowerRestart
        COMMAND homestore_test_dynamic -csv error --executor immediate --config_path ./
        --override_config homestore_config.consensus.snapshot_freq_distance:13
        --override_config homestore_config.consensus.num_reserved_log_items=13
        --override_config homestore_config.consensus.snapshot_sync_ctx_timeout_ms=5000
        --override_config homestore_config.generic.repl_dev_cleanup_interval_sec=5
        --override_config homestore_config.consensus.max_grpc_message_size:138412032
        --override_config homestore_config.consensus.replace_member_sync_check_interval_ms=1000
        --override_config homestore_config.consensus.laggy_threshold=0
        --gtest_filter=HomeObjectFixture.RestartFollower*)
add_test(NAME HomestoreResyncTestWithLeaderRestart
        COMMAND homestore_test_dynamic -csv error --executor immediate --config_path ./
        --override_config homestore_config.consensus.snapshot_freq_distance:13
        --override_config homestore_config.consensus.num_reserved_log_items=13
        --override_config homestore_config.consensus.snapshot_sync_ctx_timeout_ms=5000
        --override_config homestore_config.generic.repl_dev_cleanup_interval_sec=5
        --override_config homestore_config.consensus.max_grpc_message_size:138412032
        --override_config homestore_config.consensus.replace_member_sync_check_interval_ms=1000
        --override_config homestore_config.consensus.laggy_threshold=0
        --gtest_filter=HomeObjectFixture.RestartLeader*)

add_executable(homestore_test_gc)
target_sources(homestore_test_gc PRIVATE $<TARGET_OBJECTS:homestore_tests_gc>)
target_link_libraries(homestore_test_gc PUBLIC homeobject_homestore ${COMMON_TEST_DEPS})
add_test(NAME HomestoreTestGC COMMAND homestore_test_pg -csv error --executor immediate --config_path ./
        --override_config homestore_config.consensus.snapshot_freq_distance:0
        --override_config homestore_config.consensus.max_grpc_message_size:138412032)

