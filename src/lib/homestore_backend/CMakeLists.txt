cmake_minimum_required (VERSION 3.11)

add_library ("${PROJECT_NAME}_homestore")
target_sources("${PROJECT_NAME}_homestore" PRIVATE
    hs_homeobject.cpp
    hs_blob_manager.cpp
    hs_shard_manager.cpp
    hs_pg_manager.cpp
    index_kv.cpp
    heap_chunk_selector.cpp
    replication_state_machine.cpp
    $<TARGET_OBJECTS:${PROJECT_NAME}_core>
)
target_link_libraries("${PROJECT_NAME}_homestore"
    ${COMMON_DEPS}
)

set(FLATBUFFERS_FLATC_EXECUTABLE ${flatbuffers_LIB_DIRS}/../bin/flatc)
settings_gen_cpp(
    ${FLATBUFFERS_FLATC_EXECUTABLE}
    ${CMAKE_CURRENT_BINARY_DIR}/generated/
    "${PROJECT_NAME}_homestore"
    hs_backend_config.fbs
  )

if(BUILD_TESTING)
add_subdirectory(tests)

add_executable (homestore_test)
target_sources(homestore_test PRIVATE
    $<TARGET_OBJECTS:homestore_tests>
    $<TARGET_OBJECTS:test_fixture>
)
target_link_libraries(homestore_test
    homeobject_homestore
    ${COMMON_TEST_DEPS}
)
add_test(NAME HomestoreTest COMMAND homestore_test -csv error --executor immediate)
set_property(TEST HomestoreTest PROPERTY RUN_SERIAL 1)
endif()
