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

if(BUILD_TESTING)
    add_subdirectory(tests)

add_executable (pg_homestore_test)
target_sources(pg_homestore_test PRIVATE $<TARGET_OBJECTS:pg_test>)
target_link_libraries(pg_homestore_test
    homeobject_homestore
    ${COMMON_TEST_DEPS}
  )
add_test(NAME PGHomestoreTest COMMAND pg_homestore_test -csv error)

add_executable (shard_homestore_test)
target_sources(shard_homestore_test PRIVATE $<TARGET_OBJECTS:shard_test>)
target_link_libraries(shard_homestore_test
    homeobject_homestore
    ${COMMON_TEST_DEPS}
  )
add_test(NAME ShardHomestoreTest COMMAND shard_homestore_test -csv error)

add_executable (blob_homestore_test)
target_sources(blob_homestore_test PRIVATE $<TARGET_OBJECTS:blob_test>)
target_link_libraries(blob_homestore_test
    homeobject_homestore
    ${COMMON_TEST_DEPS}
  )
add_test(NAME BlobHomestoreTest COMMAND blob_homestore_test -csv error --num-iters 100)
endif()
