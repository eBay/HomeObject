cmake_minimum_required (VERSION 3.11)

add_library ("${PROJECT_NAME}_memory")
target_sources("${PROJECT_NAME}_memory" PRIVATE
    mem_homeobject.cpp
		mem_blob_manager.cpp
		mem_shard_manager.cpp
    mem_pg_manager.cpp
    $<TARGET_OBJECTS:${PROJECT_NAME}_core>
)
target_link_libraries("${PROJECT_NAME}_memory"
            ${COMMON_DEPS}
)

if(BUILD_TESTING)
add_executable (pg_memory_test)
target_sources(pg_memory_test PRIVATE $<TARGET_OBJECTS:pg_test>)
target_link_libraries(pg_memory_test
    homeobject_memory
    ${COMMON_TEST_DEPS}
  )
add_test(NAME PGMemoryTest COMMAND pg_memory_test -csv error)

add_executable (shard_memory_test)
target_sources(shard_memory_test PRIVATE $<TARGET_OBJECTS:shard_test>)
target_link_libraries(shard_memory_test
    homeobject_memory
    ${COMMON_TEST_DEPS}
  )
add_test(NAME ShardMemoryTest COMMAND shard_memory_test -csv error)

add_executable (blob_memory_test)
target_sources(blob_memory_test PRIVATE $<TARGET_OBJECTS:blob_test>)
target_link_libraries(blob_memory_test
    homeobject_memory
    ${COMMON_TEST_DEPS}
  )
add_test(NAME BlobMemoryTestCPU COMMAND blob_memory_test -csv error --num_iters 20000 --executor cpu)
add_test(NAME BlobMemoryTestImmediate COMMAND blob_memory_test -csv error --executor io)
endif()
