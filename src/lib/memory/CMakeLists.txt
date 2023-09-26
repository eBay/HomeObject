cmake_minimum_required (VERSION 3.11)

add_library ("${PROJECT_NAME}_memory")
target_sources("${PROJECT_NAME}_memory" PRIVATE
		homeobject.cpp
		blob_manager.cpp
		shard_manager.cpp
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
add_test(NAME BlobMemoryTest COMMAND blob_memory_test -csv error)
endif()
