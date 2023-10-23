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
add_executable (memory_test)
target_sources(memory_test PRIVATE $<TARGET_OBJECTS:test_fixture>)
target_link_libraries(memory_test
    homeobject_memory
    ${COMMON_TEST_DEPS}
  )
add_test(NAME MemoryTestCPU COMMAND memory_test -csv error --executor cpu --num_iters 20000)
add_test(NAME MemoryTestIO COMMAND memory_test -csv error --executor io --num_iters 20000)
endif()
