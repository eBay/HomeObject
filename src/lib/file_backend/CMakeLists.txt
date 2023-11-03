cmake_minimum_required (VERSION 3.11)

add_library ("${PROJECT_NAME}_file")
target_sources("${PROJECT_NAME}_file" PRIVATE
		file_homeobject.cpp
		file_blob_manager.cpp
		file_shard_manager.cpp
		file_pg_manager.cpp
        $<TARGET_OBJECTS:${PROJECT_NAME}_core>
	)
target_link_libraries("${PROJECT_NAME}_file"
            ${COMMON_DEPS}
        )

if(BUILD_TESTING)
add_executable (file_test)
target_sources(file_test PRIVATE
    $<TARGET_OBJECTS:test_fixture>
)
target_link_libraries(file_test
    homeobject_file
    ${COMMON_TEST_DEPS}
)
add_test(NAME FileTestCPU COMMAND memory_test -csv error --executor cpu --num_iters 10000)
add_test(NAME FileTestIO COMMAND memory_test -csv error --executor io --num_iters 10000)
endif()
