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
add_executable (pg_file_test)
target_sources(pg_file_test PRIVATE $<TARGET_OBJECTS:pg_test>)
target_link_libraries(pg_file_test
    homeobject_file
    ${COMMON_TEST_DEPS}
  )
add_test(NAME PGFileTest COMMAND pg_file_test -csv error)

add_executable (shard_file_test)
target_sources(shard_file_test PRIVATE $<TARGET_OBJECTS:shard_test>)
target_link_libraries(shard_file_test
    homeobject_file
    ${COMMON_TEST_DEPS}
  )
add_test(NAME ShardFileTest COMMAND shard_file_test -csv error)

add_executable (blob_file_test)
target_sources(blob_file_test PRIVATE $<TARGET_OBJECTS:blob_test>)
target_link_libraries(blob_file_test
    homeobject_file
    ${COMMON_TEST_DEPS}
  )
add_test(NAME BlobFileTest COMMAND blob_file_test -csv error)
endif()
