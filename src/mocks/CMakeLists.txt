cmake_minimum_required (VERSION 3.11)

add_library ("${PROJECT_NAME}_mock")
target_sources("${PROJECT_NAME}_mock" PRIVATE
		mock_blob_manager.cpp
		mock_pg_manager.cpp
		mock_shard_manager.cpp
	)
target_link_libraries("${PROJECT_NAME}_mock"
            ${COMMON_DEPS}
        )

add_subdirectory(tests)
