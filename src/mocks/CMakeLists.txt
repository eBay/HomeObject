cmake_minimum_required (VERSION 3.11)

add_library ("${PROJECT_NAME}_mock")
target_sources("${PROJECT_NAME}_mock" PRIVATE
		mock_homeobject.cpp
		mock_blob_manager.cpp
		mock_shard_manager.cpp
        $<TARGET_OBJECTS:${PROJECT_NAME}_core>
	)
target_link_libraries("${PROJECT_NAME}_mock"
            ${COMMON_DEPS}
        )

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
