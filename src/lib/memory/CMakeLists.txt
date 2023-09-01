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
  add_subdirectory(tests)
endif()
