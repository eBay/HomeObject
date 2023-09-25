cmake_minimum_required (VERSION 3.11)


add_library ("${PROJECT_NAME}_homestore")
target_sources("${PROJECT_NAME}_homestore" PRIVATE
        homeobject.cpp
        pg_manager.cpp
        blob_manager.cpp
        shard_manager.cpp
        heap_chunk_selector.cpp
        replication_state_machine.cpp
        $<TARGET_OBJECTS:${PROJECT_NAME}_core>
	)
target_link_libraries("${PROJECT_NAME}_homestore"
            ${COMMON_DEPS}
        )

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
