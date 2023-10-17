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
endif()
