cmake_minimum_required (VERSION 3.11)

add_flags("-Wno-unused-parameter")

add_library(${PROJECT_NAME}_core OBJECT)
target_sources(${PROJECT_NAME}_core PRIVATE
            blob_manager.cpp
            shard_manager.cpp
            pg_manager.cpp
        )
target_link_libraries(${PROJECT_NAME}_core
            ${COMMON_DEPS}
        )

if(BUILD_TESTING)
add_subdirectory(tests)
endif()

add_subdirectory(homestore)
add_subdirectory(memory)
