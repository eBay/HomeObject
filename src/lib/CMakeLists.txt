cmake_minimum_required (VERSION 3.11)

add_flags("-Wno-unused-parameter")

add_library(${PROJECT_NAME}_core OBJECT)
target_sources(${PROJECT_NAME}_core PRIVATE
    homeobject_impl.cpp
    blob_manager.cpp
    shard_manager.cpp
    pg_manager.cpp
)
target_link_libraries(${PROJECT_NAME}_core
    ${COMMON_DEPS}
)

list(GET sisl_INCLUDE_DIRS 0 SISL_TOP_INCLUDE)
include ("${SISL_TOP_INCLUDE}/../cmake/settings_gen.cmake")

option(HOMEOBJECT_MEMORY_ONLY "Build memory backend only (skip homestore_backend)" OFF)

add_subdirectory(memory_backend)
if (NOT HOMEOBJECT_MEMORY_ONLY)
    add_subdirectory(homestore_backend)
endif()
add_subdirectory(tests)
