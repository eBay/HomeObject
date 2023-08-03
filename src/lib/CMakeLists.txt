cmake_minimum_required (VERSION 3.11)

add_library(${PROJECT_NAME})
target_sources(${PROJECT_NAME} PRIVATE
		blob_manager.cpp
		shard_manager.cpp
		pg_manager.cpp
        )
target_link_libraries(${PROJECT_NAME}
            ${COMMON_DEPS}
        )
