cmake_minimum_required (VERSION 3.11)

# add warning options
add_flags("-Wall -Wextra -Werror -Wpedantic")

add_library(${PROJECT_NAME})
target_sources(${PROJECT_NAME} PRIVATE
		homeobject.cpp
        )
target_link_libraries(${PROJECT_NAME}
            ${COMMON_DEPS}
        )
