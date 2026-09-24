cmake_minimum_required (VERSION 3.11)

add_flags("-Wall -Wextra -Werror -Wpedantic")
include_directories (BEFORE include/)
include_directories (AFTER .)

find_package(Threads QUIET REQUIRED)
find_package(sisl QUIET REQUIRED)
if (NOT HOMEOBJECT_MEMORY_ONLY)
    find_package(homestore QUIET REQUIRED)
endif()

list(APPEND COMMON_DEPS sisl::sisl)

list(APPEND COMMON_TEST_DEPS
    ${COMMON_DEPS}
    GTest::gmock
)

add_subdirectory(lib)
