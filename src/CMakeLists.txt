cmake_minimum_required (VERSION 3.11)

find_package(Threads QUIET REQUIRED)
find_package(sisl QUIET REQUIRED)
find_package(homestore QUIET REQUIRED)
#find_package(home_replication QUIET REQUIRED)

find_package(GTest QUIET REQUIRED)

set (COMMON_DEPS
        sisl::sisl
    )

set(COMMON_TEST_DEPS
        ${COMMON_DEPS}
	GTest::gmock
    )
add_flags("-Wall -Wextra -Werror -Wpedantic -Wno-unused-parameter")

include_directories (BEFORE .)
include_directories (BEFORE include/)
include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR}/lib)

add_subdirectory(lib)
add_subdirectory(mocks)
add_subdirectory(tests)
