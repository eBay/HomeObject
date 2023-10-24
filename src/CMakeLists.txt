cmake_minimum_required (VERSION 3.11)

add_flags("-Wall -Wextra -Werror -Wpedantic")
include_directories (BEFORE include/)
include_directories (AFTER .)

find_package(Threads QUIET REQUIRED)
find_package(homestore QUIET REQUIRED)

list(APPEND COMMON_DEPS homestore::homestore)

if(BUILD_TESTING)
# This is a work-around for not being able to specify the link
# order in a conan recipe. We link these explicitly and thus
# need to specify the LINK path. They should only be needed
# to build a DSO (test executable) however.
link_directories(${spdk_LIB_DIRS} ${dpdk_LIB_DIRS})
list(APPEND COMMON_TEST_DEPS
    ${COMMON_DEPS}
    GTest::gmock
    ${spdk_LIBRARY_LIST}
    ${dpdk_LIBRARY_LIST}
)
endif()

add_subdirectory(lib)
