cmake_minimum_required(VERSION 3.13)

include_directories (BEFORE .)

link_directories(${spdk_LIB_DIRS} ${dpdk_LIB_DIRS})

if (NOT ${BUILD_COVERAGE})
add_executable(test_home_object)
target_sources(test_home_object PRIVATE test_home_object.cpp)
target_link_libraries(test_home_object
            homeobject
            ${COMMON_TEST_DEPS}
        )
add_test(NAME HomeObject COMMAND ${CMAKE_BINARY_DIR}/bin/test_home_object)
set_property(TEST HomeObject PROPERTY RUN_SERIAL 1)
endif()
