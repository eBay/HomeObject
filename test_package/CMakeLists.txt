cmake_minimum_required(VERSION 3.11)
project(test_package LANGUAGES CXX)

find_package(HomeObject QUIET REQUIRED)

link_directories(${spdk_LIB_DIRS} ${dpdk_LIB_DIRS})

add_executable(test_package)
target_sources(test_package PRIVATE test_package.cpp)
target_compile_features(${PROJECT_NAME} PUBLIC cxx_std_20)
target_link_libraries(${PROJECT_NAME} homeobject::memory
        ${spdk_LIBRARY_LIST}
        ${dpdk_LIBRARY_LIST}
        )
