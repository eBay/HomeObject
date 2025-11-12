cmake_minimum_required (VERSION 3.11)
project (homeobject LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)

if (NOT DEFINED CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE "Debug")
endif()

include (cmake/Flags.cmake)
enable_testing()
if ((DEFINED CODE_COVERAGE) AND (${CODE_COVERAGE}))
  include (cmake/CodeCoverage.cmake)
  APPEND_COVERAGE_COMPILER_FLAGS()
elseif ((DEFINED MEMORY_SANITIZER_ON) AND (${MEMORY_SANITIZER_ON}))
  message(WARNING "********* Running with Memory Sanitizer *********")
  add_flags("-fsanitize=address                 \
             -fsanitize=undefined               \
             -fsanitize-address-use-after-scope \
             -fno-sanitize=alignment            \
             -DCDS_ADDRESS_SANITIZER_ENABLED    \
             -fno-omit-frame-pointer            \
             -fno-optimize-sibling-calls        "
             )
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")
endif()
find_package(GTest QUIET REQUIRED)

find_program(CCACHE_FOUND ccache QUIET)
if (CCACHE_FOUND)
   set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
   set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif ()

# add component version information
add_flags("-DPACKAGE_NAME=\\\"${PROJECT_NAME}\\\"")
message(STATUS "Conan package version: ${CONAN_PACKAGE_VERSION}")
if (NOT DEFINED CONAN_PACKAGE_VERSION)
   set(CONAN_PACKAGE_VERSION "0.0.0")
endif ()
add_flags("-DPACKAGE_VERSION=\\\"${CONAN_PACKAGE_VERSION}\\\"")

add_subdirectory(src)

# build info
string(TOUPPER "${CMAKE_BUILD_TYPE}" UC_CMAKE_BUILD_TYPE)
message(STATUS "Build configuration:       ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ compiler info:         ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION} ${CMAKE_CXX_COMPILER_EXTERNAL_TOOLCHAIN}")
message(STATUS "C flags:                   ${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
message(STATUS "C++ flags:                 ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
message(STATUS "Linker flags (executable): ${CMAKE_EXE_LINKER_FLAGS} ${CMAKE_EXE_LINKER_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
message(STATUS "Linker flags (shared):     ${CMAKE_SHARED_LINKER_FLAGS} ${CMAKE_SHARED_LINKER_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
message(STATUS "Linker flags (module):     ${CMAKE_MODULE_LINKER_FLAGS} ${CMAKE_MODULE_LINKER_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
message(STATUS "Linker flags (static):     ${CMAKE_STATIC_LINKER_FLAGS} ${CMAKE_STATIC_LINKER_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
