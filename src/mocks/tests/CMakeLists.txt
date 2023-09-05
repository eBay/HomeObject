cmake_minimum_required (VERSION 3.11)

find_package(GTest REQUIRED)

include_directories(BEFORE ${homeobject_INCLUDE_DIRS})

add_executable (replsvc_mock_test)
target_sources(replsvc_mock_test PRIVATE ReplServiceTest.cpp)
target_link_libraries(replsvc_mock_test
    home_replication_mock
    ${COMMON_TEST_DEPS}
    GTest::gmock
  )
add_test(NAME MockReplServiceTest COMMAND replsvc_mock_test -csv critical)
