cmake_minimum_required (VERSION 3.11)

find_package(GTest REQUIRED)

include_directories(BEFORE ${homeobject_INCLUDE_DIRS})

add_executable (pg_mock_test)
target_sources(pg_mock_test PRIVATE PGManagerTest.cpp)
target_link_libraries(pg_mock_test
    homeobject_mock
    sisl::sisl
    GTest::gmock
  )
add_test(NAME PGMockTest COMMAND pg_mock_test -csv critical)

add_executable (shard_mock_test)
target_sources(shard_mock_test PRIVATE ShardManagerTest.cpp)
target_link_libraries(shard_mock_test
    homeobject_mock
    sisl::sisl
    GTest::gmock
  )
add_test(NAME ShardMockTest COMMAND shard_mock_test -csv critical)
