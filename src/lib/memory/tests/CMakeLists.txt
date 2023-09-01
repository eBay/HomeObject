cmake_minimum_required (VERSION 3.11)

find_package(GTest REQUIRED)

include_directories(BEFORE ${homeobject_INCLUDE_DIRS})

add_executable (pg_memory_test)
target_sources(pg_memory_test PRIVATE PGManagerTest.cpp)
target_link_libraries(pg_memory_test
    homeobject_memory
    ${COMMON_TEST_DEPS}
  )
add_test(NAME PGMemoryTest COMMAND pg_memory_test -csv error)

add_executable (shard_memory_test)
target_sources(shard_memory_test PRIVATE ShardManagerTest.cpp)
target_link_libraries(shard_memory_test
    homeobject_memory
    ${COMMON_TEST_DEPS}
  )
add_test(NAME ShardMemoryTest COMMAND shard_memory_test -csv error)

add_executable (blob_memory_test)
target_sources(blob_memory_test PRIVATE BlobManagerTest.cpp)
target_link_libraries(blob_memory_test
    homeobject_memory
    ${COMMON_TEST_DEPS}
  )
add_test(NAME BlobMemoryTest COMMAND blob_memory_test -csv error)
