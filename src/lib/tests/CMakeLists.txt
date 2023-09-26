cmake_minimum_required (VERSION 3.11)

add_library (pg_test OBJECT)
target_sources(pg_test PRIVATE PGManagerTest.cpp)
target_link_libraries(pg_test
    ${COMMON_TEST_DEPS}
  )

add_library (shard_test OBJECT)
target_sources(shard_test PRIVATE ShardManagerTest.cpp)
target_link_libraries(shard_test
    ${COMMON_TEST_DEPS}
  )

add_library (blob_test OBJECT)
target_sources(blob_test PRIVATE BlobManagerTest.cpp)
target_link_libraries(blob_test
    ${COMMON_TEST_DEPS}
  )
