cmake_minimum_required (VERSION 3.11)

add_library (test_fixture OBJECT)
target_sources(test_fixture PRIVATE
    BlobManagerTest.cpp
    ShardManagerTest.cpp
    PGManagerTest.cpp
    fixture_app.cpp
)
target_link_libraries(test_fixture
    ${COMMON_TEST_DEPS}
)
