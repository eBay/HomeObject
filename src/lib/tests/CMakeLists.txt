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

add_executable(test_watchdog_registry)
target_sources(test_watchdog_registry PRIVATE
    test_watchdog_registry.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../watchdog_registry.cpp
)
target_link_libraries(test_watchdog_registry ${COMMON_TEST_DEPS} Threads::Threads)
add_test(NAME WatchdogRegistryTest COMMAND test_watchdog_registry)
