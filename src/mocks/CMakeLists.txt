cmake_minimum_required (VERSION 3.11)

add_library (home_replication_mock)
target_sources(home_replication_mock PRIVATE
		repl_service.cpp
	)
target_link_libraries(home_replication_mock
            ${COMMON_DEPS}
        )

add_subdirectory(tests)
