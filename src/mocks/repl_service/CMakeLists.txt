cmake_minimum_required (VERSION 3.11)

add_library ("replication_mock")
target_sources("replication_mock" PRIVATE
		repl_service.cpp
	)
target_link_libraries("replication_mock"
            nuraft_mesg::nuraft_mesg
            ${COMMON_DEPS}
        )
