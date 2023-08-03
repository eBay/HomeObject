cmake_minimum_required (VERSION 3.11)

add_compile_options(-Wno-unused-parameter)

add_library (mock_homeobject OBJECT)
target_sources(mock_homeobject PRIVATE
		mock_blob_manager.cpp
		mock_pg_manager.cpp
		mock_shard_manager.cpp
	)
target_link_libraries(mock_homeobject
    homeobject::homeobject
    nuobject_proto::nuobject_proto
    sisl::sisl
  )
