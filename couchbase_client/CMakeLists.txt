add_subdirectory(util)

aux_source_directory(. couchbase_client_SRCS)

include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_library(couchbase_client_lib ${couchbase_client_SRCS})

target_link_libraries(
  couchbase_client_lib couchbase
  pthread rt
  boost_thread boost_system
)
