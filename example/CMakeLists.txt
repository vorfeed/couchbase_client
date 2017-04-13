aux_source_directory(. example_SRCS)

include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_library(example_lib ${example_SRCS})

target_link_libraries(
  example_lib couchbase_client_lib
  pthread rt
  boost_thread boost_system
)

add_executable(example example.cpp)
target_link_libraries(example example_lib)
