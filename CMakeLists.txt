cmake_minimum_required(VERSION 3.20)
project(flow_pipe_stage_plugins LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
enable_testing()

# ------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------
find_package(Arrow REQUIRED)
find_package(flowpipe REQUIRED)
find_package(Protobuf REQUIRED)

# ------------------------------------------------------------
# Formatting
# ------------------------------------------------------------
find_program(CLANG_FORMAT_EXE NAMES clang-format)
if(CLANG_FORMAT_EXE)
  file(GLOB_RECURSE CLANG_FORMAT_FILES
    CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/stages/*.h
    ${CMAKE_CURRENT_SOURCE_DIR}/stages/*.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/stages/*.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/stages/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/util/*.h
    ${CMAKE_CURRENT_SOURCE_DIR}/util/*.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/util/*.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/util/*.cpp
  )
  add_custom_target(format
    COMMAND ${CLANG_FORMAT_EXE} -i --style=file ${CLANG_FORMAT_FILES}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running clang-format on source files"
    VERBATIM
  )
else()
  message(STATUS "clang-format not found; format target will be unavailable.")
endif()

# Build all plugin stages
add_subdirectory(proto/arrow)
add_subdirectory(stages/kafka_sink)
add_subdirectory(stages/kafka_source)
add_subdirectory(stages/csv_arrow_sink)
add_subdirectory(stages/csv_arrow_source)
add_subdirectory(stages/parquet_arrow_sink)
add_subdirectory(stages/parquet_arrow_source)
add_subdirectory(stages/orc_arrow_sink)
add_subdirectory(stages/orc_arrow_source)
add_subdirectory(stages/json_arrow_source)
add_subdirectory(stages/file_sink)
add_subdirectory(stages/file_source)
add_subdirectory(stages/opencv_dnn_inference_transform)
add_subdirectory(stages/opencv_resize_letterbox_transform)
add_subdirectory(stages/redis_pubsub_sink)
add_subdirectory(stages/redis_pubsub_source)
add_subdirectory(stages/redis_stream_sink)
add_subdirectory(stages/redis_stream_source)
add_subdirectory(stages/udp_sink)
add_subdirectory(stages/udp_source)
add_subdirectory(stages/tcp_sink)
add_subdirectory(stages/tcp_source)
add_subdirectory(stages/http_sink)
add_subdirectory(stages/nats_sink)
add_subdirectory(stages/nats_source)
add_subdirectory(stages/nats_jetstream_sink)
add_subdirectory(stages/nats_jetstream_source)

# ------------------------------------------------------------
# Tests
# ------------------------------------------------------------
find_package(GTest QUIET)
if(GTest_FOUND)
  set(GTEST_MAIN_TARGET GTest::gtest_main)
else()
  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
  )
  # For Windows builds inside FetchContent
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  set(GTEST_MAIN_TARGET gtest_main)
endif()

add_executable(arrow_stage_tests
  tests/arrow_stage_tests.cc
)
target_include_directories(arrow_stage_tests
  PRIVATE
  /opt/flow-pipe/include
  ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(arrow_stage_tests
  PRIVATE
  Arrow::arrow_shared
  flowpipe::flowpipe_runtime
  ${GTEST_MAIN_TARGET}
  stage_arrow_proto
  stage_csv_arrow_source_proto
  stage_csv_arrow_sink_proto
  stage_json_arrow_source_proto
  stage_parquet_arrow_source_proto
  stage_parquet_arrow_sink_proto
)
target_compile_features(arrow_stage_tests
  PRIVATE
  cxx_std_20
)

add_test(NAME arrow_stage_tests COMMAND arrow_stage_tests)
