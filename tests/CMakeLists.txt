cmake_minimum_required(VERSION 3.20)

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
  FetchContent_MakeAvailable(googletest)
  set(GTEST_MAIN_TARGET gtest_main)
endif()

add_executable(arrow_stage_tests
  csv_arrow_sink_tests.cc
  csv_arrow_source_tests.cc
  json_arrow_source_tests.cc
  parquet_arrow_sink_tests.cc
  parquet_arrow_source_tests.cc
)
target_include_directories(arrow_stage_tests
  PRIVATE
  /opt/flow-pipe/include
  ${CMAKE_CURRENT_SOURCE_DIR}/..
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
