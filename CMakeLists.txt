cmake_minimum_required(VERSION 3.20)
project(flow_pipe_stage_plugins LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
enable_testing()

# ------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------
include(cmake/AwsSdk.cmake)

find_package(Protobuf REQUIRED)

# Build all plugin stages
add_subdirectory(stages/kafka_sink)
add_subdirectory(stages/kafka_source)
add_subdirectory(util)
add_subdirectory(stages/s3_sink)
add_subdirectory(stages/s3_source)
add_subdirectory(stages/sns_sink)
add_subdirectory(stages/sqs_sink)
add_subdirectory(stages/sqs_source)
