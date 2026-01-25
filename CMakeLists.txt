cmake_minimum_required(VERSION 3.20)
project(flow_pipe_stage_plugins LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
enable_testing()

# ------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------
include(ExternalProject)

ExternalProject_Add(libawscpp-download
        GIT_REPOSITORY    https://github.com/aws/aws-sdk-cpp.git
        GIT_TAG           1.11.734
        LIST_SEPARATOR    "|"
        CMAKE_ARGS       -DBUILD_SHARED_LIBS=OFF
        -DBUILD_ONLY=s3
        BUILD_ALWAYS      TRUE
        TEST_COMMAND      ""
)

find_package(Protobuf REQUIRED)

# Build all plugin stages
add_subdirectory(stages/kafka_sink)
add_subdirectory(stages/kafka_source)
add_subdirectory(stages/s3_common)
add_subdirectory(stages/s3_sink)
add_subdirectory(stages/s3_source)
