cmake_minimum_required(VERSION 3.20)

project(stage_sqs_common LANGUAGES CXX)

# ------------------------------------------------------------
# Shared AWS SDK guard
# ------------------------------------------------------------
add_library(stage_sqs_common SHARED
        aws_sdk_guard.cc
)

target_include_directories(stage_sqs_common
        PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
)

target_link_libraries(stage_sqs_common
        PRIVATE
        AWS::core
)

target_compile_features(stage_sqs_common
        PRIVATE
        cxx_std_20
)

# ------------------------------------------------------------
# Warnings (recommended)
# ------------------------------------------------------------
target_compile_options(stage_sqs_common
        PRIVATE
        -Wall -Wextra -Wpedantic
)

# ------------------------------------------------------------
# Install
# ------------------------------------------------------------
install(TARGETS stage_sqs_common
        LIBRARY DESTINATION /opt/flow-pipe/lib
)
