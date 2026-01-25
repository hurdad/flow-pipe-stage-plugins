cmake_minimum_required(VERSION 3.20)

project(stage_s3_common LANGUAGES CXX)

# ------------------------------------------------------------
# Shared AWS SDK guard
# ------------------------------------------------------------
add_library(stage_s3_common SHARED
        aws_sdk_guard.cc
)

target_include_directories(stage_s3_common
        PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
)

target_link_libraries(stage_s3_common
        PUBLIC
        AWS::core
)

target_compile_features(stage_s3_common
        PRIVATE
        cxx_std_20
)

# ------------------------------------------------------------
# Warnings (recommended)
# ------------------------------------------------------------
target_compile_options(stage_s3_common
        PRIVATE
        -Wall -Wextra -Wpedantic
)

# ------------------------------------------------------------
# Install
# ------------------------------------------------------------
install(TARGETS stage_s3_common
        LIBRARY DESTINATION /opt/flow-pipe/lib
)
