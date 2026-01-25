cmake_minimum_required(VERSION 3.20)

project(stage_sns_common LANGUAGES CXX)

# ------------------------------------------------------------
# Shared AWS SDK guard
# ------------------------------------------------------------
add_library(stage_sns_common SHARED
        aws_sdk_guard.cc
)

target_include_directories(stage_sns_common
        PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
)

target_link_libraries(stage_sns_common
        PUBLIC
        AWS::core
)

target_compile_features(stage_sns_common
        PRIVATE
        cxx_std_20
)

# ------------------------------------------------------------
# Warnings (recommended)
# ------------------------------------------------------------
target_compile_options(stage_sns_common
        PRIVATE
        -Wall -Wextra -Wpedantic
)

# ------------------------------------------------------------
# Install
# ------------------------------------------------------------
install(TARGETS stage_sns_common
        LIBRARY DESTINATION /opt/flow-pipe/lib
)
