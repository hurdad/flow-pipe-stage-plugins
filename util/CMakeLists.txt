cmake_minimum_required(VERSION 3.20)

project(stage_util LANGUAGES CXX)

add_library(stage_util SHARED
        aws_sdk_guard.cc
)

target_include_directories(stage_util
        PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
)

target_link_libraries(stage_util
        PUBLIC
        AWS::core
)

target_compile_features(stage_util
        PRIVATE
        cxx_std_20
)

target_compile_options(stage_util
        PRIVATE
        -Wall -Wextra -Wpedantic
)

install(TARGETS stage_util
        LIBRARY DESTINATION /opt/flow-pipe/lib
)
