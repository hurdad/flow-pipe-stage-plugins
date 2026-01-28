cmake_minimum_required(VERSION 3.20)

project(stage_arrow_proto LANGUAGES CXX)

find_package(Protobuf REQUIRED)

set(PROTO_FILES
        arrow_common.proto
        arrow_fs_azure.proto
        arrow_fs_gcs.proto
        arrow_fs_s3.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(stage_arrow_proto STATIC
        ${PROTO_SRCS}
        ${PROTO_HDRS}
)

target_link_libraries(stage_arrow_proto
        PUBLIC protobuf::libprotobuf
)

target_include_directories(stage_arrow_proto
        PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_features(stage_arrow_proto
        PUBLIC cxx_std_20
)