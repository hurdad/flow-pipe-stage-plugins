cmake_minimum_required(VERSION 3.20)

project(stage_tcp_sink LANGUAGES CXX)

# ------------------------------------------------------------
# Locate installed Flow-Pipe runtime
# ------------------------------------------------------------
list(APPEND CMAKE_PREFIX_PATH "/opt/flow-pipe")

# ------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------
find_package(flowpipe REQUIRED)
find_package(Protobuf REQUIRED)

# ------------------------------------------------------------
# Proto
# ------------------------------------------------------------
set(PROTO_FILES
        tcp_sink.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(stage_tcp_sink_proto STATIC
        ${PROTO_SRCS}
        ${PROTO_HDRS}
)

target_link_libraries(stage_tcp_sink_proto
        PUBLIC protobuf::libprotobuf
)

target_include_directories(stage_tcp_sink_proto
        PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_features(stage_tcp_sink_proto
        PUBLIC cxx_std_20
)

# ------------------------------------------------------------
# Stage plugin
# ------------------------------------------------------------
add_library(stage_tcp_sink SHARED
        tcp_sink.cc
)

# Flow-Pipe public headers
# ------------------------------------------------------------

target_include_directories(stage_tcp_sink
        PRIVATE
        /opt/flow-pipe/include
)

# ------------------------------------------------------------
# Link against runtime shared library
# ------------------------------------------------------------

target_link_libraries(stage_tcp_sink
        PRIVATE
        flowpipe::flowpipe_runtime
        stage_tcp_sink_proto
)

# ------------------------------------------------------------
# C++ standard
# ------------------------------------------------------------

target_compile_features(stage_tcp_sink
        PRIVATE
        cxx_std_20
)

# ------------------------------------------------------------
# Warnings (recommended)
# ------------------------------------------------------------

target_compile_options(stage_tcp_sink
        PRIVATE
        -Wall -Wextra -Wpedantic
)

# ------------------------------------------------------------
# Hide all symbols by default (ABI safety)
# ------------------------------------------------------------

set_target_properties(stage_tcp_sink PROPERTIES
        OUTPUT_NAME stage_tcp_sink
        PREFIX "lib"
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN YES
        INSTALL_RPATH "$ORIGIN/../lib"
)

# ------------------------------------------------------------
# Install
# ------------------------------------------------------------

install(TARGETS stage_tcp_sink
        LIBRARY DESTINATION /opt/flow-pipe/plugins
)
