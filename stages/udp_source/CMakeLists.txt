cmake_minimum_required(VERSION 3.20)

project(stage_udp_source LANGUAGES CXX)

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
        udp_source.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(stage_udp_source_proto STATIC
        ${PROTO_SRCS}
        ${PROTO_HDRS}
)

target_link_libraries(stage_udp_source_proto
        PUBLIC protobuf::libprotobuf
)

target_include_directories(stage_udp_source_proto
        PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_features(stage_udp_source_proto
        PUBLIC cxx_std_20
)

# ------------------------------------------------------------
# Stage plugin
# ------------------------------------------------------------
add_library(stage_udp_source SHARED
        udp_source.cc
)

# Flow-Pipe public headers
# ------------------------------------------------------------

target_include_directories(stage_udp_source
        PRIVATE
        /opt/flow-pipe/include
)

# ------------------------------------------------------------
# Link against runtime shared library
# ------------------------------------------------------------

target_link_libraries(stage_udp_source
        PRIVATE
        flowpipe::flowpipe_runtime
        stage_udp_source_proto
)

# ------------------------------------------------------------
# C++ standard
# ------------------------------------------------------------

target_compile_features(stage_udp_source
        PRIVATE
        cxx_std_20
)

# ------------------------------------------------------------
# Warnings (recommended)
# ------------------------------------------------------------

target_compile_options(stage_udp_source
        PRIVATE
        -Wall -Wextra -Wpedantic
)

# ------------------------------------------------------------
# Hide all symbols by default (ABI safety)
# ------------------------------------------------------------

set_target_properties(stage_udp_source PROPERTIES
        OUTPUT_NAME stage_udp_source
        PREFIX "lib"
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN YES
        INSTALL_RPATH "$ORIGIN/../lib"
)

# ------------------------------------------------------------
# Install
# ------------------------------------------------------------

install(TARGETS stage_udp_source
        LIBRARY DESTINATION /opt/flow-pipe/plugins
)
