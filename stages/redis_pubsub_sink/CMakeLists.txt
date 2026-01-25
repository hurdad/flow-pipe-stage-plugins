cmake_minimum_required(VERSION 3.20)

project(stage_redis_pubsub_sink LANGUAGES CXX)

# ------------------------------------------------------------
# Locate installed Flow-Pipe runtime
# ------------------------------------------------------------
list(APPEND CMAKE_PREFIX_PATH "/opt/flow-pipe")

# ------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------
find_package(flowpipe REQUIRED)
find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)

# ------------------------------------------------------------
# Proto
# ------------------------------------------------------------
set(PROTO_FILES
        redis_pubsub_sink.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(stage_redis_pubsub_sink_proto STATIC
        ${PROTO_SRCS}
        ${PROTO_HDRS}
)

target_link_libraries(stage_redis_pubsub_sink_proto
        PUBLIC protobuf::libprotobuf
)

target_include_directories(stage_redis_pubsub_sink_proto
        PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_features(stage_redis_pubsub_sink_proto
        PUBLIC cxx_std_20
)

# ------------------------------------------------------------
# Stage plugin
# ------------------------------------------------------------
add_library(stage_redis_pubsub_sink SHARED
        redis_pubsub_sink.cc
)

# Flow-Pipe public headers
# ------------------------------------------------------------

target_include_directories(stage_redis_pubsub_sink
        PRIVATE
        /opt/flow-pipe/include
        ${CMAKE_CURRENT_LIST_DIR}/../../util
)

# ------------------------------------------------------------
# Link against runtime shared library
# ------------------------------------------------------------

target_link_libraries(stage_redis_pubsub_sink
        PRIVATE
        flowpipe::flowpipe_runtime
        stage_redis_pubsub_sink_proto
        ${HIREDIS_LIBRARIES}
)

target_include_directories(stage_redis_pubsub_sink
        PRIVATE
        ${HIREDIS_INCLUDE_DIRS}
)

# ------------------------------------------------------------
# C++ standard
# ------------------------------------------------------------

target_compile_features(stage_redis_pubsub_sink
        PRIVATE
        cxx_std_20
)

# ------------------------------------------------------------
# Warnings (recommended)
# ------------------------------------------------------------

target_compile_options(stage_redis_pubsub_sink
        PRIVATE
        -Wall -Wextra -Wpedantic
)

# ------------------------------------------------------------
# Hide all symbols by default (ABI safety)
# ------------------------------------------------------------

set_target_properties(stage_redis_pubsub_sink PROPERTIES
        OUTPUT_NAME stage_redis_pubsub_sink
        PREFIX "lib"
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN YES
        INSTALL_RPATH "$ORIGIN/../lib"
)

# ------------------------------------------------------------
# Install
# ------------------------------------------------------------

install(TARGETS stage_redis_pubsub_sink
        LIBRARY DESTINATION /opt/flow-pipe/plugins
)
