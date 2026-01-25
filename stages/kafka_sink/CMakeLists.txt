cmake_minimum_required(VERSION 3.20)

project(stage_kafka_sink LANGUAGES CXX)

# ------------------------------------------------------------
# Locate installed Flow-Pipe runtime
# ------------------------------------------------------------
# This assumes the runtime was installed to /opt/flow-pipe
# (as done by your Docker images)
list(APPEND CMAKE_PREFIX_PATH "/opt/flow-pipe")

# ------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------
find_package(flowpipe REQUIRED)
find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka)

# ------------------------------------------------------------
# Proto
# ------------------------------------------------------------
set(PROTO_FILES
        kafka_sink.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(stage_kafka_sink_proto STATIC
        ${PROTO_SRCS}
        ${PROTO_HDRS}
)

target_link_libraries(stage_kafka_sink_proto
        PUBLIC protobuf::libprotobuf
)

target_include_directories(stage_kafka_sink_proto
        PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_features(stage_kafka_sink_proto
        PUBLIC cxx_std_20
)

# ------------------------------------------------------------
# Stage plugin
# ------------------------------------------------------------
add_library(stage_kafka_sink SHARED
        kafka_sink.cc
)

# Flow-Pipe public headers
target_include_directories(stage_kafka_sink
        PRIVATE
        /opt/flow-pipe/include
)

# ------------------------------------------------------------
# Link against runtime shared library
# ------------------------------------------------------------
target_link_libraries(stage_kafka_sink
        PRIVATE
        flowpipe::flowpipe_runtime
        stage_kafka_sink_proto
        ${RDKAFKA_LIBRARIES}
)

target_include_directories(stage_kafka_sink
        PRIVATE
        ${RDKAFKA_INCLUDE_DIRS}
)

# ------------------------------------------------------------
# C++ standard
# ------------------------------------------------------------
target_compile_features(stage_kafka_sink
        PRIVATE
        cxx_std_20
)

# ------------------------------------------------------------
# Warnings (recommended)
# ------------------------------------------------------------
target_compile_options(stage_kafka_sink
        PRIVATE
        -Wall -Wextra -Wpedantic
)

# ------------------------------------------------------------
# Hide all symbols by default (ABI safety)
# ------------------------------------------------------------
set_target_properties(stage_kafka_sink PROPERTIES
        OUTPUT_NAME stage_kafka_sink
        PREFIX "lib"
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN YES

        # Ensure runtime .so is found relative to plugins dir
        INSTALL_RPATH "$ORIGIN/../lib"
)

# ------------------------------------------------------------
# Install
# ------------------------------------------------------------
install(TARGETS stage_kafka_sink
        LIBRARY DESTINATION /opt/flow-pipe/plugins
)
