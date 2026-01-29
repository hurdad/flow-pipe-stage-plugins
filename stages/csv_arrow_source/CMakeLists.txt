cmake_minimum_required(VERSION 3.20)

project(stage_csv_arrow_source LANGUAGES CXX)

# ------------------------------------------------------------
# Locate installed Flow-Pipe runtime
# ------------------------------------------------------------
list(APPEND CMAKE_PREFIX_PATH "/opt/flow-pipe")

# ------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------
find_package(Arrow REQUIRED)
find_package(flowpipe REQUIRED)
find_package(Protobuf REQUIRED)

# ------------------------------------------------------------
# Proto
# ------------------------------------------------------------
set(PROTO_FILES
        csv_arrow_source.proto
)

set(PROTOBUF_IMPORT_DIRS
        ${CMAKE_CURRENT_SOURCE_DIR}/../../proto/arrow
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(stage_csv_arrow_source_proto STATIC
        ${PROTO_SRCS}
        ${PROTO_HDRS}
)

target_link_libraries(stage_csv_arrow_source_proto
        PUBLIC protobuf::libprotobuf
        stage_arrow_proto
)

target_include_directories(stage_csv_arrow_source_proto
        PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_features(stage_csv_arrow_source_proto
        PUBLIC cxx_std_20
)

# ------------------------------------------------------------
# Stage plugin
# ------------------------------------------------------------
add_library(stage_csv_arrow_source SHARED
        csv_arrow_source.cc
)

# Flow-Pipe public headers
target_include_directories(stage_csv_arrow_source
        PRIVATE
        /opt/flow-pipe/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../..
)

# ------------------------------------------------------------
# Link against runtime shared library
# ------------------------------------------------------------
target_link_libraries(stage_csv_arrow_source
        PRIVATE
        Arrow::arrow_shared
        flowpipe::flowpipe_runtime
        stage_csv_arrow_source_proto
)

# ------------------------------------------------------------
# C++ standard
# ------------------------------------------------------------
target_compile_features(stage_csv_arrow_source
        PRIVATE
        cxx_std_20
)

# ------------------------------------------------------------
# Warnings (recommended)
# ------------------------------------------------------------
target_compile_options(stage_csv_arrow_source
        PRIVATE
        -Wall -Wextra -Wpedantic
)

# ------------------------------------------------------------
# Hide all symbols by default (ABI safety)
# ------------------------------------------------------------
set_target_properties(stage_csv_arrow_source PROPERTIES
        OUTPUT_NAME stage_csv_arrow_source
        PREFIX "lib"
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN YES

        # Ensure runtime .so is found relative to plugins dir
        INSTALL_RPATH "$ORIGIN/../lib"
)

# ------------------------------------------------------------
# Install
# ------------------------------------------------------------
install(TARGETS stage_csv_arrow_source
        LIBRARY DESTINATION /opt/flow-pipe/plugins
)
