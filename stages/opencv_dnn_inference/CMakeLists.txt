cmake_minimum_required(VERSION 3.20)

project(stage_opencv_dnn_inference LANGUAGES CXX)

# ------------------------------------------------------------
# Locate installed Flow-Pipe runtime
# ------------------------------------------------------------
list(APPEND CMAKE_PREFIX_PATH "/opt/flow-pipe")

# ------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------
find_package(flowpipe REQUIRED)
find_package(Protobuf REQUIRED)
find_package(OpenCV REQUIRED COMPONENTS core dnn imgcodecs imgproc)

# ------------------------------------------------------------
# Proto
# ------------------------------------------------------------
set(PROTO_FILES
        opencv_dnn_inference.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(stage_opencv_dnn_inference_proto STATIC
        ${PROTO_SRCS}
        ${PROTO_HDRS}
)

target_link_libraries(stage_opencv_dnn_inference_proto
        PUBLIC protobuf::libprotobuf
)

target_include_directories(stage_opencv_dnn_inference_proto
        PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_features(stage_opencv_dnn_inference_proto
        PUBLIC cxx_std_20
)

# ------------------------------------------------------------
# Stage plugin
# ------------------------------------------------------------
add_library(stage_opencv_dnn_inference SHARED
        opencv_dnn_inference.cc
)

# Flow-Pipe public headers
target_include_directories(stage_opencv_dnn_inference
        PRIVATE
        /opt/flow-pipe/include
)

# ------------------------------------------------------------
# Link against runtime shared library
# ------------------------------------------------------------
target_link_libraries(stage_opencv_dnn_inference
        PRIVATE
        flowpipe::flowpipe_runtime
        stage_opencv_dnn_inference_proto
        ${OpenCV_LIBS}
)

# ------------------------------------------------------------
# C++ standard
# ------------------------------------------------------------
target_compile_features(stage_opencv_dnn_inference
        PRIVATE
        cxx_std_20
)

# ------------------------------------------------------------
# Warnings (recommended)
# ------------------------------------------------------------
target_compile_options(stage_opencv_dnn_inference
        PRIVATE
        -Wall -Wextra -Wpedantic
)

# ------------------------------------------------------------
# Hide all symbols by default (ABI safety)
# ------------------------------------------------------------
set_target_properties(stage_opencv_dnn_inference PROPERTIES
        OUTPUT_NAME stage_opencv_dnn_inference
        PREFIX "lib"
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN YES

        # Ensure runtime .so is found relative to plugins dir
        INSTALL_RPATH "$ORIGIN/../lib"
)

# ------------------------------------------------------------
# Install
# ------------------------------------------------------------
install(TARGETS stage_opencv_dnn_inference
        LIBRARY DESTINATION /opt/flow-pipe/plugins
)
