include_guard(GLOBAL)

find_package(AWSSDK QUIET COMPONENTS core s3)

if(NOT AWSSDK_FOUND)
  include(FetchContent)

  set(BUILD_ONLY "s3" CACHE STRING "AWS SDK components to build" FORCE)
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build AWS SDK as static libraries" FORCE)
  set(ENABLE_TESTING OFF CACHE BOOL "Disable AWS SDK tests" FORCE)
  set(AUTORUN_UNIT_TESTS OFF CACHE BOOL "Disable AWS SDK unit tests" FORCE)

  FetchContent_Declare(
    aws-sdk-cpp
    GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
    GIT_TAG 1.11.734
  )
  FetchContent_MakeAvailable(aws-sdk-cpp)

  if(TARGET aws-cpp-sdk-core AND NOT TARGET AWS::core)
    add_library(AWS::core ALIAS aws-cpp-sdk-core)
  endif()
  if(TARGET aws-cpp-sdk-s3 AND NOT TARGET AWS::s3)
    add_library(AWS::s3 ALIAS aws-cpp-sdk-s3)
  endif()
endif()

if(NOT TARGET AWS::core)
  message(FATAL_ERROR "AWS SDK core target is not available. Install the AWS SDK for C++ or enable the FetchContent fallback.")
endif()
if(NOT TARGET AWS::s3)
  message(FATAL_ERROR "AWS SDK s3 target is not available. Install the AWS SDK for C++ or enable the FetchContent fallback.")
endif()
