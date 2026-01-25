#pragma once

#include <aws/core/Aws.h>

#include <mutex>

namespace flowpipe::stages::s3 {

class AwsSdkGuard {
public:
  AwsSdkGuard();
  ~AwsSdkGuard();

  AwsSdkGuard(const AwsSdkGuard&) = delete;
  AwsSdkGuard& operator=(const AwsSdkGuard&) = delete;

private:
  static std::mutex mutex_;
  static int ref_count_;
  static Aws::SDKOptions options_;
};

}  // namespace flowpipe::stages::s3
