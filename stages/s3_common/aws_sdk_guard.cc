#include "aws_sdk_guard.h"

namespace flowpipe::stages::s3 {

AwsSdkGuard::AwsSdkGuard() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (ref_count_++ == 0) {
    Aws::InitAPI(options_);
  }
}

AwsSdkGuard::~AwsSdkGuard() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (--ref_count_ == 0) {
    Aws::ShutdownAPI(options_);
  }
}

std::mutex AwsSdkGuard::mutex_{};
int AwsSdkGuard::ref_count_ = 0;
Aws::SDKOptions AwsSdkGuard::options_{};

}  // namespace flowpipe::stages::s3
