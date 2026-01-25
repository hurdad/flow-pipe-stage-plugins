#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

#include "s3_source.pb.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <mutex>
#include <string>

#include "aws_sdk_guard.h"

using namespace flowpipe;

using S3SourceConfig = flowpipe::stages::s3::source::v1::S3SourceConfig;
using flowpipe::stages::util::AwsSdkGuard;

namespace {
Aws::Client::ClientConfiguration BuildClientConfig(const S3SourceConfig& cfg) {
  Aws::Client::ClientConfiguration client_config;
  if (!cfg.region().empty()) {
    client_config.region = cfg.region();
  }
  if (!cfg.endpoint().empty()) {
    client_config.endpointOverride = cfg.endpoint();
  }
  return client_config;
}

std::unique_ptr<Aws::S3::S3Client> BuildS3Client(const S3SourceConfig& cfg) {
  Aws::Client::ClientConfiguration client_config = BuildClientConfig(cfg);
  const bool use_virtual_addressing = !cfg.use_path_style();

  if (!cfg.access_key_id().empty() || !cfg.secret_access_key().empty()) {
    if (cfg.access_key_id().empty() || cfg.secret_access_key().empty()) {
      FP_LOG_ERROR("s3_source requires both access_key_id and secret_access_key");
      return nullptr;
    }

    Aws::Auth::AWSCredentials credentials(
        cfg.access_key_id(),
        cfg.secret_access_key(),
        cfg.session_token());

    return std::make_unique<Aws::S3::S3Client>(
        credentials,
        client_config,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        use_virtual_addressing);
  }

  return std::make_unique<Aws::S3::S3Client>(
      client_config,
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
      use_virtual_addressing);
}
}  // namespace

// ============================================================
// S3Source
// ============================================================
class S3Source final
    : public ISourceStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "s3_source";
  }

  S3Source() {
    FP_LOG_INFO("s3_source constructed");
  }

  ~S3Source() override {
    FP_LOG_INFO("s3_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("s3_source failed to serialize config");
      return false;
    }

    S3SourceConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("s3_source invalid config");
      return false;
    }

    if (cfg.bucket().empty() || cfg.key().empty()) {
      FP_LOG_ERROR("s3_source requires bucket and key");
      return false;
    }

    config_ = std::move(cfg);
    client_ = BuildS3Client(config_);

    if (!client_) {
      return false;
    }

    produced_ = false;

    FP_LOG_INFO("s3_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("s3_source stop requested, skipping produce");
      return false;
    }

    if (produced_) {
      return false;
    }

    if (!client_) {
      FP_LOG_ERROR("s3_source client not initialized");
      return false;
    }

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(config_.bucket());
    request.SetKey(config_.key());

    auto outcome = client_->GetObject(request);
    if (!outcome.IsSuccess()) {
      FP_LOG_ERROR("s3_source failed to fetch object: "
                   + outcome.GetError().GetMessage());
      return false;
    }

    auto result = outcome.GetResultWithOwnership();
    auto& stream = result.GetBody();
    std::string body((std::istreambuf_iterator<char>(stream)),
                     std::istreambuf_iterator<char>());

    payload.assign(body.begin(), body.end());
    produced_ = true;

    FP_LOG_DEBUG("s3_source produced payload from S3");
    return true;
  }

private:
  AwsSdkGuard sdk_guard_{};
  S3SourceConfig config_{};
  std::unique_ptr<Aws::S3::S3Client> client_{};
  bool produced_{false};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating s3_source stage");
  return new S3Source();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying s3_source stage");
  delete stage;
}

}  // extern "C"
