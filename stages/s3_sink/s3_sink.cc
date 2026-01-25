#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

#include "s3_sink.pb.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <mutex>
#include <string>

#include "aws_sdk_guard.h"

using namespace flowpipe;

using S3SinkConfig = flowpipe::stages::s3::sink::v1::S3SinkConfig;
using flowpipe::stages::util::AwsSdkGuard;

namespace {
Aws::Client::ClientConfiguration BuildClientConfig(const S3SinkConfig& cfg) {
  Aws::Client::ClientConfiguration client_config;
  if (!cfg.region().empty()) {
    client_config.region = cfg.region();
  }
  if (!cfg.endpoint().empty()) {
    client_config.endpointOverride = cfg.endpoint();
  }
  return client_config;
}

std::unique_ptr<Aws::S3::S3Client> BuildS3Client(const S3SinkConfig& cfg) {
  Aws::Client::ClientConfiguration client_config = BuildClientConfig(cfg);
  const bool use_virtual_addressing = !cfg.use_path_style();

  if (!cfg.access_key_id().empty() || !cfg.secret_access_key().empty()) {
    if (cfg.access_key_id().empty() || cfg.secret_access_key().empty()) {
      FP_LOG_ERROR("s3_sink requires both access_key_id and secret_access_key");
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
// S3Sink
// ============================================================
class S3Sink final
    : public ISinkStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "s3_sink";
  }

  S3Sink() {
    FP_LOG_INFO("s3_sink constructed");
  }

  ~S3Sink() override {
    FP_LOG_INFO("s3_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("s3_sink failed to serialize config");
      return false;
    }

    S3SinkConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("s3_sink invalid config");
      return false;
    }

    if (cfg.bucket().empty() || cfg.key().empty()) {
      FP_LOG_ERROR("s3_sink requires bucket and key");
      return false;
    }

    config_ = std::move(cfg);
    client_ = BuildS3Client(config_);

    if (!client_) {
      return false;
    }

    FP_LOG_INFO("s3_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("s3_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("s3_sink received empty payload");
      return;
    }

    if (!client_) {
      FP_LOG_ERROR("s3_sink client not initialized");
      return;
    }

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(config_.bucket());
    request.SetKey(config_.key());

    if (!config_.content_type().empty()) {
      request.SetContentType(config_.content_type());
    }

    auto body = Aws::MakeShared<Aws::StringStream>("s3_sink_body");
    body->write(reinterpret_cast<const char*>(payload.data()),
                static_cast<std::streamsize>(payload.size()));
    request.SetBody(body);

    auto outcome = client_->PutObject(request);
    if (!outcome.IsSuccess()) {
      FP_LOG_ERROR("s3_sink failed to upload object: "
                   + outcome.GetError().GetMessage());
      return;
    }

    FP_LOG_DEBUG("s3_sink uploaded object to S3");
  }

private:
  AwsSdkGuard sdk_guard_{};
  S3SinkConfig config_{};
  std::unique_ptr<Aws::S3::S3Client> client_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating s3_sink stage");
  return new S3Sink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying s3_sink stage");
  delete stage;
}

}  // extern "C"
