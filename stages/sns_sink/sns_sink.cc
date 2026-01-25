#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

#include "sns_sink.pb.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <aws/core/Aws.h>
#include <aws/sns/SNSClient.h>
#include <aws/sns/model/PublishRequest.h>

#include <string>

#include "aws_sdk_guard.h"

using namespace flowpipe;

using SNSSinkConfig = flowpipe::stages::sns::sink::v1::SNSSinkConfig;
using flowpipe::stages::util::AwsSdkGuard;

namespace {
Aws::Client::ClientConfiguration BuildClientConfig(const SNSSinkConfig& cfg) {
  Aws::Client::ClientConfiguration client_config;
  if (!cfg.region().empty()) {
    client_config.region = cfg.region();
  }
  if (!cfg.endpoint().empty()) {
    client_config.endpointOverride = cfg.endpoint();
  }
  return client_config;
}

std::unique_ptr<Aws::SNS::SNSClient> BuildSnsClient(
    const SNSSinkConfig& cfg) {
  Aws::Client::ClientConfiguration client_config = BuildClientConfig(cfg);

  if (!cfg.access_key_id().empty() || !cfg.secret_access_key().empty()) {
    if (cfg.access_key_id().empty() || cfg.secret_access_key().empty()) {
      FP_LOG_ERROR("sns_sink requires both access_key_id and secret_access_key");
      return nullptr;
    }

    Aws::Auth::AWSCredentials credentials(
        cfg.access_key_id(),
        cfg.secret_access_key(),
        cfg.session_token());

    return std::make_unique<Aws::SNS::SNSClient>(credentials, client_config);
  }

  return std::make_unique<Aws::SNS::SNSClient>(client_config);
}
}  // namespace

// ============================================================
// SNSSink
// ============================================================
class SNSSink final
    : public ISinkStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "sns_sink";
  }

  SNSSink() {
    FP_LOG_INFO("sns_sink constructed");
  }

  ~SNSSink() override {
    FP_LOG_INFO("sns_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("sns_sink failed to serialize config");
      return false;
    }

    SNSSinkConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("sns_sink invalid config");
      return false;
    }

    if (cfg.topic_arn().empty()) {
      FP_LOG_ERROR("sns_sink requires topic_arn");
      return false;
    }

    config_ = std::move(cfg);
    client_ = BuildSnsClient(config_);

    if (!client_) {
      return false;
    }

    FP_LOG_INFO("sns_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("sns_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("sns_sink received empty payload");
      return;
    }

    if (!client_) {
      FP_LOG_ERROR("sns_sink client not initialized");
      return;
    }

    Aws::SNS::Model::PublishRequest request;
    request.SetTopicArn(config_.topic_arn());

    std::string body(payload.begin(), payload.end());
    request.SetMessage(body);

    if (!config_.subject().empty()) {
      request.SetSubject(config_.subject());
    }

    if (!config_.message_structure().empty()) {
      request.SetMessageStructure(config_.message_structure());
    }

    auto outcome = client_->Publish(request);
    if (!outcome.IsSuccess()) {
      FP_LOG_ERROR("sns_sink failed to publish message: "
                   + outcome.GetError().GetMessage());
      return;
    }

    FP_LOG_DEBUG("sns_sink published message to SNS");
  }

private:
  AwsSdkGuard sdk_guard_{};
  SNSSinkConfig config_{};
  std::unique_ptr<Aws::SNS::SNSClient> client_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating sns_sink stage");
  return new SNSSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying sns_sink stage");
  delete stage;
}

}  // extern "C"
