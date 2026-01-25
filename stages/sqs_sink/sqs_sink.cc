#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

#include "sqs_sink.pb.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <aws/core/Aws.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/SendMessageRequest.h>

#include <string>

#include "aws_sdk_guard.h"

using namespace flowpipe;

using SQSSinkConfig = flowpipe::stages::sqs::sink::v1::SQSSinkConfig;
using flowpipe::stages::util::AwsSdkGuard;

namespace {
Aws::Client::ClientConfiguration BuildClientConfig(const SQSSinkConfig& cfg) {
  Aws::Client::ClientConfiguration client_config;
  if (!cfg.region().empty()) {
    client_config.region = cfg.region();
  }
  if (!cfg.endpoint().empty()) {
    client_config.endpointOverride = cfg.endpoint();
  }
  return client_config;
}

std::unique_ptr<Aws::SQS::SQSClient> BuildSqsClient(
    const SQSSinkConfig& cfg) {
  Aws::Client::ClientConfiguration client_config = BuildClientConfig(cfg);

  if (!cfg.access_key_id().empty() || !cfg.secret_access_key().empty()) {
    if (cfg.access_key_id().empty() || cfg.secret_access_key().empty()) {
      FP_LOG_ERROR("sqs_sink requires both access_key_id and secret_access_key");
      return nullptr;
    }

    Aws::Auth::AWSCredentials credentials(
        cfg.access_key_id(),
        cfg.secret_access_key(),
        cfg.session_token());

    return std::make_unique<Aws::SQS::SQSClient>(credentials, client_config);
  }

  return std::make_unique<Aws::SQS::SQSClient>(client_config);
}
}  // namespace

// ============================================================
// SQSSink
// ============================================================
class SQSSink final
    : public ISinkStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "sqs_sink";
  }

  SQSSink() {
    FP_LOG_INFO("sqs_sink constructed");
  }

  ~SQSSink() override {
    FP_LOG_INFO("sqs_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("sqs_sink failed to serialize config");
      return false;
    }

    SQSSinkConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("sqs_sink invalid config");
      return false;
    }

    if (cfg.queue_url().empty()) {
      FP_LOG_ERROR("sqs_sink requires queue_url");
      return false;
    }

    config_ = std::move(cfg);
    client_ = BuildSqsClient(config_);

    if (!client_) {
      return false;
    }

    FP_LOG_INFO("sqs_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("sqs_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("sqs_sink received empty payload");
      return;
    }

    if (!client_) {
      FP_LOG_ERROR("sqs_sink client not initialized");
      return;
    }

    Aws::SQS::Model::SendMessageRequest request;
    request.SetQueueUrl(config_.queue_url());

    std::string body(payload.begin(), payload.end());
    request.SetMessageBody(body);

    if (!config_.message_group_id().empty()) {
      request.SetMessageGroupId(config_.message_group_id());
    }

    if (!config_.message_deduplication_id().empty()) {
      request.SetMessageDeduplicationId(config_.message_deduplication_id());
    }

    auto outcome = client_->SendMessage(request);
    if (!outcome.IsSuccess()) {
      FP_LOG_ERROR("sqs_sink failed to send message: "
                   + outcome.GetError().GetMessage());
      return;
    }

    FP_LOG_DEBUG("sqs_sink sent message to SQS");
  }

private:
  AwsSdkGuard sdk_guard_{};
  SQSSinkConfig config_{};
  std::unique_ptr<Aws::SQS::SQSClient> client_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating sqs_sink stage");
  return new SQSSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying sqs_sink stage");
  delete stage;
}

}  // extern "C"
