#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

#include "sqs_source.pb.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <aws/core/Aws.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>

#include <deque>
#include <string>

#include "aws_sdk_guard.h"

using namespace flowpipe;

using SQSSourceConfig = flowpipe::stages::sqs::source::v1::SQSSourceConfig;
using flowpipe::stages::util::AwsSdkGuard;

namespace {
Aws::Client::ClientConfiguration BuildClientConfig(const SQSSourceConfig& cfg) {
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
    const SQSSourceConfig& cfg) {
  Aws::Client::ClientConfiguration client_config = BuildClientConfig(cfg);

  if (!cfg.access_key_id().empty() || !cfg.secret_access_key().empty()) {
    if (cfg.access_key_id().empty() || cfg.secret_access_key().empty()) {
      FP_LOG_ERROR("sqs_source requires both access_key_id and secret_access_key");
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

struct PendingMessage {
  std::string body;
  std::string receipt_handle;
};
}  // namespace

// ============================================================
// SQSSource
// ============================================================
class SQSSource final
    : public ISourceStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "sqs_source";
  }

  SQSSource() {
    FP_LOG_INFO("sqs_source constructed");
  }

  ~SQSSource() override {
    FP_LOG_INFO("sqs_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("sqs_source failed to serialize config");
      return false;
    }

    SQSSourceConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("sqs_source invalid config");
      return false;
    }

    if (cfg.queue_url().empty()) {
      FP_LOG_ERROR("sqs_source requires queue_url");
      return false;
    }

    config_ = std::move(cfg);
    client_ = BuildSqsClient(config_);

    if (!client_) {
      return false;
    }

    buffered_messages_.clear();

    FP_LOG_INFO("sqs_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("sqs_source stop requested, skipping produce");
      return false;
    }

    if (!client_) {
      FP_LOG_ERROR("sqs_source client not initialized");
      return false;
    }

    if (buffered_messages_.empty() && !ReceiveMessages()) {
      return false;
    }

    if (buffered_messages_.empty()) {
      return false;
    }

    auto message = buffered_messages_.front();
    buffered_messages_.pop_front();

    payload.assign(message.body.begin(), message.body.end());

    if (config_.delete_after_read()) {
      DeleteMessage(message.receipt_handle);
    }

    FP_LOG_DEBUG("sqs_source produced payload from SQS");
    return true;
  }

private:
  bool ReceiveMessages() {
    Aws::SQS::Model::ReceiveMessageRequest request;
    request.SetQueueUrl(config_.queue_url());

    if (config_.max_number_of_messages() > 0) {
      request.SetMaxNumberOfMessages(
          static_cast<int>(config_.max_number_of_messages()));
    }

    if (config_.wait_time_seconds() > 0) {
      request.SetWaitTimeSeconds(
          static_cast<int>(config_.wait_time_seconds()));
    }

    if (config_.visibility_timeout_seconds() > 0) {
      request.SetVisibilityTimeout(
          static_cast<int>(config_.visibility_timeout_seconds()));
    }

    auto outcome = client_->ReceiveMessage(request);
    if (!outcome.IsSuccess()) {
      FP_LOG_ERROR("sqs_source failed to receive message: "
                   + outcome.GetError().GetMessage());
      return false;
    }

    const auto& messages = outcome.GetResult().GetMessages();
    if (messages.empty()) {
      return false;
    }

    for (const auto& message : messages) {
      buffered_messages_.push_back(
          PendingMessage{message.GetBody(), message.GetReceiptHandle()});
    }

    return true;
  }

  void DeleteMessage(const std::string& receipt_handle) {
    Aws::SQS::Model::DeleteMessageRequest request;
    request.SetQueueUrl(config_.queue_url());
    request.SetReceiptHandle(receipt_handle);

    auto outcome = client_->DeleteMessage(request);
    if (!outcome.IsSuccess()) {
      FP_LOG_ERROR("sqs_source failed to delete message: "
                   + outcome.GetError().GetMessage());
    }
  }

  AwsSdkGuard sdk_guard_{};
  SQSSourceConfig config_{};
  std::unique_ptr<Aws::SQS::SQSClient> client_{};
  std::deque<PendingMessage> buffered_messages_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating sqs_source stage");
  return new SQSSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying sqs_source stage");
  delete stage;
}

}  // extern "C"
