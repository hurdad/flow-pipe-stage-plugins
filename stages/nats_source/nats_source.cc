#include <google/protobuf/struct.pb.h>
#include <nats/nats.h>

#include <cstring>
#include <string>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "nats_source.pb.h"

using namespace flowpipe;

using NatsSourceConfig = flowpipe::v1::stages::nats::source::v1::NatsSourceConfig;

namespace {
constexpr int kDefaultPollTimeoutMs = 1000;
const char* kDefaultNatsUrl = "nats://127.0.0.1:4222";

std::string StatusToString(natsStatus status) {
  return std::string(natsStatus_GetText(status));
}
}  // namespace

// ============================================================
// NatsSource
// ============================================================
class NatsSource final : public ISourceStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "nats_source";
  }

  NatsSource() {
    FP_LOG_INFO("nats_source constructed");
  }

  ~NatsSource() override {
    ShutdownConnection();
    FP_LOG_INFO("nats_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    NatsSourceConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<NatsSourceConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("nats_source invalid config: " + error);
      return false;
    }

    if (cfg.subject().empty()) {
      FP_LOG_ERROR("nats_source requires subject");
      return false;
    }

    std::string url = cfg.url().empty() ? kDefaultNatsUrl : cfg.url();
    int poll_timeout_ms =
        cfg.poll_timeout_ms() > 0 ? static_cast<int>(cfg.poll_timeout_ms()) : kDefaultPollTimeoutMs;

    if (!InitializeConnection(url, cfg.subject(), cfg.queue_group())) {
      return false;
    }

    config_ = std::move(cfg);
    poll_timeout_ms_ = poll_timeout_ms;

    FP_LOG_INFO("nats_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("nats_source stop requested, skipping produce");
      return false;
    }

    if (!subscription_) {
      FP_LOG_ERROR("nats_source subscription not initialized");
      return false;
    }

    natsMsg* msg = nullptr;
    natsStatus status = natsSubscription_NextMsg(&msg, subscription_, poll_timeout_ms_);
    if (status == NATS_TIMEOUT) {
      return false;
    }

    if (status != NATS_OK || !msg) {
      FP_LOG_ERROR("nats_source receive failed: " + StatusToString(status));
      return false;
    }

    const char* data = natsMsg_GetData(msg);
    int data_len = natsMsg_GetDataLength(msg);
    if (data_len < 0) {
      natsMsg_Destroy(msg);
      FP_LOG_ERROR("nats_source invalid message length");
      return false;
    }

    auto buffer = AllocatePayloadBuffer(static_cast<size_t>(data_len));
    if (!buffer) {
      natsMsg_Destroy(msg);
      FP_LOG_ERROR("nats_source failed to allocate payload");
      return false;
    }

    if (data_len > 0 && data) {
      std::memcpy(buffer.get(), data, static_cast<size_t>(data_len));
    }

    payload = Payload(std::move(buffer), static_cast<size_t>(data_len));
    natsMsg_Destroy(msg);
    return true;
  }

 private:
  bool InitializeConnection(const std::string& url, const std::string& subject,
                            const std::string& queue_group) {
    ShutdownConnection();

    natsConnection* connection = nullptr;
    natsStatus status = natsConnection_ConnectTo(&connection, url.c_str());
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_source connect failed: " + StatusToString(status));
      return false;
    }

    natsSubscription* subscription = nullptr;
    if (!queue_group.empty()) {
      status = natsConnection_QueueSubscribeSync(&subscription, connection, subject.c_str(),
                                                 queue_group.c_str());
    } else {
      status = natsConnection_SubscribeSync(&subscription, connection, subject.c_str());
    }

    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_source subscribe failed: " + StatusToString(status));
      natsConnection_Destroy(connection);
      return false;
    }

    connection_ = connection;
    subscription_ = subscription;
    return true;
  }

  void ShutdownConnection() {
    if (subscription_) {
      natsSubscription_Destroy(subscription_);
      subscription_ = nullptr;
    }
    if (connection_) {
      natsConnection_Destroy(connection_);
      connection_ = nullptr;
    }
  }

  NatsSourceConfig config_{};
  natsConnection* connection_{nullptr};
  natsSubscription* subscription_{nullptr};
  int poll_timeout_ms_{kDefaultPollTimeoutMs};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating nats_source stage");
  return new NatsSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying nats_source stage");
  delete stage;
}

}  // extern "C"
