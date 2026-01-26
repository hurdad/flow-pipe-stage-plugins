#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

#include "nats_jetstream_source.pb.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <nats/nats.h>
#include <nats/jetstream.h>

#include <cstring>
#include <string>

using namespace flowpipe;

using NatsJetStreamSourceConfig =
    flowpipe::stages::nats::jetstream::source::v1::NatsJetStreamSourceConfig;

namespace {
constexpr int kDefaultPollTimeoutMs = 1000;
const char* kDefaultNatsUrl = "nats://127.0.0.1:4222";

std::string StatusToString(natsStatus status) {
  return std::string(natsStatus_GetText(status));
}
}  // namespace

// ============================================================
// NatsJetStreamSource
// ============================================================
class NatsJetStreamSource final
    : public ISourceStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "nats_jetstream_source";
  }

  NatsJetStreamSource() {
    FP_LOG_INFO("nats_jetstream_source constructed");
  }

  ~NatsJetStreamSource() override {
    ShutdownConnection();
    FP_LOG_INFO("nats_jetstream_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("nats_jetstream_source failed to serialize config");
      return false;
    }

    NatsJetStreamSourceConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("nats_jetstream_source invalid config");
      return false;
    }

    if (cfg.subject().empty()) {
      FP_LOG_ERROR("nats_jetstream_source requires subject");
      return false;
    }

    std::string url = cfg.url().empty() ? kDefaultNatsUrl : cfg.url();
    int poll_timeout_ms = cfg.poll_timeout_ms() > 0
        ? static_cast<int>(cfg.poll_timeout_ms())
        : kDefaultPollTimeoutMs;

    if (!InitializeConnection(url,
                              cfg.subject(),
                              cfg.stream_name(),
                              cfg.durable_name())) {
      return false;
    }

    config_ = std::move(cfg);
    poll_timeout_ms_ = poll_timeout_ms;

    FP_LOG_INFO("nats_jetstream_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("nats_jetstream_source stop requested, skipping produce");
      return false;
    }

    if (!subscription_) {
      FP_LOG_ERROR("nats_jetstream_source subscription not initialized");
      return false;
    }

    natsMsg* msg = nullptr;
    natsStatus status = natsSubscription_NextMsg(&msg,
                                                 subscription_,
                                                 poll_timeout_ms_);
    if (status == NATS_TIMEOUT) {
      return false;
    }

    if (status != NATS_OK || !msg) {
      FP_LOG_ERROR("nats_jetstream_source receive failed: "
                   + StatusToString(status));
      return false;
    }

    const char* data = natsMsg_GetData(msg);
    int data_len = natsMsg_GetDataLength(msg);
    if (data_len < 0) {
      natsMsg_Destroy(msg);
      FP_LOG_ERROR("nats_jetstream_source invalid message length");
      return false;
    }

    auto buffer = AllocatePayloadBuffer(static_cast<size_t>(data_len));
    if (!buffer) {
      natsMsg_Destroy(msg);
      FP_LOG_ERROR("nats_jetstream_source failed to allocate payload");
      return false;
    }

    if (data_len > 0 && data) {
      std::memcpy(buffer.get(), data, static_cast<size_t>(data_len));
    }

    payload = Payload(std::move(buffer), static_cast<size_t>(data_len));

    status = natsMsg_Ack(msg);
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_jetstream_source ack failed: "
                   + StatusToString(status));
    }

    natsMsg_Destroy(msg);
    return true;
  }

private:
  bool InitializeConnection(const std::string& url,
                            const std::string& subject,
                            const std::string& stream_name,
                            const std::string& durable_name) {
    ShutdownConnection();

    natsConnection* connection = nullptr;
    natsStatus status = natsConnection_ConnectTo(&connection, url.c_str());
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_jetstream_source connect failed: "
                   + StatusToString(status));
      return false;
    }

    jsCtx* jetstream = nullptr;
    status = natsConnection_JetStream(&jetstream, connection, nullptr);
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_jetstream_source jetstream init failed: "
                   + StatusToString(status));
      natsConnection_Destroy(connection);
      return false;
    }

    jsSubOptions sub_options;
    jsSubOptions* options_ptr = nullptr;
    jsSubOptions_Init(&sub_options);

    if (!stream_name.empty()) {
      sub_options.Stream = stream_name.c_str();
      options_ptr = &sub_options;
    }

    if (!durable_name.empty()) {
      sub_options.Durable = durable_name.c_str();
      options_ptr = &sub_options;
    }

    natsSubscription* subscription = nullptr;
    status = js_SubscribeSync(&subscription,
                              jetstream,
                              subject.c_str(),
                              options_ptr);
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_jetstream_source subscribe failed: "
                   + StatusToString(status));
      jsCtx_Destroy(jetstream);
      natsConnection_Destroy(connection);
      return false;
    }

    connection_ = connection;
    jetstream_ = jetstream;
    subscription_ = subscription;
    return true;
  }

  void ShutdownConnection() {
    if (subscription_) {
      natsSubscription_Destroy(subscription_);
      subscription_ = nullptr;
    }
    if (jetstream_) {
      jsCtx_Destroy(jetstream_);
      jetstream_ = nullptr;
    }
    if (connection_) {
      natsConnection_Destroy(connection_);
      connection_ = nullptr;
    }
  }

  NatsJetStreamSourceConfig config_{};
  natsConnection* connection_{nullptr};
  jsCtx* jetstream_{nullptr};
  natsSubscription* subscription_{nullptr};
  int poll_timeout_ms_{kDefaultPollTimeoutMs};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating nats_jetstream_source stage");
  return new NatsJetStreamSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying nats_jetstream_source stage");
  delete stage;
}

}  // extern "C"
