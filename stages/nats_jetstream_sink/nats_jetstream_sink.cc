#include <google/protobuf/struct.pb.h>
#include "flowpipe/protobuf_config.h"
#include <nats/nats.h>

#include <string>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "nats_jetstream_sink.pb.h"

using namespace flowpipe;

using NatsJetStreamSinkConfig =
    flowpipe::stages::nats::jetstream::sink::v1::NatsJetStreamSinkConfig;

namespace {
const char* kDefaultNatsUrl = "nats://127.0.0.1:4222";

std::string StatusToString(natsStatus status) {
  return std::string(natsStatus_GetText(status));
}
}  // namespace

// ============================================================
// NatsJetStreamSink
// ============================================================
class NatsJetStreamSink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "nats_jetstream_sink";
  }

  NatsJetStreamSink() {
    FP_LOG_INFO("nats_jetstream_sink constructed");
  }

  ~NatsJetStreamSink() override {
    ShutdownConnection();
    FP_LOG_INFO("nats_jetstream_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    NatsJetStreamSinkConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<NatsJetStreamSinkConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("nats_jetstream_sink invalid config: " + error);
      return false;
    }

    if (cfg.subject().empty()) {
      FP_LOG_ERROR("nats_jetstream_sink requires subject");
      return false;
    }

    std::string url = cfg.url().empty() ? kDefaultNatsUrl : cfg.url();
    if (!InitializeConnection(url)) {
      return false;
    }

    config_ = std::move(cfg);
    subject_ = config_.subject();

    FP_LOG_INFO("nats_jetstream_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("nats_jetstream_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("nats_jetstream_sink received empty payload");
      return;
    }

    if (!jetstream_) {
      FP_LOG_ERROR("nats_jetstream_sink connection not initialized");
      return;
    }

    jsPubAck* ack = nullptr;
    jsErrCode err_code = static_cast<jsErrCode>(0);
    natsStatus status = js_Publish(&ack,
                                   jetstream_,
                                   subject_.c_str(),
                                   payload.data(),
                                   payload.size,
                                   nullptr,
                                   &err_code);
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_jetstream_sink publish failed: " + StatusToString(status) +
                   " (err_code=" + std::to_string(err_code) + ")");
    }

    if (ack) {
      jsPubAck_Destroy(ack);
    }
  }

 private:
  bool InitializeConnection(const std::string& url) {
    ShutdownConnection();

    natsConnection* connection = nullptr;
    natsStatus status = natsConnection_ConnectTo(&connection, url.c_str());
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_jetstream_sink connect failed: " + StatusToString(status));
      return false;
    }

    jsCtx* jetstream = nullptr;
    status = natsConnection_JetStream(&jetstream, connection, nullptr);
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_jetstream_sink jetstream init failed: " + StatusToString(status));
      natsConnection_Destroy(connection);
      return false;
    }

    connection_ = connection;
    jetstream_ = jetstream;
    return true;
  }

  void ShutdownConnection() {
    if (jetstream_) {
      jsCtx_Destroy(jetstream_);
      jetstream_ = nullptr;
    }
    if (connection_) {
      natsConnection_Destroy(connection_);
      connection_ = nullptr;
    }
  }

  NatsJetStreamSinkConfig config_{};
  natsConnection* connection_{nullptr};
  jsCtx* jetstream_{nullptr};
  std::string subject_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating nats_jetstream_sink stage");
  return new NatsJetStreamSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying nats_jetstream_sink stage");
  delete stage;
}

}  // extern "C"
