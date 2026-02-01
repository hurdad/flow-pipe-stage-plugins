#include <google/protobuf/struct.pb.h>
#include <nats/nats.h>

#include <string>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "nats_sink.pb.h"

using namespace flowpipe;

using NatsSinkConfig = flowpipe::v1::stages::nats::sink::v1::NatsSinkConfig;

namespace {
constexpr int kDefaultFlushTimeoutMs = 1000;
const char* kDefaultNatsUrl = "nats://127.0.0.1:4222";

std::string StatusToString(natsStatus status) {
  return std::string(natsStatus_GetText(status));
}
}  // namespace

// ============================================================
// NatsSink
// ============================================================
class NatsSink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "nats_sink";
  }

  NatsSink() {
    FP_LOG_INFO("nats_sink constructed");
  }

  ~NatsSink() override {
    ShutdownConnection();
    FP_LOG_INFO("nats_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    NatsSinkConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<NatsSinkConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("nats_sink invalid config: " + error);
      return false;
    }

    if (cfg.subject().empty()) {
      FP_LOG_ERROR("nats_sink requires subject");
      return false;
    }

    std::string url = cfg.url().empty() ? kDefaultNatsUrl : cfg.url();
    int flush_timeout_ms = cfg.flush_timeout_ms() > 0 ? static_cast<int>(cfg.flush_timeout_ms())
                                                      : kDefaultFlushTimeoutMs;

    if (!InitializeConnection(url)) {
      return false;
    }

    config_ = std::move(cfg);
    subject_ = config_.subject();
    flush_timeout_ms_ = flush_timeout_ms;

    FP_LOG_INFO("nats_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("nats_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("nats_sink received empty payload");
      return;
    }

    if (!connection_) {
      FP_LOG_ERROR("nats_sink connection not initialized");
      return;
    }

    natsStatus status =
        natsConnection_Publish(connection_, subject_.c_str(), payload.data(), payload.size);
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_sink publish failed: " + StatusToString(status));
      return;
    }

    if (flush_timeout_ms_ > 0) {
      status = natsConnection_FlushTimeout(connection_, flush_timeout_ms_);
      if (status != NATS_OK) {
        FP_LOG_ERROR("nats_sink flush failed: " + StatusToString(status));
      }
    }
  }

 private:
  bool InitializeConnection(const std::string& url) {
    ShutdownConnection();

    natsConnection* connection = nullptr;
    natsStatus status = natsConnection_ConnectTo(&connection, url.c_str());
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_sink connect failed: " + StatusToString(status));
      return false;
    }

    connection_ = connection;
    return true;
  }

  void ShutdownConnection() {
    if (connection_) {
      natsConnection_Destroy(connection_);
      connection_ = nullptr;
    }
  }

  NatsSinkConfig config_{};
  natsConnection* connection_{nullptr};
  std::string subject_{};
  int flush_timeout_ms_{kDefaultFlushTimeoutMs};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating nats_sink stage");
  return new NatsSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying nats_sink stage");
  delete stage;
}

}  // extern "C"
