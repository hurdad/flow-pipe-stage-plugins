#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

#include "kafka_sink.pb.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <cstdlib>
#include <cstring>

using namespace flowpipe;

using KafkaSinkConfig =
    flowpipe::stages::kafka::sink::v1::KafkaSinkConfig;

// ============================================================
// KafkaSink
// ============================================================
class KafkaSink final
    : public ISinkStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "kafka_sink";
  }

  KafkaSink() {
    FP_LOG_INFO("kafka_sink constructed");
  }

  ~KafkaSink() override {
    FP_LOG_INFO("kafka_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status =
        google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("kafka_sink failed to serialize config");
      return false;
    }

    KafkaSinkConfig cfg;
    status =
        google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("kafka_sink invalid config");
      return false;
    }

    config_ = std::move(cfg);

    FP_LOG_INFO("kafka_sink configured");

    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  // Called once per payload by the runtime
  void consume(StageContext& ctx, const Payload& payload) override
  {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("kafka_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("kafka_sink received empty payload");
      return;
    }
  }

private:
  KafkaSinkConfig config_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating kafka_sink stage");
  return new KafkaSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying kafka_sink stage");
  delete stage;
}

}  // extern "C"