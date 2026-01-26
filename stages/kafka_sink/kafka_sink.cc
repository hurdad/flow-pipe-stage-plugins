#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <librdkafka/rdkafka.h>

#include <cstdlib>
#include <cstring>
#include <string>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "kafka_sink.pb.h"

using namespace flowpipe;

using KafkaSinkConfig = flowpipe::stages::kafka::sink::v1::KafkaSinkConfig;

// ============================================================
// KafkaSink
// ============================================================
class KafkaSink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "kafka_sink";
  }

  KafkaSink() {
    FP_LOG_INFO("kafka_sink constructed");
  }

  ~KafkaSink() override {
    ShutdownProducer();
    FP_LOG_INFO("kafka_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("kafka_sink failed to serialize config");
      return false;
    }

    KafkaSinkConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("kafka_sink invalid config");
      return false;
    }

    if (cfg.brokers().empty() || cfg.topic().empty()) {
      FP_LOG_ERROR("kafka_sink requires brokers and topic");
      return false;
    }

    config_ = std::move(cfg);

    if (!InitializeProducer()) {
      return false;
    }

    FP_LOG_INFO("kafka_sink configured");

    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  // Called once per payload by the runtime
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("kafka_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("kafka_sink received empty payload");
      return;
    }

    if (!producer_ || !topic_) {
      FP_LOG_ERROR("kafka_sink producer not initialized");
      return;
    }

    const int32_t partition = (config_.partition() >= -1) ? config_.partition() : -1;

    int result = rd_kafka_produce(topic_, partition, RD_KAFKA_MSG_F_COPY,
                                  const_cast<char*>(reinterpret_cast<const char*>(payload.data())),
                                  payload.size, nullptr, 0, nullptr);

    if (result != 0) {
      FP_LOG_ERROR("kafka_sink failed to produce message: " +
                   std::string(rd_kafka_err2str(rd_kafka_last_error())));
      return;
    }

    rd_kafka_poll(producer_, 0);
  }

 private:
  static void DeliveryReportCallback(rd_kafka_t*, const rd_kafka_message_t* message, void*) {
    if (message->err) {
      FP_LOG_ERROR("kafka_sink delivery failed: " + std::string(rd_kafka_message_errstr(message)));
      return;
    }

    FP_LOG_DEBUG("kafka_sink delivered message");
  }

  bool InitializeProducer() {
    ShutdownProducer();

    char errstr[512] = {0};
    rd_kafka_conf_t* conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", config_.brokers().c_str(), errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK) {
      FP_LOG_ERROR("kafka_sink invalid brokers config: " + std::string(errstr));
      rd_kafka_conf_destroy(conf);
      return false;
    }

    if (!config_.client_id().empty()) {
      if (rd_kafka_conf_set(conf, "client.id", config_.client_id().c_str(), errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        FP_LOG_ERROR("kafka_sink invalid client.id config: " + std::string(errstr));
        rd_kafka_conf_destroy(conf);
        return false;
      }
    }

    for (const auto& entry : config_.producer_config()) {
      if (rd_kafka_conf_set(conf, entry.first.c_str(), entry.second.c_str(), errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        FP_LOG_ERROR("kafka_sink invalid producer config " + entry.first + ": " +
                     std::string(errstr));
        rd_kafka_conf_destroy(conf);
        return false;
      }
    }

    rd_kafka_conf_set_dr_msg_cb(conf, &KafkaSink::DeliveryReportCallback);

    producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer_) {
      FP_LOG_ERROR("kafka_sink failed to create producer: " + std::string(errstr));
      rd_kafka_conf_destroy(conf);
      return false;
    }

    topic_ = rd_kafka_topic_new(producer_, config_.topic().c_str(), nullptr);
    if (!topic_) {
      FP_LOG_ERROR("kafka_sink failed to create topic: " +
                   std::string(rd_kafka_err2str(rd_kafka_last_error())));
      ShutdownProducer();
      return false;
    }

    return true;
  }

  void ShutdownProducer() {
    if (producer_) {
      rd_kafka_flush(producer_, 5000);
    }

    if (topic_) {
      rd_kafka_topic_destroy(topic_);
      topic_ = nullptr;
    }

    if (producer_) {
      rd_kafka_destroy(producer_);
      producer_ = nullptr;
    }
  }

  KafkaSinkConfig config_{};
  rd_kafka_t* producer_{nullptr};
  rd_kafka_topic_t* topic_{nullptr};
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
