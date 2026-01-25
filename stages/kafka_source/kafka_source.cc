#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

#include "kafka_source.pb.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <cstdlib>
#include <cstring>
#include <string>

#include <librdkafka/rdkafka.h>

using namespace flowpipe;

using KafkaSourceConfig =
    flowpipe::stages::kafka::source::v1::KafkaSourceConfig;

// ============================================================
// KafkaSource
// ============================================================
class KafkaSource final
    : public ISourceStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "kafka_source";
  }

  KafkaSource() {
    FP_LOG_INFO("kafka_source constructed");
  }

  ~KafkaSource() override {
    ShutdownConsumer();
    FP_LOG_INFO("kafka_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status =
        google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("kafka_source failed to serialize config");
      return false;
    }

    KafkaSourceConfig cfg;
    status =
        google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("kafka_source invalid config");
      return false;
    }

    if (cfg.brokers().empty() || cfg.topic().empty()
        || cfg.group_id().empty()) {
      FP_LOG_ERROR("kafka_source requires brokers, topic, and group_id");
      return false;
    }

    config_ = std::move(cfg);

    if (!InitializeConsumer()) {
      return false;
    }

    FP_LOG_INFO("kafka_source configured");

    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("kafka_source stop requested, exiting");
      return false;
    }

    if (!consumer_) {
      FP_LOG_ERROR("kafka_source consumer not initialized");
      return false;
    }

    const int timeout_ms = config_.poll_timeout_ms() > 0
        ? config_.poll_timeout_ms()
        : 1000;

    rd_kafka_message_t* message = rd_kafka_consumer_poll(consumer_, timeout_ms);
    if (!message) {
      return false;
    }

    if (message->err) {
      if (message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        FP_LOG_ERROR("kafka_source consume error: "
                     + std::string(rd_kafka_message_errstr(message)));
      }
      rd_kafka_message_destroy(message);
      return false;
    }

    if (message->payload && message->len > 0) {
      // ----------------------------------------------------------
      // Allocate payload buffer with shared ownership
      // ----------------------------------------------------------
      auto buffer = AllocatePayloadBuffer(message->len);
      if (!buffer) {
        FP_LOG_ERROR("noop_source failed to allocate payload");
        return false;
      }

      std::memcpy(buffer.get(), message->payload, message->len);
      payload = Payload(std::move(buffer), message->len);
    }
    rd_kafka_message_destroy(message);
    return true;
  }

private:
  bool InitializeConsumer() {
    ShutdownConsumer();

    char errstr[512] = {0};
    rd_kafka_conf_t* conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf,
                          "bootstrap.servers",
                          config_.brokers().c_str(),
                          errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK) {
      FP_LOG_ERROR("kafka_source invalid brokers config: "
                   + std::string(errstr));
      rd_kafka_conf_destroy(conf);
      return false;
    }

    if (rd_kafka_conf_set(conf,
                          "group.id",
                          config_.group_id().c_str(),
                          errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK) {
      FP_LOG_ERROR("kafka_source invalid group.id config: "
                   + std::string(errstr));
      rd_kafka_conf_destroy(conf);
      return false;
    }

    if (!config_.client_id().empty()) {
      if (rd_kafka_conf_set(conf,
                            "client.id",
                            config_.client_id().c_str(),
                            errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        FP_LOG_ERROR("kafka_source invalid client.id config: "
                     + std::string(errstr));
        rd_kafka_conf_destroy(conf);
        return false;
      }
    }

    if (!config_.auto_offset_reset().empty()) {
      if (rd_kafka_conf_set(conf,
                            "auto.offset.reset",
                            config_.auto_offset_reset().c_str(),
                            errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        FP_LOG_ERROR("kafka_source invalid auto.offset.reset config: "
                     + std::string(errstr));
        rd_kafka_conf_destroy(conf);
        return false;
      }
    }

    for (const auto& entry : config_.consumer_config()) {
      if (rd_kafka_conf_set(conf,
                            entry.first.c_str(),
                            entry.second.c_str(),
                            errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        FP_LOG_ERROR("kafka_source invalid consumer config "
                     + entry.first + ": " + std::string(errstr));
        rd_kafka_conf_destroy(conf);
        return false;
      }
    }

    consumer_ = rd_kafka_new(RD_KAFKA_CONSUMER,
                             conf,
                             errstr,
                             sizeof(errstr));
    if (!consumer_) {
      FP_LOG_ERROR("kafka_source failed to create consumer: "
                   + std::string(errstr));
      rd_kafka_conf_destroy(conf);
      return false;
    }

    rd_kafka_poll_set_consumer(consumer_);

    if (config_.partition() >= 0) {
      rd_kafka_topic_partition_list_t* partitions =
          rd_kafka_topic_partition_list_new(1);
      rd_kafka_topic_partition_list_add(partitions,
                                        config_.topic().c_str(),
                                        config_.partition());
      auto err = rd_kafka_assign(consumer_, partitions);
      rd_kafka_topic_partition_list_destroy(partitions);
      if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        FP_LOG_ERROR("kafka_source failed to assign partition: "
                     + std::string(rd_kafka_err2str(err)));
        ShutdownConsumer();
        return false;
      }
    } else {
      rd_kafka_topic_partition_list_t* topics =
          rd_kafka_topic_partition_list_new(1);
      rd_kafka_topic_partition_list_add(topics,
                                        config_.topic().c_str(),
                                        RD_KAFKA_PARTITION_UA);
      auto err = rd_kafka_subscribe(consumer_, topics);
      rd_kafka_topic_partition_list_destroy(topics);
      if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        FP_LOG_ERROR("kafka_source failed to subscribe: "
                     + std::string(rd_kafka_err2str(err)));
        ShutdownConsumer();
        return false;
      }
    }

    return true;
  }

  void ShutdownConsumer() {
    if (consumer_) {
      rd_kafka_consumer_close(consumer_);
      rd_kafka_destroy(consumer_);
      consumer_ = nullptr;
    }
  }

  KafkaSourceConfig config_{};
  rd_kafka_t* consumer_{nullptr};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating kafka_source stage");
  return new KafkaSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying kafka_source stage");
  delete stage;
}

}  // extern "C"
