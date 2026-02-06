#include <google/protobuf/struct.pb.h>
#include <hiredis/hiredis.h>

#include <string>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "redis_client.h"
#include "redis_pubsub_sink.pb.h"

using namespace flowpipe;

using RedisPubSubSinkConfig = flowpipe::v1::stages::redis::pubsub::sink::v1::RedisPubSubSinkConfig;

// ============================================================
// RedisPubSubSink
// ============================================================
class RedisPubSubSink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "redis_pubsub_sink";
  }

  RedisPubSubSink() {
    FP_LOG_INFO("redis_pubsub_sink constructed");
  }

  ~RedisPubSubSink() override {
    ShutdownConnection();
    FP_LOG_INFO("redis_pubsub_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    RedisPubSubSinkConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<RedisPubSubSinkConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("redis_pubsub_sink invalid config: " + error);
      return false;
    }

    if (cfg.channel().empty()) {
      FP_LOG_ERROR("redis_pubsub_sink requires channel");
      return false;
    }

    config_ = std::move(cfg);

    if (!InitializeConnection()) {
      return false;
    }

    FP_LOG_INFO("redis_pubsub_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("redis_pubsub_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("redis_pubsub_sink received empty payload");
      return;
    }

    if (!context_) {
      FP_LOG_ERROR("redis_pubsub_sink connection not initialized");
      return;
    }

    redisReply* reply = static_cast<redisReply*>(redisCommand(
        context_, "PUBLISH %s %b", config_.channel().c_str(), payload.data(), payload.size));

    const bool ok = flowpipe::v1::stages::util::ValidateRedisReply(reply, name(), "PUBLISH");
    freeReplyObject(reply);
    if (!ok) {
      return;
    }
  }

 private:
  bool InitializeConnection() {
    ShutdownConnection();

    flowpipe::v1::stages::util::RedisConnectionConfig options;
    options.host = config_.host();
    options.port = static_cast<int>(config_.port());
    options.username = config_.username();
    options.password = config_.password();
    options.database = static_cast<int>(config_.database());

    context_ = flowpipe::v1::stages::util::ConnectRedis(options, "redis_pubsub_sink");
    if (!context_) {
      return false;
    }

    if (config_.command_timeout_ms() > 0) {
      flowpipe::v1::stages::util::ApplyRedisTimeout(context_, config_.command_timeout_ms());
    }

    return true;
  }

  void ShutdownConnection() {
    flowpipe::v1::stages::util::CloseRedis(context_);
  }

  RedisPubSubSinkConfig config_{};
  redisContext* context_{nullptr};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating redis_pubsub_sink stage");
  return new RedisPubSubSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying redis_pubsub_sink stage");
  delete stage;
}

}  // extern "C"
