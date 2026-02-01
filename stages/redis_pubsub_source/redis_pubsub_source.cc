#include <google/protobuf/struct.pb.h>
#include <hiredis/hiredis.h>

#include <cstring>
#include <string>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "redis_client.h"
#include "redis_pubsub_source.pb.h"

using namespace flowpipe;

using RedisPubSubSourceConfig =
    flowpipe::v1::stages::redis::pubsub::source::v1::RedisPubSubSourceConfig;

// ============================================================
// RedisPubSubSource
// ============================================================
class RedisPubSubSource final : public ISourceStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "redis_pubsub_source";
  }

  RedisPubSubSource() {
    FP_LOG_INFO("redis_pubsub_source constructed");
  }

  ~RedisPubSubSource() override {
    ShutdownConnection();
    FP_LOG_INFO("redis_pubsub_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    RedisPubSubSourceConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<RedisPubSubSourceConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("redis_pubsub_source invalid config: " + error);
      return false;
    }

    if (cfg.channel().empty()) {
      FP_LOG_ERROR("redis_pubsub_source requires channel");
      return false;
    }

    config_ = std::move(cfg);

    if (!InitializeConnection()) {
      return false;
    }

    FP_LOG_INFO("redis_pubsub_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("redis_pubsub_source stop requested, exiting");
      return false;
    }

    if (!context_) {
      FP_LOG_ERROR("redis_pubsub_source connection not initialized");
      return false;
    }

    void* reply_ptr = nullptr;
    const int result = redisGetReply(context_, &reply_ptr);
    if (result != REDIS_OK || !reply_ptr) {
      if (context_->err) {
        FP_LOG_ERROR("redis_pubsub_source read error: " + std::string(context_->errstr));
      }
      return false;
    }

    redisReply* reply = static_cast<redisReply*>(reply_ptr);
    bool produced = false;

    if (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3) {
      redisReply* kind = reply->element[0];
      redisReply* message = reply->element[2];

      if (kind && kind->type == REDIS_REPLY_STRING &&
          std::string(kind->str, kind->len) == "message" && message &&
          message->type == REDIS_REPLY_STRING) {
        auto buffer = AllocatePayloadBuffer(message->len);
        if (!buffer) {
          FP_LOG_ERROR("redis_pubsub_source failed to allocate payload");
        } else {
          std::memcpy(buffer.get(), message->str, message->len);
          payload = Payload(std::move(buffer), message->len);
          produced = true;
        }
      }
    }

    freeReplyObject(reply);
    return produced;
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

    context_ = flowpipe::v1::stages::util::ConnectRedis(options, "redis_pubsub_source");
    if (!context_) {
      return false;
    }

    const int timeout_ms = config_.poll_timeout_ms() > 0 ? config_.poll_timeout_ms() : 1000;
    flowpipe::v1::stages::util::ApplyRedisTimeout(context_, timeout_ms);

    redisReply* reply =
        static_cast<redisReply*>(redisCommand(context_, "SUBSCRIBE %s", config_.channel().c_str()));
    if (!reply) {
      FP_LOG_ERROR("redis_pubsub_source failed to subscribe to channel");
      ShutdownConnection();
      return false;
    }

    const bool ok = reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3 &&
                    reply->element[0]->type == REDIS_REPLY_STRING &&
                    std::string(reply->element[0]->str, reply->element[0]->len) == "subscribe";
    freeReplyObject(reply);

    if (!ok) {
      FP_LOG_ERROR("redis_pubsub_source unexpected subscribe response");
      ShutdownConnection();
      return false;
    }

    return true;
  }

  void ShutdownConnection() {
    flowpipe::v1::stages::util::CloseRedis(context_);
  }

  RedisPubSubSourceConfig config_{};
  redisContext* context_{nullptr};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating redis_pubsub_source stage");
  return new RedisPubSubSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying redis_pubsub_source stage");
  delete stage;
}

}  // extern "C"
