#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <hiredis/hiredis.h>

#include <string>
#include <vector>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "redis_client.h"
#include "redis_stream_sink.pb.h"

using namespace flowpipe;

using RedisStreamSinkConfig = flowpipe::stages::redis::stream::sink::v1::RedisStreamSinkConfig;

namespace {
redisReply* ExecuteXAdd(redisContext* context, const RedisStreamSinkConfig& config,
                        const Payload& payload) {
  std::vector<std::string> args;
  args.emplace_back("XADD");
  args.emplace_back(config.stream());

  if (config.maxlen() > 0) {
    args.emplace_back("MAXLEN");
    args.emplace_back(config.maxlen_approx() ? "~" : "=");
    args.emplace_back(std::to_string(config.maxlen()));
  }

  const std::string id = config.id().empty() ? "*" : config.id();
  args.emplace_back(id);

  const std::string field_name = config.field_name().empty() ? "data" : config.field_name();
  args.emplace_back(field_name);

  std::vector<const char*> argv;
  std::vector<size_t> argvlen;
  argv.reserve(args.size() + 1);
  argvlen.reserve(args.size() + 1);

  for (const auto& arg : args) {
    argv.push_back(arg.c_str());
    argvlen.push_back(arg.size());
  }

  argv.push_back(reinterpret_cast<const char*>(payload.data()));
  argvlen.push_back(payload.size);

  return static_cast<redisReply*>(
      redisCommandArgv(context, static_cast<int>(argv.size()), argv.data(), argvlen.data()));
}
}  // namespace

// ============================================================
// RedisStreamSink
// ============================================================
class RedisStreamSink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "redis_stream_sink";
  }

  RedisStreamSink() {
    FP_LOG_INFO("redis_stream_sink constructed");
  }

  ~RedisStreamSink() override {
    ShutdownConnection();
    FP_LOG_INFO("redis_stream_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("redis_stream_sink failed to serialize config");
      return false;
    }

    RedisStreamSinkConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("redis_stream_sink invalid config");
      return false;
    }

    if (cfg.stream().empty()) {
      FP_LOG_ERROR("redis_stream_sink requires stream");
      return false;
    }

    config_ = std::move(cfg);

    if (!InitializeConnection()) {
      return false;
    }

    FP_LOG_INFO("redis_stream_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("redis_stream_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("redis_stream_sink received empty payload");
      return;
    }

    if (!context_) {
      FP_LOG_ERROR("redis_stream_sink connection not initialized");
      return;
    }

    redisReply* reply = ExecuteXAdd(context_, config_, payload);

    if (!reply) {
      FP_LOG_ERROR("redis_stream_sink failed to add stream entry");
      return;
    }

    freeReplyObject(reply);
  }

 private:
  bool InitializeConnection() {
    ShutdownConnection();

    flowpipe::stages::util::RedisConnectionConfig options;
    options.host = config_.host();
    options.port = static_cast<int>(config_.port());
    options.username = config_.username();
    options.password = config_.password();
    options.database = static_cast<int>(config_.database());

    context_ = flowpipe::stages::util::ConnectRedis(options, "redis_stream_sink");
    if (!context_) {
      return false;
    }

    if (config_.command_timeout_ms() > 0) {
      flowpipe::stages::util::ApplyRedisTimeout(context_, config_.command_timeout_ms());
    }

    return true;
  }

  void ShutdownConnection() {
    flowpipe::stages::util::CloseRedis(context_);
  }

  RedisStreamSinkConfig config_{};
  redisContext* context_{nullptr};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating redis_stream_sink stage");
  return new RedisStreamSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying redis_stream_sink stage");
  delete stage;
}

}  // extern "C"
