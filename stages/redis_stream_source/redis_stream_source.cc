#include <google/protobuf/struct.pb.h>
#include "flowpipe/protobuf_config.h"
#include <hiredis/hiredis.h>

#include <cstring>
#include <string>
#include <vector>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "redis_client.h"
#include "redis_stream_source.pb.h"

using namespace flowpipe;

using RedisStreamSourceConfig =
    flowpipe::stages::redis::stream::source::v1::RedisStreamSourceConfig;

namespace {
struct StreamMessage {
  std::string id;
  std::string payload;
};

bool ExtractStreamMessage(const redisReply* reply, const std::string& field_name,
                          StreamMessage& message) {
  if (!reply) {
    return false;
  }

  if (reply->type == REDIS_REPLY_NIL || reply->elements == 0) {
    return false;
  }

  if (reply->type != REDIS_REPLY_ARRAY) {
    return false;
  }

  const redisReply* stream_entry = reply->element[0];
  if (!stream_entry || stream_entry->type != REDIS_REPLY_ARRAY || stream_entry->elements < 2) {
    return false;
  }

  const redisReply* entries = stream_entry->element[1];
  if (!entries || entries->type != REDIS_REPLY_ARRAY || entries->elements == 0) {
    return false;
  }

  const redisReply* entry = entries->element[0];
  if (!entry || entry->type != REDIS_REPLY_ARRAY || entry->elements < 2) {
    return false;
  }

  const redisReply* id_reply = entry->element[0];
  const redisReply* fields = entry->element[1];
  if (!id_reply || id_reply->type != REDIS_REPLY_STRING || !fields ||
      fields->type != REDIS_REPLY_ARRAY || fields->elements < 2) {
    return false;
  }

  message.id.assign(id_reply->str, id_reply->len);

  for (size_t i = 0; i + 1 < fields->elements; i += 2) {
    const redisReply* field = fields->element[i];
    const redisReply* value = fields->element[i + 1];
    if (!field || !value || field->type != REDIS_REPLY_STRING ||
        value->type != REDIS_REPLY_STRING) {
      continue;
    }

    const std::string current_field(field->str, field->len);
    if (!field_name.empty() && current_field != field_name) {
      continue;
    }

    message.payload.assign(value->str, value->len);
    return true;
  }

  if (field_name.empty()) {
    return false;
  }

  return false;
}

redisReply* ExecuteXRead(redisContext* context, const RedisStreamSourceConfig& config,
                         const std::string& last_id) {
  std::vector<std::string> args;
  args.emplace_back("XREAD");

  if (config.block_timeout_ms() > 0) {
    args.emplace_back("BLOCK");
    args.emplace_back(std::to_string(config.block_timeout_ms()));
  }

  if (config.count() > 0) {
    args.emplace_back("COUNT");
    args.emplace_back(std::to_string(config.count()));
  }

  args.emplace_back("STREAMS");
  args.emplace_back(config.stream());
  args.emplace_back(last_id);

  std::vector<const char*> argv;
  std::vector<size_t> argvlen;
  argv.reserve(args.size());
  argvlen.reserve(args.size());

  for (const auto& arg : args) {
    argv.push_back(arg.c_str());
    argvlen.push_back(arg.size());
  }

  return static_cast<redisReply*>(
      redisCommandArgv(context, static_cast<int>(argv.size()), argv.data(), argvlen.data()));
}
}  // namespace

// ============================================================
// RedisStreamSource
// ============================================================
class RedisStreamSource final : public ISourceStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "redis_stream_source";
  }

  RedisStreamSource() {
    FP_LOG_INFO("redis_stream_source constructed");
  }

  ~RedisStreamSource() override {
    ShutdownConnection();
    FP_LOG_INFO("redis_stream_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    RedisStreamSourceConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<RedisStreamSourceConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("redis_stream_source invalid config: " + error);
      return false;
    }

    if (cfg.stream().empty()) {
      FP_LOG_ERROR("redis_stream_source requires stream");
      return false;
    }

    config_ = std::move(cfg);
    last_id_ = config_.start_id().empty() ? "$" : config_.start_id();

    if (!InitializeConnection()) {
      return false;
    }

    FP_LOG_INFO("redis_stream_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("redis_stream_source stop requested, exiting");
      return false;
    }

    if (!context_) {
      FP_LOG_ERROR("redis_stream_source connection not initialized");
      return false;
    }

    redisReply* reply = ExecuteXRead(context_, config_, last_id_);
    if (!reply) {
      if (context_->err) {
        FP_LOG_ERROR("redis_stream_source read error: " + std::string(context_->errstr));
      }
      return false;
    }

    StreamMessage message;
    const bool has_message = ExtractStreamMessage(reply, config_.field_name(), message);
    freeReplyObject(reply);

    if (!has_message) {
      return false;
    }

    last_id_ = message.id;

    auto buffer = AllocatePayloadBuffer(message.payload.size());
    if (!buffer) {
      FP_LOG_ERROR("redis_stream_source failed to allocate payload");
      return false;
    }

    std::memcpy(buffer.get(), message.payload.data(), message.payload.size());
    payload = Payload(std::move(buffer), message.payload.size());
    return true;
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

    context_ = flowpipe::stages::util::ConnectRedis(options, "redis_stream_source");
    if (!context_) {
      return false;
    }

    if (config_.block_timeout_ms() > 0) {
      flowpipe::stages::util::ApplyRedisTimeout(context_, config_.block_timeout_ms());
    }

    return true;
  }

  void ShutdownConnection() {
    flowpipe::stages::util::CloseRedis(context_);
  }

  RedisStreamSourceConfig config_{};
  redisContext* context_{nullptr};
  std::string last_id_;
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating redis_stream_source stage");
  return new RedisStreamSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying redis_stream_source stage");
  delete stage;
}

}  // extern "C"
