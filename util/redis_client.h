#pragma once

#include <hiredis/hiredis.h>
#include <sys/time.h>

#include <string>

#include "flowpipe/observability/logging.h"

namespace flowpipe::stages::util {

struct RedisConnectionConfig {
  std::string host;
  int port{6379};
  std::string username;
  std::string password;
  int database{0};
  int connect_timeout_ms{2000};
};

inline void CloseRedis(redisContext*& context) {
  if (context) {
    redisFree(context);
    context = nullptr;
  }
}

inline void ApplyRedisTimeout(redisContext* context, int timeout_ms) {
  if (!context || timeout_ms <= 0) {
    return;
  }

  timeval timeout{};
  timeout.tv_sec = timeout_ms / 1000;
  timeout.tv_usec = (timeout_ms % 1000) * 1000;
  redisSetTimeout(context, timeout);
}

inline redisContext* ConnectRedis(const RedisConnectionConfig& config,
                                  const std::string& stage_name) {
  const std::string host = config.host.empty() ? "127.0.0.1" : config.host;
  const int port = config.port > 0 ? config.port : 6379;
  const int timeout_ms = config.connect_timeout_ms > 0 ? config.connect_timeout_ms : 2000;

  timeval timeout{};
  timeout.tv_sec = timeout_ms / 1000;
  timeout.tv_usec = (timeout_ms % 1000) * 1000;

  redisContext* context = redisConnectWithTimeout(host.c_str(), port, timeout);
  if (!context || context->err) {
    FP_LOG_ERROR(stage_name + " failed to connect to Redis at " + host + ":" +
                 std::to_string(port) +
                 (context && context->err ? " (" + std::string(context->errstr) + ")" : ""));
    CloseRedis(context);
    return nullptr;
  }

  if (!config.username.empty() || !config.password.empty()) {
    if (config.password.empty()) {
      FP_LOG_ERROR(stage_name + " requires password when username is provided");
      CloseRedis(context);
      return nullptr;
    }

    redisReply* reply = nullptr;
    if (!config.username.empty()) {
      reply = static_cast<redisReply*>(
          redisCommand(context, "AUTH %s %s", config.username.c_str(), config.password.c_str()));
    } else {
      reply = static_cast<redisReply*>(redisCommand(context, "AUTH %s", config.password.c_str()));
    }

    if (!reply) {
      FP_LOG_ERROR(stage_name + " failed to authenticate with Redis");
      CloseRedis(context);
      return nullptr;
    }

    const bool ok =
        reply->type == REDIS_REPLY_STATUS && reply->str && std::string(reply->str) == "OK";
    freeReplyObject(reply);

    if (!ok) {
      FP_LOG_ERROR(stage_name + " Redis AUTH failed");
      CloseRedis(context);
      return nullptr;
    }
  }

  if (config.database > 0) {
    redisReply* reply =
        static_cast<redisReply*>(redisCommand(context, "SELECT %d", config.database));
    if (!reply) {
      FP_LOG_ERROR(stage_name + " failed to select Redis database");
      CloseRedis(context);
      return nullptr;
    }

    const bool ok =
        reply->type == REDIS_REPLY_STATUS && reply->str && std::string(reply->str) == "OK";
    freeReplyObject(reply);

    if (!ok) {
      FP_LOG_ERROR(stage_name + " Redis SELECT failed");
      CloseRedis(context);
      return nullptr;
    }
  }

  return context;
}

}  // namespace flowpipe::stages::util
