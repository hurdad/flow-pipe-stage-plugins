#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <curlpp/Easy.hpp>
#include <curlpp/Exception.hpp>
#include <curlpp/Infos.hpp>
#include <curlpp/Options.hpp>
#include <curlpp/cURLpp.hpp>
#include <list>
#include <string>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "http_sink.pb.h"

using namespace flowpipe;

using HttpSinkConfig = flowpipe::stages::http::sink::v1::HttpSinkConfig;

namespace {
HttpSinkConfig::Method ResolveMethod(HttpSinkConfig::Method method) {
  if (method == HttpSinkConfig::METHOD_UNSPECIFIED) {
    return HttpSinkConfig::METHOD_POST;
  }
  return method;
}

std::string BuildHeader(const HttpSinkConfig::Header& header) {
  return header.name() + ": " + header.value();
}
}  // namespace

// ============================================================
// HttpSink
// ============================================================
class HttpSink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "http_sink";
  }

  HttpSink() {
    FP_LOG_INFO("http_sink constructed");
  }

  ~HttpSink() override {
    FP_LOG_INFO("http_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("http_sink failed to serialize config");
      return false;
    }

    HttpSinkConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("http_sink invalid config");
      return false;
    }

    if (cfg.url().empty()) {
      FP_LOG_ERROR("http_sink requires url");
      return false;
    }

    HttpSinkConfig::Method method = ResolveMethod(cfg.method());
    if (method != HttpSinkConfig::METHOD_POST && method != HttpSinkConfig::METHOD_PUT) {
      FP_LOG_ERROR("http_sink method must be POST or PUT");
      return false;
    }

    uint32_t timeout_ms = cfg.timeout_ms();
    if (timeout_ms == 0) {
      timeout_ms = 5000;
    }

    std::list<std::string> new_headers;
    std::string content_type = cfg.content_type();
    if (content_type.empty()) {
      content_type = "application/json";
    }
    if (!content_type.empty()) {
      new_headers.emplace_back("Content-Type: " + content_type);
    }

    for (const auto& header : cfg.headers()) {
      if (header.name().empty()) {
        FP_LOG_WARN("http_sink ignoring header with empty name");
        continue;
      }
      new_headers.emplace_back(BuildHeader(header));
    }

    request_.reset();
    request_.setOpt(curlpp::options::Url(cfg.url()));
    request_.setOpt(curlpp::options::Timeout(timeout_ms));
    request_.setOpt(curlpp::options::NoSignal(true));
    if (!new_headers.empty()) {
      request_.setOpt(curlpp::options::HttpHeader(new_headers));
    }

    config_ = std::move(cfg);
    method_ = method;
    timeout_ms_ = timeout_ms;
    headers_ = std::move(new_headers);

    FP_LOG_INFO("http_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("http_sink stop requested, skipping payload");
      return;
    }

    if (config_.url().empty()) {
      FP_LOG_ERROR("http_sink not configured");
      return;
    }

    try {
      request_.reset();
      request_.setOpt(curlpp::options::Url(config_.url()));
      request_.setOpt(curlpp::options::Timeout(timeout_ms_));
      request_.setOpt(curlpp::options::NoSignal(true));
      if (!headers_.empty()) {
        request_.setOpt(curlpp::options::HttpHeader(headers_));
      }

      std::string body(reinterpret_cast<const char*>(payload.data()), payload.size);
      request_.setOpt(curlpp::options::PostFields(body));
      request_.setOpt(curlpp::options::PostFieldSize(static_cast<long>(body.size())));

      if (method_ == HttpSinkConfig::METHOD_PUT) {
        request_.setOpt(curlpp::options::CustomRequest("PUT"));
      } else {
        request_.setOpt(curlpp::options::CustomRequest(""));
      }

      request_.perform();

      long response_code = curlpp::infos::ResponseCode::get(request_);
      if (response_code < 200 || response_code >= 300) {
        FP_LOG_ERROR("http_sink non-success response: " + std::to_string(response_code));
        return;
      }
    } catch (const curlpp::RuntimeError& err) {
      FP_LOG_ERROR(std::string("http_sink curl runtime error: ") + err.what());
      return;
    } catch (const curlpp::LogicError& err) {
      FP_LOG_ERROR(std::string("http_sink curl logic error: ") + err.what());
      return;
    }

    FP_LOG_DEBUG("http_sink sent payload");
  }

 private:
  HttpSinkConfig config_{};
  HttpSinkConfig::Method method_{HttpSinkConfig::METHOD_POST};
  uint32_t timeout_ms_{5000};
  curlpp::Cleanup cleanup_{};
  curlpp::Easy request_{};
  std::list<std::string> headers_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating http_sink stage");
  return new HttpSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying http_sink stage");
  delete stage;
}

}  // extern "C"
