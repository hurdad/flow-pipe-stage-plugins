#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

#include "file_sink.pb.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <zlib.h>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <string>

using namespace flowpipe;

using FileSinkConfig = flowpipe::stages::file::sink::v1::FileSinkConfig;

namespace {
std::string ToLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

enum class CompressionType {
  kNone,
  kGzip,
};

bool ParseCompression(const std::string& compression,
                      CompressionType& out_type) {
  if (compression.empty()) {
    out_type = CompressionType::kNone;
    return true;
  }

  std::string normalized = ToLower(compression);
  if (normalized == "none") {
    out_type = CompressionType::kNone;
    return true;
  }
  if (normalized == "gzip") {
    out_type = CompressionType::kGzip;
    return true;
  }

  return false;
}

std::string BuildGzipMode(bool append, int compression_level) {
  std::string mode = append ? "ab" : "wb";
  if (compression_level > 0 && compression_level <= 9) {
    mode.push_back(static_cast<char>('0' + compression_level));
  }
  return mode;
}
}  // namespace

// ============================================================
// FileSink
// ============================================================
class FileSink final
    : public ISinkStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "file_sink";
  }

  FileSink() {
    FP_LOG_INFO("file_sink constructed");
  }

  ~FileSink() override {
    FP_LOG_INFO("file_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("file_sink failed to serialize config");
      return false;
    }

    FileSinkConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("file_sink invalid config");
      return false;
    }

    if (cfg.path().empty()) {
      FP_LOG_ERROR("file_sink requires path");
      return false;
    }

    CompressionType compression;
    if (!ParseCompression(cfg.compression(), compression)) {
      FP_LOG_ERROR("file_sink unsupported compression: " + cfg.compression());
      return false;
    }

    config_ = std::move(cfg);
    compression_ = compression;

    FP_LOG_INFO("file_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("file_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("file_sink received empty payload");
      return;
    }

    switch (compression_) {
      case CompressionType::kNone: {
        std::ios_base::openmode mode = std::ios::binary;
        mode |= config_.append() ? std::ios::app : std::ios::trunc;
        std::ofstream output(config_.path(), mode);
        if (!output.is_open()) {
          FP_LOG_ERROR("file_sink failed to open file: " + config_.path());
          return;
        }

        output.write(reinterpret_cast<const char*>(payload.data()),
                     static_cast<std::streamsize>(payload.size()));
        if (!output.good()) {
          FP_LOG_ERROR("file_sink failed to write file: " + config_.path());
          return;
        }
        break;
      }
      case CompressionType::kGzip: {
        std::string mode = BuildGzipMode(config_.append(),
                                         config_.compression_level());
        gzFile file = gzopen(config_.path().c_str(), mode.c_str());
        if (!file) {
          FP_LOG_ERROR("file_sink failed to open gzip file: " + config_.path());
          return;
        }

        int written = gzwrite(file, payload.data(),
                              static_cast<unsigned int>(payload.size()));
        if (written == 0) {
          int err = 0;
          const char* error_message = gzerror(file, &err);
          std::string message = error_message ? error_message : "unknown";
          FP_LOG_ERROR("file_sink gzip write error: " + message);
          gzclose(file);
          return;
        }

        if (gzclose(file) != Z_OK) {
          FP_LOG_ERROR("file_sink failed to close gzip file: " + config_.path());
          return;
        }
        break;
      }
    }

    FP_LOG_DEBUG("file_sink wrote payload to file");
  }

private:
  FileSinkConfig config_{};
  CompressionType compression_{CompressionType::kNone};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating file_sink stage");
  return new FileSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying file_sink stage");
  delete stage;
}

}  // extern "C"
