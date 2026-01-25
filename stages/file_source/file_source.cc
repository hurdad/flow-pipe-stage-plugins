#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

#include "file_source.pb.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <zlib.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

using namespace flowpipe;

using FileSourceConfig = flowpipe::stages::file::source::v1::FileSourceConfig;

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
                      const std::string& path,
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
  if (normalized == "auto") {
    if (path.size() >= 3 && path.rfind(".gz") == path.size() - 3) {
      out_type = CompressionType::kGzip;
    } else {
      out_type = CompressionType::kNone;
    }
    return true;
  }

  return false;
}

bool ReadFileRaw(const std::string& path, std::vector<char>& out) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    return false;
  }

  input.seekg(0, std::ios::end);
  std::streamsize size = input.tellg();
  if (size < 0) {
    return false;
  }
  input.seekg(0, std::ios::beg);

  out.resize(static_cast<size_t>(size));
  if (size == 0) {
    return true;
  }

  if (!input.read(out.data(), size)) {
    return false;
  }

  return true;
}

bool ReadFileGzip(const std::string& path, std::vector<char>& out, std::string& error) {
  gzFile file = gzopen(path.c_str(), "rb");
  if (!file) {
    error = "failed to open gzip file";
    return false;
  }

  constexpr int kChunkSize = 8192;
  std::vector<char> buffer(kChunkSize);
  out.clear();

  while (true) {
    int read = gzread(file, buffer.data(), static_cast<unsigned int>(buffer.size()));
    if (read > 0) {
      out.insert(out.end(), buffer.begin(), buffer.begin() + read);
      continue;
    }
    if (read == 0) {
      break;
    }

    int err = 0;
    const char* error_message = gzerror(file, &err);
    if (err != Z_OK) {
      error = error_message ? error_message : "gzip read error";
      gzclose(file);
      return false;
    }
  }

  if (gzclose(file) != Z_OK) {
    error = "failed to close gzip file";
    return false;
  }

  return true;
}
}  // namespace

// ============================================================
// FileSource
// ============================================================
class FileSource final
    : public ISourceStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "file_source";
  }

  FileSource() {
    FP_LOG_INFO("file_source constructed");
  }

  ~FileSource() override {
    FP_LOG_INFO("file_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("file_source failed to serialize config");
      return false;
    }

    FileSourceConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("file_source invalid config");
      return false;
    }

    if (cfg.path().empty()) {
      FP_LOG_ERROR("file_source requires path");
      return false;
    }

    CompressionType compression;
    if (!ParseCompression(cfg.compression(), cfg.path(), compression)) {
      FP_LOG_ERROR("file_source unsupported compression: " + cfg.compression());
      return false;
    }

    config_ = std::move(cfg);
    compression_ = compression;
    produced_ = false;

    FP_LOG_INFO("file_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("file_source stop requested, skipping produce");
      return false;
    }

    if (produced_) {
      return false;
    }

    std::vector<char> data;
    switch (compression_) {
      case CompressionType::kNone: {
        if (!ReadFileRaw(config_.path(), data)) {
          FP_LOG_ERROR("file_source failed to read file: " + config_.path());
          return false;
        }
        break;
      }
      case CompressionType::kGzip: {
        std::string error;
        if (!ReadFileGzip(config_.path(), data, error)) {
          FP_LOG_ERROR("file_source gzip error: " + error);
          return false;
        }
        break;
      }
    }

    auto buffer = AllocatePayloadBuffer(data.size());
    if (!buffer) {
      FP_LOG_ERROR("file_source failed to allocate payload");
      return false;
    }

    if (!data.empty()) {
      std::memcpy(buffer.get(), data.data(), data.size());
    }

    payload = Payload(std::move(buffer), data.size());
    produced_ = true;

    FP_LOG_DEBUG("file_source produced payload from file");
    return true;
  }

private:
  FileSourceConfig config_{};
  CompressionType compression_{CompressionType::kNone};
  bool produced_{false};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating file_source stage");
  return new FileSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying file_source stage");
  delete stage;
}

}  // extern "C"
