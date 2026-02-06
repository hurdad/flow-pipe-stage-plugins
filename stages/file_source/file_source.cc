#include <google/protobuf/struct.pb.h>
#include <zlib.h>

#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "file_source.pb.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"

using namespace flowpipe;

using FileSourceConfig = flowpipe::v1::stages::file::source::v1::FileSourceConfig;

namespace file_source_internal {
using CompressionType = FileSourceConfig::CompressionType;

bool ResolveCompression(CompressionType compression, const std::string& path,
                        CompressionType& out_type) {
  switch (compression) {
    case FileSourceConfig::COMPRESSION_UNSPECIFIED:
    case FileSourceConfig::COMPRESSION_NONE:
      out_type = FileSourceConfig::COMPRESSION_NONE;
      return true;
    case FileSourceConfig::COMPRESSION_GZIP:
      out_type = FileSourceConfig::COMPRESSION_GZIP;
      return true;
    case FileSourceConfig::COMPRESSION_AUTO:
      if (path.size() >= 3 && path.rfind(".gz") == path.size() - 3) {
        out_type = FileSourceConfig::COMPRESSION_GZIP;
      } else {
        out_type = FileSourceConfig::COMPRESSION_NONE;
      }
      return true;
    default:
      return false;
  }
}

bool ReadFileRaw(const std::string& path, size_t max_bytes, std::vector<char>& out,
                 std::string& error) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    error = "failed to open file";
    return false;
  }

  input.seekg(0, std::ios::end);
  std::streamsize size = input.tellg();
  if (size < 0) {
    error = "failed to read file size";
    return false;
  }
  if (max_bytes > 0 && static_cast<size_t>(size) > max_bytes) {
    size = static_cast<std::streamsize>(max_bytes);
  }
  input.seekg(0, std::ios::beg);

  out.resize(static_cast<size_t>(size));
  if (size == 0) {
    return true;
  }

  if (!input.read(out.data(), size)) {
    error = "failed to read file contents";
    return false;
  }

  return true;
}

bool ReadFileGzip(const std::string& path, size_t max_bytes, std::vector<char>& out,
                  std::string& error) {
  gzFile file = gzopen(path.c_str(), "rb");
  if (!file) {
    error = "failed to open gzip file";
    return false;
  }

  constexpr int kChunkSize = 8192;
  std::vector<char> buffer(kChunkSize);
  out.clear();

  while (true) {
    size_t read_limit = buffer.size();
    if (max_bytes > 0) {
      size_t remaining = max_bytes - out.size();
      if (remaining == 0) {
        break;
      }
      if (remaining < read_limit) {
        read_limit = remaining;
      }
    }

    int read = gzread(file, buffer.data(), static_cast<unsigned int>(read_limit));
    if (read > 0) {
      if (max_bytes > 0 && out.size() + static_cast<size_t>(read) > max_bytes) {
        error = "gzip data exceeds max_bytes limit";
        gzclose(file);
        return false;
      }
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
}  // namespace file_source_internal

// ============================================================
// FileSource
// ============================================================
class FileSource final : public ISourceStage, public ConfigurableStage {
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
  bool configure(const google::protobuf::Struct& config) override {
    FileSourceConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<FileSourceConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("file_source invalid config: " + error);
      return false;
    }

    if (cfg.path().empty()) {
      FP_LOG_ERROR("file_source requires path");
      return false;
    }

    file_source_internal::CompressionType compression;
    if (!file_source_internal::ResolveCompression(cfg.compression(), cfg.path(), compression)) {
      FP_LOG_ERROR("file_source unsupported compression enum value");
      return false;
    }

    config_ = std::move(cfg);
    compression_ = compression;
    max_bytes_ = static_cast<size_t>(config_.max_bytes());
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
      case FileSourceConfig::COMPRESSION_NONE: {
        std::string error;
        if (!file_source_internal::ReadFileRaw(config_.path(), max_bytes_, data, error)) {
          FP_LOG_ERROR("file_source failed to read file: " + error);
          return false;
        }
        break;
      }
      case FileSourceConfig::COMPRESSION_GZIP: {
        std::string error;
        if (!file_source_internal::ReadFileGzip(config_.path(), max_bytes_, data, error)) {
          FP_LOG_ERROR("file_source gzip error: " + error);
          return false;
        }
        break;
      }
      case FileSourceConfig::COMPRESSION_UNSPECIFIED:
      case FileSourceConfig::COMPRESSION_AUTO:
      default:
        FP_LOG_ERROR("file_source invalid resolved compression");
        return false;
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
  file_source_internal::CompressionType compression_{FileSourceConfig::COMPRESSION_NONE};
  size_t max_bytes_{0};
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
