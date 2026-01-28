#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <arrow/adapters/orc/adapter.h>
#include <arrow/buffer.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "orc_arrow_sink.pb.h"

using namespace flowpipe;

using OrcArrowSinkConfig = flowpipe::stages::orc::arrow::sink::v1::OrcArrowSinkConfig;

namespace {
arrow::adapters::orc::Compression::type MapCompressionType(
    OrcArrowSinkConfig::OrcWriterOptions::CompressionType compression,
    arrow::adapters::orc::Compression::type fallback) {
  switch (compression) {
    case OrcArrowSinkConfig::OrcWriterOptions::COMPRESSION_UNCOMPRESSED:
      return arrow::adapters::orc::Compression::UNCOMPRESSED;
    case OrcArrowSinkConfig::OrcWriterOptions::COMPRESSION_SNAPPY:
      return arrow::adapters::orc::Compression::SNAPPY;
    case OrcArrowSinkConfig::OrcWriterOptions::COMPRESSION_GZIP:
      return arrow::adapters::orc::Compression::ZLIB;
    case OrcArrowSinkConfig::OrcWriterOptions::COMPRESSION_BROTLI:
      return arrow::adapters::orc::Compression::BROTLI;
    case OrcArrowSinkConfig::OrcWriterOptions::COMPRESSION_ZSTD:
      return arrow::adapters::orc::Compression::ZSTD;
    case OrcArrowSinkConfig::OrcWriterOptions::COMPRESSION_LZ4:
      return arrow::adapters::orc::Compression::LZ4;
    case OrcArrowSinkConfig::OrcWriterOptions::COMPRESSION_TYPE_UNSPECIFIED:
    default:
      return fallback;
  }
}

arrow::adapters::orc::CompressionStrategy MapCompressionStrategy(
    OrcArrowSinkConfig::OrcWriterOptions::CompressionStrategy strategy,
    arrow::adapters::orc::CompressionStrategy fallback) {
  switch (strategy) {
    case OrcArrowSinkConfig::OrcWriterOptions::COMPRESSION_STRATEGY_SPEED:
      return arrow::adapters::orc::CompressionStrategy::kSpeed;
    case OrcArrowSinkConfig::OrcWriterOptions::COMPRESSION_STRATEGY_COMPRESSION:
      return arrow::adapters::orc::CompressionStrategy::kCompression;
    case OrcArrowSinkConfig::OrcWriterOptions::COMPRESSION_STRATEGY_UNSPECIFIED:
    default:
      return fallback;
  }
}

arrow::adapters::orc::WriteOptions BuildWriteOptions(const OrcArrowSinkConfig& config) {
  auto options = arrow::adapters::orc::WriteOptions();
  if (!config.has_writer_options()) {
    return options;
  }

  const auto& writer_options = config.writer_options();
  if (writer_options.has_batch_size()) {
    options.batch_size = writer_options.batch_size();
  }

  if (writer_options.has_file_version_major() || writer_options.has_file_version_minor()) {
    const int32_t major =
        writer_options.has_file_version_major() ? writer_options.file_version_major() : 0;
    const int32_t minor =
        writer_options.has_file_version_minor() ? writer_options.file_version_minor() : 12;
    options.file_version = arrow::adapters::orc::FileVersion(major, minor);
  }

  if (writer_options.has_stripe_size()) {
    options.stripe_size = writer_options.stripe_size();
  }

  if (writer_options.has_compression()) {
    options.compression =
        MapCompressionType(writer_options.compression(), options.compression);
  }

  if (writer_options.has_compression_block_size()) {
    options.compression_block_size = writer_options.compression_block_size();
  }

  if (writer_options.has_compression_strategy()) {
    options.compression_strategy =
        MapCompressionStrategy(writer_options.compression_strategy(),
                               options.compression_strategy);
  }

  if (writer_options.has_row_index_stride()) {
    options.row_index_stride = writer_options.row_index_stride();
  }

  if (writer_options.has_padding_tolerance()) {
    options.padding_tolerance = writer_options.padding_tolerance();
  }

  if (writer_options.has_dictionary_key_size_threshold()) {
    options.dictionary_key_size_threshold = writer_options.dictionary_key_size_threshold();
  }

  if (writer_options.bloom_filter_columns_size() > 0) {
    options.bloom_filter_columns = std::vector<int64_t>(
        writer_options.bloom_filter_columns().begin(),
        writer_options.bloom_filter_columns().end());
  }

  if (writer_options.has_bloom_filter_fpp()) {
    options.bloom_filter_fpp = writer_options.bloom_filter_fpp();
  }

  return options;
}

arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>> ResolveFileSystem(
    const OrcArrowSinkConfig& config) {
  std::string path = config.path();
  switch (config.filesystem()) {
    case OrcArrowSinkConfig::FILE_SYSTEM_LOCAL: {
      return std::make_pair(std::make_shared<arrow::fs::LocalFileSystem>(), path);
    }
    case OrcArrowSinkConfig::FILE_SYSTEM_S3:
    case OrcArrowSinkConfig::FILE_SYSTEM_GCS:
    case OrcArrowSinkConfig::FILE_SYSTEM_HDFS: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(path, &path));
      return std::make_pair(std::move(fs), path);
    }
    case OrcArrowSinkConfig::FILE_SYSTEM_AUTO:
    default: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUriOrPath(path, &path));
      return std::make_pair(std::move(fs), path);
    }
  }
}

arrow::Result<std::shared_ptr<arrow::Table>> ReadTableFromPayload(const Payload& payload) {
  auto buffer = arrow::Buffer::Wrap(payload.data(), static_cast<int64_t>(payload.size));
  auto reader = std::make_shared<arrow::io::BufferReader>(buffer);

  ARROW_ASSIGN_OR_RAISE(auto stream_reader, arrow::ipc::RecordBatchStreamReader::Open(reader));

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, stream_reader->Next());
    if (!batch) {
      break;
    }
    batches.push_back(batch);
  }

  return arrow::Table::FromRecordBatches(stream_reader->schema(), batches);
}
}  // namespace

// ============================================================
// OrcArrowSink
// ============================================================
class OrcArrowSink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "orc_arrow_sink";
  }

  OrcArrowSink() {
    FP_LOG_INFO("orc_arrow_sink constructed");
  }

  ~OrcArrowSink() override {
    FP_LOG_INFO("orc_arrow_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("orc_arrow_sink failed to serialize config");
      return false;
    }

    OrcArrowSinkConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("orc_arrow_sink invalid config");
      return false;
    }

    if (cfg.path().empty()) {
      FP_LOG_ERROR("orc_arrow_sink requires path");
      return false;
    }

    config_ = std::move(cfg);
    FP_LOG_INFO("orc_arrow_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("orc_arrow_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("orc_arrow_sink received empty payload");
      return;
    }

    auto table_result = ReadTableFromPayload(payload);
    if (!table_result.ok()) {
      FP_LOG_ERROR("orc_arrow_sink failed to read arrow payload: " +
                   table_result.status().ToString());
      return;
    }

    auto fs_result = ResolveFileSystem(config_);
    if (!fs_result.ok()) {
      FP_LOG_ERROR("orc_arrow_sink failed to resolve filesystem: " +
                   fs_result.status().ToString());
      return;
    }

    auto fs_and_path = *fs_result;
    auto output_result = fs_and_path.first->OpenOutputStream(fs_and_path.second);
    if (!output_result.ok()) {
      FP_LOG_ERROR("orc_arrow_sink failed to open file: " + output_result.status().ToString());
      return;
    }

    auto output_stream = *output_result;
    auto write_options = BuildWriteOptions(config_);
    auto writer_result =
        arrow::adapters::orc::ORCFileWriter::Open(output_stream.get(), write_options);
    if (!writer_result.ok()) {
      FP_LOG_ERROR("orc_arrow_sink failed to create ORC writer: " +
                   writer_result.status().ToString());
      return;
    }

    auto status = (*writer_result)->Write(**table_result);
    if (!status.ok()) {
      FP_LOG_ERROR("orc_arrow_sink failed to write ORC: " + status.ToString());
      return;
    }

    status = (*writer_result)->Close();
    if (!status.ok()) {
      FP_LOG_ERROR("orc_arrow_sink failed to close ORC writer: " + status.ToString());
      return;
    }

    FP_LOG_DEBUG("orc_arrow_sink wrote arrow payload to ORC");
  }

 private:
  OrcArrowSinkConfig config_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating orc_arrow_sink stage");
  return new OrcArrowSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying orc_arrow_sink stage");
  delete stage;
}

}  // extern "C"
