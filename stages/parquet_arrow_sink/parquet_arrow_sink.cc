#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <arrow/buffer.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <string>
#include <vector>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "parquet_arrow_sink.pb.h"

using namespace flowpipe;

using ParquetArrowSinkConfig = flowpipe::stages::parquet::arrow::sink::v1::ParquetArrowSinkConfig;

namespace {
arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>> ResolveFileSystem(
    const ParquetArrowSinkConfig& config) {
  std::string path = config.path();
  switch (config.filesystem()) {
    case ParquetArrowSinkConfig::FILE_SYSTEM_LOCAL: {
      return std::make_pair(std::make_shared<arrow::fs::LocalFileSystem>(), path);
    }
    case ParquetArrowSinkConfig::FILE_SYSTEM_S3:
    case ParquetArrowSinkConfig::FILE_SYSTEM_GCS:
    case ParquetArrowSinkConfig::FILE_SYSTEM_HDFS: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(path, &path));
      return std::make_pair(std::move(fs), path);
    }
    case ParquetArrowSinkConfig::FILE_SYSTEM_AUTO:
    default: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUriOrPath(path, &path));
      return std::make_pair(std::move(fs), path);
    }
  }
}

parquet::Compression::type ResolveCompression(ParquetArrowSinkConfig::Compression compression) {
  switch (compression) {
    case ParquetArrowSinkConfig::COMPRESSION_SNAPPY:
      return parquet::Compression::SNAPPY;
    case ParquetArrowSinkConfig::COMPRESSION_GZIP:
      return parquet::Compression::GZIP;
    case ParquetArrowSinkConfig::COMPRESSION_BROTLI:
      return parquet::Compression::BROTLI;
    case ParquetArrowSinkConfig::COMPRESSION_ZSTD:
      return parquet::Compression::ZSTD;
    case ParquetArrowSinkConfig::COMPRESSION_LZ4:
      return parquet::Compression::LZ4;
    case ParquetArrowSinkConfig::COMPRESSION_LZ4_FRAME:
      return parquet::Compression::LZ4_FRAME;
    case ParquetArrowSinkConfig::COMPRESSION_LZO:
      return parquet::Compression::LZO;
    case ParquetArrowSinkConfig::COMPRESSION_BZ2:
      return parquet::Compression::BZ2;
    case ParquetArrowSinkConfig::COMPRESSION_UNCOMPRESSED:
    default:
      return parquet::Compression::UNCOMPRESSED;
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
// ParquetArrowSink
// ============================================================
class ParquetArrowSink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "parquet_arrow_sink";
  }

  ParquetArrowSink() {
    FP_LOG_INFO("parquet_arrow_sink constructed");
  }

  ~ParquetArrowSink() override {
    FP_LOG_INFO("parquet_arrow_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("parquet_arrow_sink failed to serialize config");
      return false;
    }

    ParquetArrowSinkConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("parquet_arrow_sink invalid config");
      return false;
    }

    if (cfg.path().empty()) {
      FP_LOG_ERROR("parquet_arrow_sink requires path");
      return false;
    }

    config_ = std::move(cfg);
    FP_LOG_INFO("parquet_arrow_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("parquet_arrow_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("parquet_arrow_sink received empty payload");
      return;
    }

    auto table_result = ReadTableFromPayload(payload);
    if (!table_result.ok()) {
      FP_LOG_ERROR("parquet_arrow_sink failed to read arrow payload: " +
                   table_result.status().ToString());
      return;
    }

    auto fs_result = ResolveFileSystem(config_);
    if (!fs_result.ok()) {
      FP_LOG_ERROR("parquet_arrow_sink failed to resolve filesystem: " +
                   fs_result.status().ToString());
      return;
    }

    auto fs_and_path = *fs_result;
    auto output_result = fs_and_path.first->OpenOutputStream(fs_and_path.second);
    if (!output_result.ok()) {
      FP_LOG_ERROR("parquet_arrow_sink failed to open file: " +
                   output_result.status().ToString());
      return;
    }

    parquet::WriterProperties::Builder builder;
    builder.compression(ResolveCompression(config_.compression()));
    auto properties = builder.build();

    int64_t row_group_size = static_cast<int64_t>((*table_result)->num_rows());
    if (config_.has_row_group_size() && config_.row_group_size() > 0) {
      row_group_size = config_.row_group_size();
    }

    auto status = parquet::arrow::WriteTable(**table_result, arrow::default_memory_pool(),
                                             *output_result, row_group_size, properties);
    if (!status.ok()) {
      FP_LOG_ERROR("parquet_arrow_sink failed to write parquet: " + status.ToString());
      return;
    }

    FP_LOG_DEBUG("parquet_arrow_sink wrote arrow payload to parquet");
  }

 private:
  ParquetArrowSinkConfig config_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating parquet_arrow_sink stage");
  return new ParquetArrowSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying parquet_arrow_sink stage");
  delete stage;
}

}  // extern "C"
