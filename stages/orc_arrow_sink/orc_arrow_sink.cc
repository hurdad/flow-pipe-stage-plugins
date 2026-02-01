#include <arrow/adapters/orc/adapter.h>
#include <arrow/buffer.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <google/protobuf/struct.pb.h>
#include "flowpipe/protobuf_config.h"

#include <string>
#include <utility>
#include <vector>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "orc_arrow_sink.pb.h"
#include "util/arrow.h"

using namespace flowpipe;

using OrcArrowSinkConfig = flowpipe::stages::orc::arrow::sink::v1::OrcArrowSinkConfig;

namespace {
arrow::Compression::type ResolveCompression(arrow::common::Compression compression) {
  switch (compression) {
    case arrow::common::COMPRESSION_SNAPPY:
      return arrow::Compression::SNAPPY;
    case arrow::common::COMPRESSION_GZIP:
      return arrow::Compression::GZIP;
    case arrow::common::COMPRESSION_BROTLI:
      return arrow::Compression::BROTLI;
    case arrow::common::COMPRESSION_ZSTD:
      return arrow::Compression::ZSTD;
    case arrow::common::COMPRESSION_LZ4:
      return arrow::Compression::LZ4;
    case arrow::common::COMPRESSION_LZ4_FRAME:
      return arrow::Compression::LZ4_FRAME;
    case arrow::common::COMPRESSION_LZO:
      return arrow::Compression::LZO;
    case arrow::common::COMPRESSION_BZ2:
      return arrow::Compression::BZ2;
    case arrow::common::COMPRESSION_UNCOMPRESSED:
    case arrow::common::COMPRESSION_AUTO:
    default:
      return arrow::Compression::UNCOMPRESSED;
  }
}

arrow::adapters::orc::CompressionStrategy ResolveCompressionStrategy(
    OrcArrowSinkConfig::CompressionStrategy strategy) {
  switch (strategy) {
    case OrcArrowSinkConfig::COMPRESSION_STRATEGY_COMPRESSION:
      return arrow::adapters::orc::CompressionStrategy::kCompression;
    case OrcArrowSinkConfig::COMPRESSION_STRATEGY_SPEED:
    default:
      return arrow::adapters::orc::CompressionStrategy::kSpeed;
  }
}

arrow::adapters::orc::WriteOptions ResolveWriteOptions(
    const OrcArrowSinkConfig::WriteOptions& config) {
  auto options = arrow::adapters::orc::WriteOptions();
  if (config.batch_size() > 0) {
    options.batch_size = config.batch_size();
  }
  if (config.file_version().major_version() != 0 || config.file_version().minor_version() != 0) {
    options.file_version = arrow::adapters::orc::FileVersion(config.file_version().major_version(),
                                                             config.file_version().minor_version());
  }
  if (config.stripe_size() > 0) {
    options.stripe_size = config.stripe_size();
  }
  options.compression = ResolveCompression(config.compression());
  if (config.compression_block_size() > 0) {
    options.compression_block_size = config.compression_block_size();
  }
  options.compression_strategy = ResolveCompressionStrategy(config.compression_strategy());
  if (config.row_index_stride() > 0) {
    options.row_index_stride = config.row_index_stride();
  }
  if (config.padding_tolerance() > 0.0) {
    options.padding_tolerance = config.padding_tolerance();
  }
  if (config.dictionary_key_size_threshold() > 0.0) {
    options.dictionary_key_size_threshold = config.dictionary_key_size_threshold();
  }
  if (!config.bloom_filter_columns().empty()) {
    options.bloom_filter_columns.assign(config.bloom_filter_columns().begin(),
                                        config.bloom_filter_columns().end());
  }
  if (config.bloom_filter_fpp() > 0.0) {
    options.bloom_filter_fpp = config.bloom_filter_fpp();
  }
  return options;
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
    OrcArrowSinkConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<OrcArrowSinkConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("orc_arrow_sink invalid config: " + error);
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

    auto fs_result = ResolveFileSystem(config_.path(), config_.common());
    if (!fs_result.ok()) {
      FP_LOG_ERROR("orc_arrow_sink failed to resolve filesystem: " + fs_result.status().ToString());
      return;
    }

    auto fs_and_path = *fs_result;
    auto output_result = fs_and_path.first->OpenOutputStream(fs_and_path.second);
    if (!output_result.ok()) {
      FP_LOG_ERROR("orc_arrow_sink failed to open file: " + output_result.status().ToString());
      return;
    }

    auto output_stream = *output_result;
    auto write_options = ResolveWriteOptions(config_.write_options());
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
