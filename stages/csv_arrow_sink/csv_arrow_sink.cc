#include <arrow/buffer.h>
#include <arrow/csv/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/io/compressed.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <arrow/util/compression.h>
#include <google/protobuf/struct.pb.h>

#include <string>
#include <vector>

#include "csv_arrow_sink.pb.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "util/arrow.h"

using namespace flowpipe;

using CsvArrowSinkConfig = flowpipe::stages::csv::arrow::sink::v1::CsvArrowSinkConfig;

namespace {
char ResolveChar(const std::string& value, char fallback) {
  if (value.empty()) {
    return fallback;
  }
  return value[0];
}

arrow::csv::QuotingStyle ResolveQuotingStyle(CsvArrowSinkConfig::QuotingStyle style) {
  switch (style) {
    case CsvArrowSinkConfig::QUOTING_STYLE_ALL_VALID:
      return arrow::csv::QuotingStyle::AllValid;
    case CsvArrowSinkConfig::QUOTING_STYLE_NONE:
      return arrow::csv::QuotingStyle::None;
    case CsvArrowSinkConfig::QUOTING_STYLE_NEEDED:
    default:
      return arrow::csv::QuotingStyle::Needed;
  }
}

arrow::Result<std::shared_ptr<arrow::io::OutputStream>> MaybeWrapCompressedOutput(
    const std::shared_ptr<arrow::io::OutputStream>& output, arrow::Compression::type compression) {
  if (compression == arrow::Compression::UNCOMPRESSED) {
    return output;
  }
  ARROW_ASSIGN_OR_RAISE(auto codec, arrow::util::Codec::Create(compression));
  ARROW_ASSIGN_OR_RAISE(auto compressed_stream,
                        arrow::io::CompressedOutputStream::Make(codec.get(), output));
  return compressed_stream;
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

arrow::csv::WriteOptions BuildWriteOptions(const CsvArrowSinkConfig& config) {
  auto options = arrow::csv::WriteOptions::Defaults();
  if (!config.has_write_options()) {
    return options;
  }
  const auto& write_options = config.write_options();
  if (write_options.has_include_header()) {
    options.include_header = write_options.include_header();
  }
  if (write_options.has_batch_size()) {
    options.batch_size = static_cast<int32_t>(write_options.batch_size());
  }
  options.delimiter = ResolveChar(write_options.delimiter(), options.delimiter);
  if (write_options.has_null_string()) {
    options.null_string = write_options.null_string();
  }
  if (!write_options.eol().empty()) {
    options.eol = write_options.eol();
  }
  if (write_options.has_quoting_style()) {
    options.quoting_style = ResolveQuotingStyle(write_options.quoting_style());
  }
  if (write_options.has_quoting_header()) {
    options.quoting_header = ResolveQuotingStyle(write_options.quoting_header());
  }
  return options;
}
}  // namespace

// ============================================================
// CsvArrowSink
// ============================================================
class CsvArrowSink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "csv_arrow_sink";
  }

  CsvArrowSink() {
    FP_LOG_INFO("csv_arrow_sink constructed");
  }

  ~CsvArrowSink() override {
    FP_LOG_INFO("csv_arrow_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    CsvArrowSinkConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<CsvArrowSinkConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("csv_arrow_sink invalid config: " + error);
      return false;
    }

    if (cfg.path().empty()) {
      FP_LOG_ERROR("csv_arrow_sink requires path");
      return false;
    }

    config_ = std::move(cfg);
    FP_LOG_INFO("csv_arrow_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("csv_arrow_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("csv_arrow_sink received empty payload");
      return;
    }

    auto table_result = ReadTableFromPayload(payload);
    if (!table_result.ok()) {
      FP_LOG_ERROR("csv_arrow_sink failed to read arrow payload: " +
                   table_result.status().ToString());
      return;
    }

    auto write_options = BuildWriteOptions(config_);
    auto fs_result = ResolveFileSystem(config_.path(), config_.common());
    if (!fs_result.ok()) {
      FP_LOG_ERROR("csv_arrow_sink failed to resolve filesystem: " + fs_result.status().ToString());
      return;
    }

    auto fs_and_path = *fs_result;
    arrow::Result<std::shared_ptr<arrow::io::OutputStream>> output_result;
    if (config_.append()) {
      output_result = fs_and_path.first->OpenAppendStream(fs_and_path.second);
    } else {
      output_result = fs_and_path.first->OpenOutputStream(fs_and_path.second);
    }

    if (!output_result.ok()) {
      FP_LOG_ERROR("csv_arrow_sink failed to open file: " + output_result.status().ToString());
      return;
    }

    auto compression_result =
        ResolveCompression(fs_and_path.second, config_.common().compression());
    if (!compression_result.ok()) {
      FP_LOG_ERROR("csv_arrow_sink failed to resolve compression: " +
                   compression_result.status().ToString());
      return;
    }

    auto output_stream_result = MaybeWrapCompressedOutput(*output_result, *compression_result);
    if (!output_stream_result.ok()) {
      FP_LOG_ERROR("csv_arrow_sink failed to wrap compression: " +
                   output_stream_result.status().ToString());
      return;
    }

    auto status = arrow::csv::WriteCSV(**table_result, write_options, output_stream_result->get());
    if (!status.ok()) {
      FP_LOG_ERROR("csv_arrow_sink failed to write CSV: " + status.ToString());
      return;
    }

    FP_LOG_DEBUG("csv_arrow_sink wrote arrow payload to CSV");
  }

 private:
  CsvArrowSinkConfig config_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating csv_arrow_sink stage");
  return new CsvArrowSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying csv_arrow_sink stage");
  delete stage;
}

}  // extern "C"
