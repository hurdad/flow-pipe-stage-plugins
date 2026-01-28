#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <arrow/csv/api.h>
#include <arrow/buffer.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/io/compressed.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <arrow/util/compression.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "csv_arrow_source.pb.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"

using namespace flowpipe;

using CsvArrowSourceConfig = flowpipe::stages::csv::arrow::source::v1::CsvArrowSourceConfig;

namespace {
char ResolveChar(const std::string& value, char fallback) {
  if (value.empty()) {
    return fallback;
  }
  return value[0];
}

arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeTable(
    const std::shared_ptr<arrow::Table>& table) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output, arrow::io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(auto writer,
                        arrow::ipc::MakeStreamWriter(buffer_output, table->schema()));

  arrow::TableBatchReader reader(*table);
  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    ARROW_RETURN_NOT_OK(reader.ReadNext(&batch));
    if (!batch) {
      break;
    }
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }

  ARROW_RETURN_NOT_OK(writer->Close());
  return buffer_output->Finish();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeRecordBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output, arrow::io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(auto writer,
                        arrow::ipc::MakeStreamWriter(buffer_output, batch->schema()));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  return buffer_output->Finish();
}

arrow::Result<std::pair<std::shared_ptr<arrow::fs::FileSystem>, std::string>> ResolveFileSystem(
    const CsvArrowSourceConfig& config) {
  std::string path = config.path();
  switch (config.filesystem()) {
    case CsvArrowSourceConfig::FILE_SYSTEM_LOCAL: {
      return std::make_pair(std::make_shared<arrow::fs::LocalFileSystem>(), path);
    }
    case CsvArrowSourceConfig::FILE_SYSTEM_S3:
    case CsvArrowSourceConfig::FILE_SYSTEM_GCS:
    case CsvArrowSourceConfig::FILE_SYSTEM_HDFS: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(path, &path));
      return std::make_pair(std::move(fs), path);
    }
    case CsvArrowSourceConfig::FILE_SYSTEM_AUTO:
    default: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUriOrPath(path, &path));
      return std::make_pair(std::move(fs), path);
    }
  }
}

arrow::Result<arrow::Compression::type> ResolveCompression(
    const CsvArrowSourceConfig& config, const std::string& path) {
  switch (config.compression()) {
    case CsvArrowSourceConfig::COMPRESSION_UNCOMPRESSED:
      return arrow::Compression::UNCOMPRESSED;
    case CsvArrowSourceConfig::COMPRESSION_SNAPPY:
      return arrow::Compression::SNAPPY;
    case CsvArrowSourceConfig::COMPRESSION_GZIP:
      return arrow::Compression::GZIP;
    case CsvArrowSourceConfig::COMPRESSION_BROTLI:
      return arrow::Compression::BROTLI;
    case CsvArrowSourceConfig::COMPRESSION_ZSTD:
      return arrow::Compression::ZSTD;
    case CsvArrowSourceConfig::COMPRESSION_LZ4:
      return arrow::Compression::LZ4;
    case CsvArrowSourceConfig::COMPRESSION_LZ4_FRAME:
      return arrow::Compression::LZ4_FRAME;
    case CsvArrowSourceConfig::COMPRESSION_LZO:
      return arrow::Compression::LZO;
    case CsvArrowSourceConfig::COMPRESSION_BZ2:
      return arrow::Compression::BZ2;
    case CsvArrowSourceConfig::COMPRESSION_AUTO:
    default:
      return arrow::util::Codec::GetCompressionType(path);
  }
}

arrow::Result<std::shared_ptr<arrow::io::InputStream>> MaybeWrapCompressedInput(
    const std::shared_ptr<arrow::io::InputStream>& input,
    arrow::Compression::type compression) {
  if (compression == arrow::Compression::UNCOMPRESSED) {
    return input;
  }
  ARROW_ASSIGN_OR_RAISE(auto codec, arrow::util::Codec::Create(compression));
  ARROW_ASSIGN_OR_RAISE(auto compressed_stream,
                        arrow::io::CompressedInputStream::Make(codec.get(), input));
  return compressed_stream;
}

arrow::csv::ReadOptions BuildReadOptions(const CsvArrowSourceConfig& config) {
  auto options = arrow::csv::ReadOptions::Defaults();
  if (config.has_use_threads()) {
    options.use_threads = config.use_threads();
  }
  if (config.has_block_size()) {
    options.block_size = config.block_size();
  }
  if (config.has_skip_rows()) {
    options.skip_rows = config.skip_rows();
  }
  if (config.has_autogenerate_column_names()) {
    options.autogenerate_column_names = config.autogenerate_column_names();
  }
  if (!config.column_names().empty()) {
    options.column_names.assign(config.column_names().begin(), config.column_names().end());
  }
  return options;
}

arrow::csv::ParseOptions BuildParseOptions(const CsvArrowSourceConfig& config) {
  auto options = arrow::csv::ParseOptions::Defaults();
  options.delimiter = ResolveChar(config.delimiter(), options.delimiter);
  options.quote_char = ResolveChar(config.quote_char(), options.quote_char);
  if (!config.escape_char().empty()) {
    options.escape_char = ResolveChar(config.escape_char(), options.escape_char);
  }
  if (config.has_double_quote()) {
    options.double_quote = config.double_quote();
  }
  if (config.has_newlines_in_values()) {
    options.newlines_in_values = config.newlines_in_values();
  }
  return options;
}

arrow::csv::ConvertOptions BuildConvertOptions(const CsvArrowSourceConfig& config) {
  auto options = arrow::csv::ConvertOptions::Defaults();
  if (config.has_check_utf8()) {
    options.check_utf8 = config.check_utf8();
  }
  if (config.has_strings_can_be_null()) {
    options.strings_can_be_null = config.strings_can_be_null();
  }
  if (config.has_quoted_strings_can_be_null()) {
    options.quoted_strings_can_be_null = config.quoted_strings_can_be_null();
  }
  if (!config.null_values().empty()) {
    options.null_values.assign(config.null_values().begin(), config.null_values().end());
  }
  if (!config.true_values().empty()) {
    options.true_values.assign(config.true_values().begin(), config.true_values().end());
  }
  if (!config.false_values().empty()) {
    options.false_values.assign(config.false_values().begin(), config.false_values().end());
  }
  return options;
}
}  // namespace

// ============================================================
// CsvArrowSource
// ============================================================
class CsvArrowSource final : public ISourceStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "csv_arrow_source";
  }

  CsvArrowSource() {
    FP_LOG_INFO("csv_arrow_source constructed");
  }

  ~CsvArrowSource() override {
    FP_LOG_INFO("csv_arrow_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("csv_arrow_source failed to serialize config");
      return false;
    }

    CsvArrowSourceConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("csv_arrow_source invalid config");
      return false;
    }

    if (cfg.path().empty()) {
      FP_LOG_ERROR("csv_arrow_source requires path");
      return false;
    }

    config_ = std::move(cfg);
    record_batches_.clear();
    table_.reset();
    batch_index_ = 0;
    table_emitted_ = false;

    auto read_options = BuildReadOptions(config_);
    auto parse_options = BuildParseOptions(config_);
    auto convert_options = BuildConvertOptions(config_);

    auto fs_result = ResolveFileSystem(config_);
    if (!fs_result.ok()) {
      FP_LOG_ERROR("csv_arrow_source failed to resolve filesystem: " +
                   fs_result.status().ToString());
      return false;
    }

    auto fs_and_path = *fs_result;
    auto input_result = fs_and_path.first->OpenInputFile(fs_and_path.second);
    if (!input_result.ok()) {
      FP_LOG_ERROR("csv_arrow_source failed to open file: " + input_result.status().ToString());
      return false;
    }

    auto compression_result = ResolveCompression(config_, fs_and_path.second);
    if (!compression_result.ok()) {
      FP_LOG_ERROR("csv_arrow_source failed to resolve compression: " +
                   compression_result.status().ToString());
      return false;
    }

    auto input_stream_result = MaybeWrapCompressedInput(*input_result, *compression_result);
    if (!input_stream_result.ok()) {
      FP_LOG_ERROR("csv_arrow_source failed to wrap compression: " +
                   input_stream_result.status().ToString());
      return false;
    }

    arrow::io::IOContext io_context = arrow::io::default_io_context();
    auto reader_result = arrow::csv::TableReader::Make(io_context, *input_stream_result,
                                                       read_options, parse_options, convert_options);
    if (!reader_result.ok()) {
      FP_LOG_ERROR("csv_arrow_source failed to create reader: " +
                   reader_result.status().ToString());
      return false;
    }

    auto table_result = (*reader_result)->Read();
    if (!table_result.ok()) {
      FP_LOG_ERROR("csv_arrow_source failed to read CSV: " + table_result.status().ToString());
      return false;
    }

    table_ = *table_result;

    if (config_.output_type() == CsvArrowSourceConfig::OUTPUT_TYPE_RECORD_BATCH) {
      arrow::TableBatchReader reader(*table_);
      std::shared_ptr<arrow::RecordBatch> batch;
      while (true) {
        auto status = reader.ReadNext(&batch);
        if (!status.ok()) {
          FP_LOG_ERROR("csv_arrow_source failed reading batches: " + status.ToString());
          return false;
        }
        if (!batch) {
          break;
        }
        record_batches_.push_back(batch);
      }
    }

    FP_LOG_INFO("csv_arrow_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("csv_arrow_source stop requested, skipping produce");
      return false;
    }

    if (!table_) {
      FP_LOG_ERROR("csv_arrow_source table not loaded");
      return false;
    }

    std::shared_ptr<arrow::Buffer> buffer;
    if (config_.output_type() == CsvArrowSourceConfig::OUTPUT_TYPE_RECORD_BATCH) {
      if (batch_index_ >= record_batches_.size()) {
        FP_LOG_DEBUG("csv_arrow_source finished record batches");
        return false;
      }

      auto buffer_result = SerializeRecordBatch(record_batches_[batch_index_]);
      if (!buffer_result.ok()) {
        FP_LOG_ERROR("csv_arrow_source failed to serialize record batch: " +
                     buffer_result.status().ToString());
        return false;
      }
      buffer = *buffer_result;
      ++batch_index_;
    } else {
      if (table_emitted_) {
        FP_LOG_DEBUG("csv_arrow_source table already emitted");
        return false;
      }

      auto buffer_result = SerializeTable(table_);
      if (!buffer_result.ok()) {
        FP_LOG_ERROR("csv_arrow_source failed to serialize table: " +
                     buffer_result.status().ToString());
        return false;
      }
      buffer = *buffer_result;
      table_emitted_ = true;
    }

    auto payload_buffer = AllocatePayloadBuffer(buffer->size());
    if (!payload_buffer) {
      FP_LOG_ERROR("csv_arrow_source failed to allocate payload");
      return false;
    }

    if (buffer->size() > 0) {
      std::memcpy(payload_buffer.get(), buffer->data(), buffer->size());
    }

    payload = Payload(std::move(payload_buffer), buffer->size());
    FP_LOG_DEBUG("csv_arrow_source produced payload");
    return true;
  }

 private:
  CsvArrowSourceConfig config_{};
  std::shared_ptr<arrow::Table> table_{};
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches_{};
  size_t batch_index_{0};
  bool table_emitted_{false};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating csv_arrow_source stage");
  return new CsvArrowSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying csv_arrow_source stage");
  delete stage;
}

}  // extern "C"
