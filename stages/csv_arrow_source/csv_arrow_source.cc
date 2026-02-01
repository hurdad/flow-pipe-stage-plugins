#include <arrow/buffer.h>
#include <arrow/csv/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/io/compressed.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <arrow/util/compression.h>
#include <google/protobuf/struct.pb.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "csv_arrow_source.pb.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "util/arrow.h"

using namespace flowpipe;

using CsvArrowSourceConfig = flowpipe::v1::stages::csv::arrow::source::v1::CsvArrowSourceConfig;

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
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(buffer_output, table->schema()));

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
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(buffer_output, batch->schema()));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  return buffer_output->Finish();
}

arrow::Result<std::shared_ptr<arrow::io::InputStream>> MaybeWrapCompressedInput(
    const std::shared_ptr<arrow::io::InputStream>& input, arrow::Compression::type compression) {
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
  if (!config.has_read_options()) {
    return options;
  }
  const auto& read_options = config.read_options();
  if (read_options.has_use_threads()) {
    options.use_threads = read_options.use_threads();
  }
  if (read_options.has_block_size()) {
    options.block_size = read_options.block_size();
  }
  if (read_options.has_skip_rows()) {
    options.skip_rows = read_options.skip_rows();
  }
  if (read_options.has_skip_rows_after_names()) {
    options.skip_rows_after_names = read_options.skip_rows_after_names();
  }
  if (read_options.has_autogenerate_column_names()) {
    options.autogenerate_column_names = read_options.autogenerate_column_names();
  }
  if (!read_options.column_names().empty()) {
    options.column_names.assign(read_options.column_names().begin(),
                                read_options.column_names().end());
  }
  return options;
}

arrow::csv::ParseOptions BuildParseOptions(const CsvArrowSourceConfig& config) {
  auto options = arrow::csv::ParseOptions::Defaults();
  if (!config.has_parse_options()) {
    return options;
  }
  const auto& parse_options = config.parse_options();
  options.delimiter = ResolveChar(parse_options.delimiter(), options.delimiter);
  if (parse_options.has_quoting()) {
    options.quoting = parse_options.quoting();
  }
  options.quote_char = ResolveChar(parse_options.quote_char(), options.quote_char);
  if (parse_options.has_escaping()) {
    options.escaping = parse_options.escaping();
  }
  if (!parse_options.escape_char().empty()) {
    options.escape_char = ResolveChar(parse_options.escape_char(), options.escape_char);
  }
  if (parse_options.has_double_quote()) {
    options.double_quote = parse_options.double_quote();
  }
  if (parse_options.has_newlines_in_values()) {
    options.newlines_in_values = parse_options.newlines_in_values();
  }
  if (parse_options.has_ignore_empty_lines()) {
    options.ignore_empty_lines = parse_options.ignore_empty_lines();
  }
  return options;
}

arrow::Result<arrow::csv::ConvertOptions> BuildConvertOptions(const CsvArrowSourceConfig& config) {
  auto options = arrow::csv::ConvertOptions::Defaults();
  if (!config.has_convert_options()) {
    return options;
  }
  const auto& convert_options = config.convert_options();
  if (convert_options.has_check_utf8()) {
    options.check_utf8 = convert_options.check_utf8();
  }
  if (convert_options.has_strings_can_be_null()) {
    options.strings_can_be_null = convert_options.strings_can_be_null();
  }
  if (convert_options.has_quoted_strings_can_be_null()) {
    options.quoted_strings_can_be_null = convert_options.quoted_strings_can_be_null();
  }
  if (convert_options.has_auto_dict_encode()) {
    options.auto_dict_encode = convert_options.auto_dict_encode();
  }
  if (convert_options.has_auto_dict_max_cardinality()) {
    options.auto_dict_max_cardinality =
        static_cast<int32_t>(convert_options.auto_dict_max_cardinality());
  }
  if (!convert_options.decimal_point().empty()) {
    options.decimal_point = ResolveChar(convert_options.decimal_point(), options.decimal_point);
  }
  if (!convert_options.null_values().empty()) {
    options.null_values.assign(convert_options.null_values().begin(),
                               convert_options.null_values().end());
  }
  if (!convert_options.true_values().empty()) {
    options.true_values.assign(convert_options.true_values().begin(),
                               convert_options.true_values().end());
  }
  if (!convert_options.false_values().empty()) {
    options.false_values.assign(convert_options.false_values().begin(),
                                convert_options.false_values().end());
  }
  if (!convert_options.include_columns().empty()) {
    options.include_columns.assign(convert_options.include_columns().begin(),
                                   convert_options.include_columns().end());
  }
  if (convert_options.has_include_missing_columns()) {
    options.include_missing_columns = convert_options.include_missing_columns();
  }
  if (!convert_options.column_types().empty()) {
    for (const auto& entry : convert_options.column_types()) {
      ARROW_ASSIGN_OR_RAISE(auto type, ConvertColumnType(entry.second));
      options.column_types.emplace(entry.first, std::move(type));
    }
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
  bool configure(const google::protobuf::Struct& config) override {
    CsvArrowSourceConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<CsvArrowSourceConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("csv_arrow_source invalid config: " + error);
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
    auto convert_options_result = BuildConvertOptions(config_);
    if (!convert_options_result.ok()) {
      FP_LOG_ERROR("csv_arrow_source invalid convert options: " +
                   convert_options_result.status().ToString());
      return false;
    }
    auto convert_options = *convert_options_result;

    auto fs_result = ResolveFileSystem(config_.path(), config_.common());
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

    auto compression_result =
        ResolveCompression(fs_and_path.second, config_.common().compression());
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
    auto reader_result = arrow::csv::TableReader::Make(
        io_context, *input_stream_result, read_options, parse_options, convert_options);
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

    if (config_.output_type() == flowpipe_arrow::common::OUTPUT_TYPE_RECORD_BATCH) {
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
    if (config_.output_type() == flowpipe_arrow::common::OUTPUT_TYPE_RECORD_BATCH) {
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
