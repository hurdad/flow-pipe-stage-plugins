#include <arrow/buffer.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/io/compressed.h>
#include <arrow/ipc/api.h>
#include <arrow/json/api.h>
#include <arrow/table.h>
#include <arrow/util/compression.h>
#include <google/protobuf/struct.pb.h>
#include "flowpipe/protobuf_config.h"

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "json_arrow_source.pb.h"
#include "util/arrow.h"

using namespace flowpipe;

using JsonArrowSourceConfig = flowpipe::stages::json::arrow::source::v1::JsonArrowSourceConfig;

namespace {
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

arrow::json::ReadOptions BuildReadOptions(const JsonArrowSourceConfig& config) {
  auto options = arrow::json::ReadOptions::Defaults();
  if (config.has_read_options()) {
    const auto& read_options = config.read_options();
    if (read_options.has_use_threads()) {
      options.use_threads = read_options.use_threads();
    }
    if (read_options.has_block_size()) {
      options.block_size = read_options.block_size();
    }
  }
  return options;
}

arrow::json::ParseOptions BuildParseOptions(const JsonArrowSourceConfig& config) {
  auto options = arrow::json::ParseOptions::Defaults();
  if (!config.has_parse_options()) {
    return options;
  }

  const auto& parse_options = config.parse_options();
  if (parse_options.has_newlines_in_values()) {
    options.newlines_in_values = parse_options.newlines_in_values();
  }

  switch (parse_options.unexpected_field_behavior()) {
    case JsonArrowSourceConfig::UNEXPECTED_FIELD_BEHAVIOR_IGNORE:
      options.unexpected_field_behavior = arrow::json::UnexpectedFieldBehavior::Ignore;
      break;
    case JsonArrowSourceConfig::UNEXPECTED_FIELD_BEHAVIOR_ERROR:
      options.unexpected_field_behavior = arrow::json::UnexpectedFieldBehavior::Error;
      break;
    case JsonArrowSourceConfig::UNEXPECTED_FIELD_BEHAVIOR_INFER_TYPE:
    default:
      options.unexpected_field_behavior = arrow::json::UnexpectedFieldBehavior::InferType;
      break;
  }

  if (!parse_options.explicit_schema().empty()) {
    FP_LOG_WARN("json_arrow_source explicit_schema is not yet supported; ignoring value");
  }

  return options;
}
}  // namespace

// ============================================================
// JsonArrowSource
// ============================================================
class JsonArrowSource final : public ISourceStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "json_arrow_source";
  }

  JsonArrowSource() {
    FP_LOG_INFO("json_arrow_source constructed");
  }

  ~JsonArrowSource() override {
    FP_LOG_INFO("json_arrow_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    JsonArrowSourceConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<JsonArrowSourceConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("json_arrow_source invalid config: " + error);
      return false;
    }

    if (cfg.path().empty()) {
      FP_LOG_ERROR("json_arrow_source requires path");
      return false;
    }

    config_ = std::move(cfg);
    record_batches_.clear();
    table_.reset();
    batch_index_ = 0;
    table_emitted_ = false;

    auto read_options = BuildReadOptions(config_);
    auto parse_options = BuildParseOptions(config_);

    auto fs_result = ResolveFileSystem(config_.path(), config_.common());
    if (!fs_result.ok()) {
      FP_LOG_ERROR("json_arrow_source failed to resolve filesystem: " +
                   fs_result.status().ToString());
      return false;
    }

    auto fs_and_path = *fs_result;
    auto input_result = fs_and_path.first->OpenInputFile(fs_and_path.second);
    if (!input_result.ok()) {
      FP_LOG_ERROR("json_arrow_source failed to open file: " + input_result.status().ToString());
      return false;
    }

    auto compression_result =
        ResolveCompression(fs_and_path.second, config_.common().compression());
    if (!compression_result.ok()) {
      FP_LOG_ERROR("json_arrow_source failed to resolve compression: " +
                   compression_result.status().ToString());
      return false;
    }

    auto input_stream_result = MaybeWrapCompressedInput(*input_result, *compression_result);
    if (!input_stream_result.ok()) {
      FP_LOG_ERROR("json_arrow_source failed to wrap compression: " +
                   input_stream_result.status().ToString());
      return false;
    }

    auto reader_result = arrow::json::TableReader::Make(
        arrow::default_memory_pool(), *input_stream_result, read_options, parse_options);
    if (!reader_result.ok()) {
      FP_LOG_ERROR("json_arrow_source failed to create reader: " +
                   reader_result.status().ToString());
      return false;
    }

    auto table_result = (*reader_result)->Read();
    if (!table_result.ok()) {
      FP_LOG_ERROR("json_arrow_source failed to read JSON: " + table_result.status().ToString());
      return false;
    }

    table_ = *table_result;

    if (config_.output_type() == arrow::common::OUTPUT_TYPE_RECORD_BATCH) {
      arrow::TableBatchReader reader(*table_);
      std::shared_ptr<arrow::RecordBatch> batch;
      while (true) {
        auto status = reader.ReadNext(&batch);
        if (!status.ok()) {
          FP_LOG_ERROR("json_arrow_source failed reading batches: " + status.ToString());
          return false;
        }
        if (!batch) {
          break;
        }
        record_batches_.push_back(batch);
      }
    }

    FP_LOG_INFO("json_arrow_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("json_arrow_source stop requested, skipping produce");
      return false;
    }

    if (!table_) {
      FP_LOG_ERROR("json_arrow_source table not loaded");
      return false;
    }

    std::shared_ptr<arrow::Buffer> buffer;
    if (config_.output_type() == arrow::common::OUTPUT_TYPE_RECORD_BATCH) {
      if (batch_index_ >= record_batches_.size()) {
        FP_LOG_DEBUG("json_arrow_source finished record batches");
        return false;
      }

      auto buffer_result = SerializeRecordBatch(record_batches_[batch_index_]);
      if (!buffer_result.ok()) {
        FP_LOG_ERROR("json_arrow_source failed to serialize record batch: " +
                     buffer_result.status().ToString());
        return false;
      }
      buffer = *buffer_result;
      ++batch_index_;
    } else {
      if (table_emitted_) {
        FP_LOG_DEBUG("json_arrow_source table already emitted");
        return false;
      }

      auto buffer_result = SerializeTable(table_);
      if (!buffer_result.ok()) {
        FP_LOG_ERROR("json_arrow_source failed to serialize table: " +
                     buffer_result.status().ToString());
        return false;
      }
      buffer = *buffer_result;
      table_emitted_ = true;
    }

    auto payload_buffer = AllocatePayloadBuffer(buffer->size());
    if (!payload_buffer) {
      FP_LOG_ERROR("json_arrow_source failed to allocate payload");
      return false;
    }

    if (buffer->size() > 0) {
      std::memcpy(payload_buffer.get(), buffer->data(), buffer->size());
    }

    payload = Payload(std::move(payload_buffer), buffer->size());
    FP_LOG_DEBUG("json_arrow_source produced payload");
    return true;
  }

 private:
  JsonArrowSourceConfig config_{};
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
  FP_LOG_INFO("creating json_arrow_source stage");
  return new JsonArrowSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying json_arrow_source stage");
  delete stage;
}

}  // extern "C"
