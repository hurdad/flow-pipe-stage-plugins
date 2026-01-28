#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <arrow/buffer.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/io/compressed.h>
#include <arrow/ipc/api.h>
#include <arrow/json/api.h>
#include <arrow/table.h>
#include <arrow/util/compression.h>

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

using namespace flowpipe;

using JsonArrowSourceConfig = flowpipe::stages::json::arrow::source::v1::JsonArrowSourceConfig;

namespace {
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
    const JsonArrowSourceConfig& config) {
  std::string path = config.path();
  switch (config.filesystem()) {
    case JsonArrowSourceConfig::FILE_SYSTEM_LOCAL: {
      return std::make_pair(std::make_shared<arrow::fs::LocalFileSystem>(), path);
    }
    case JsonArrowSourceConfig::FILE_SYSTEM_S3:
    case JsonArrowSourceConfig::FILE_SYSTEM_GCS:
    case JsonArrowSourceConfig::FILE_SYSTEM_HDFS: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(path, &path));
      return std::make_pair(std::move(fs), path);
    }
    case JsonArrowSourceConfig::FILE_SYSTEM_AUTO:
    default: {
      ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUriOrPath(path, &path));
      return std::make_pair(std::move(fs), path);
    }
  }
}

arrow::Result<arrow::Compression::type> ResolveCompression(
    const JsonArrowSourceConfig& config, const std::string& path) {
  switch (config.compression()) {
    case JsonArrowSourceConfig::COMPRESSION_UNCOMPRESSED:
      return arrow::Compression::UNCOMPRESSED;
    case JsonArrowSourceConfig::COMPRESSION_SNAPPY:
      return arrow::Compression::SNAPPY;
    case JsonArrowSourceConfig::COMPRESSION_GZIP:
      return arrow::Compression::GZIP;
    case JsonArrowSourceConfig::COMPRESSION_BROTLI:
      return arrow::Compression::BROTLI;
    case JsonArrowSourceConfig::COMPRESSION_ZSTD:
      return arrow::Compression::ZSTD;
    case JsonArrowSourceConfig::COMPRESSION_LZ4:
      return arrow::Compression::LZ4;
    case JsonArrowSourceConfig::COMPRESSION_LZ4_FRAME:
      return arrow::Compression::LZ4_FRAME;
    case JsonArrowSourceConfig::COMPRESSION_LZO:
      return arrow::Compression::LZO;
    case JsonArrowSourceConfig::COMPRESSION_BZ2:
      return arrow::Compression::BZ2;
    case JsonArrowSourceConfig::COMPRESSION_AUTO:
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

arrow::json::ReadOptions BuildReadOptions(const JsonArrowSourceConfig& config) {
  auto options = arrow::json::ReadOptions::Defaults();
  if (config.has_use_threads()) {
    options.use_threads = config.use_threads();
  }
  if (config.has_block_size()) {
    options.block_size = config.block_size();
  }
  return options;
}

arrow::json::ParseOptions BuildParseOptions() {
  return arrow::json::ParseOptions::Defaults();
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
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("json_arrow_source failed to serialize config");
      return false;
    }

    JsonArrowSourceConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("json_arrow_source invalid config");
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
    auto parse_options = BuildParseOptions();

    auto fs_result = ResolveFileSystem(config_);
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

    auto compression_result = ResolveCompression(config_, fs_and_path.second);
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

    arrow::io::IOContext io_context = arrow::io::default_io_context();
    auto reader_result = arrow::json::TableReader::Make(io_context, *input_stream_result,
                                                        read_options, parse_options);
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

    if (config_.output_type() == JsonArrowSourceConfig::OUTPUT_TYPE_RECORD_BATCH) {
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
    if (config_.output_type() == JsonArrowSourceConfig::OUTPUT_TYPE_RECORD_BATCH) {
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
