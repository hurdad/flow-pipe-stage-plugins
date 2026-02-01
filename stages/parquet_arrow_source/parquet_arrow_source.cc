#include <arrow/buffer.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/partition.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <google/protobuf/struct.pb.h>
#include "flowpipe/protobuf_config.h"

#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "parquet_arrow_source.pb.h"
#include "util/arrow.h"

using namespace flowpipe;

using ParquetArrowSourceConfig =
    flowpipe::stages::parquet::arrow::source::v1::ParquetArrowSourceConfig;

namespace {
bool IsHiveSegment(const std::filesystem::path& segment) {
  auto segment_string = segment.string();
  auto separator = segment_string.find('=');
  return separator != std::string::npos && separator > 0 &&
         separator + 1 < segment_string.size();
}

std::filesystem::path PartitionBaseDirForFile(const std::filesystem::path& file_path) {
  auto parent = file_path.parent_path();
  std::filesystem::path base;
  bool found_hive_segment = false;
  for (const auto& part : parent) {
    if (IsHiveSegment(part)) {
      found_hive_segment = true;
      break;
    }
    base /= part;
  }

  if (!found_hive_segment) {
    return parent;
  }

  if (base.empty()) {
    if (parent.has_root_path()) {
      return parent.root_path();
    }
    return ".";
  }

  return base;
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

arrow::Result<std::shared_ptr<arrow::Table>> ReadParquetTable(
    const std::shared_ptr<arrow::fs::FileSystem>& filesystem, const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto info, filesystem->GetFileInfo(path));

  auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
  arrow::dataset::FileSystemFactoryOptions options;
  options.partitioning = arrow::dataset::HivePartitioning::MakeFactory();

  std::shared_ptr<arrow::dataset::DatasetFactory> factory;
  if (info.type() == arrow::fs::FileType::File) {
    std::vector<arrow::fs::FileInfo> files{info};
    options.partition_base_dir =
        PartitionBaseDirForFile(std::filesystem::path(info.path())).string();
    ARROW_ASSIGN_OR_RAISE(
        factory,
        arrow::dataset::FileSystemDatasetFactory::Make(filesystem, files, format, options));
  } else if (info.type() == arrow::fs::FileType::Directory) {
    arrow::fs::FileSelector selector;
    selector.base_dir = info.path();
    selector.recursive = true;
    options.partition_base_dir = info.path();
    ARROW_ASSIGN_OR_RAISE(
        factory,
        arrow::dataset::FileSystemDatasetFactory::Make(filesystem, selector, format, options));
  } else {
    return arrow::Status::Invalid("Unsupported parquet path: ", path);
  }

  ARROW_ASSIGN_OR_RAISE(auto dataset, factory->Finish());
  ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
  ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
  return scanner->ToTable();
}
}  // namespace

// ============================================================
// ParquetArrowSource
// ============================================================
class ParquetArrowSource final : public ISourceStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "parquet_arrow_source";
  }

  ParquetArrowSource() {
    FP_LOG_INFO("parquet_arrow_source constructed");
  }

  ~ParquetArrowSource() override {
    FP_LOG_INFO("parquet_arrow_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    ParquetArrowSourceConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<ParquetArrowSourceConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("parquet_arrow_source invalid config: " + error);
      return false;
    }

    if (cfg.path().empty()) {
      FP_LOG_ERROR("parquet_arrow_source requires path");
      return false;
    }

    config_ = std::move(cfg);
    record_batches_.clear();
    table_.reset();
    batch_index_ = 0;
    table_emitted_ = false;

    auto fs_result = ResolveFileSystem(config_.path(), config_.common());
    if (!fs_result.ok()) {
      FP_LOG_ERROR("parquet_arrow_source failed to resolve filesystem: " +
                   fs_result.status().ToString());
      return false;
    }

    auto fs_and_path = *fs_result;
    auto table_result = ReadParquetTable(fs_and_path.first, fs_and_path.second);
    if (!table_result.ok()) {
      FP_LOG_ERROR("parquet_arrow_source failed to read parquet: " +
                   table_result.status().ToString());
      return false;
    }

    table_ = *table_result;

    if (config_.output_type() == arrow::common::OUTPUT_TYPE_RECORD_BATCH) {
      arrow::TableBatchReader reader(*table_);
      if (config_.has_batch_size() && config_.batch_size() > 0) {
        reader.set_chunksize(static_cast<int64_t>(config_.batch_size()));
      }
      std::shared_ptr<arrow::RecordBatch> batch;
      while (true) {
        auto status = reader.ReadNext(&batch);
        if (!status.ok()) {
          FP_LOG_ERROR("parquet_arrow_source failed reading batches: " + status.ToString());
          return false;
        }
        if (!batch) {
          break;
        }
        record_batches_.push_back(batch);
      }
    }

    FP_LOG_INFO("parquet_arrow_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("parquet_arrow_source stop requested, skipping produce");
      return false;
    }

    if (!table_) {
      FP_LOG_ERROR("parquet_arrow_source table not loaded");
      return false;
    }

    std::shared_ptr<arrow::Buffer> buffer;
    if (config_.output_type() == arrow::common::OUTPUT_TYPE_RECORD_BATCH) {
      if (batch_index_ >= record_batches_.size()) {
        FP_LOG_DEBUG("parquet_arrow_source finished record batches");
        return false;
      }

      auto buffer_result = SerializeRecordBatch(record_batches_[batch_index_]);
      if (!buffer_result.ok()) {
        FP_LOG_ERROR("parquet_arrow_source failed to serialize record batch: " +
                     buffer_result.status().ToString());
        return false;
      }
      buffer = *buffer_result;
      ++batch_index_;
    } else {
      if (table_emitted_) {
        FP_LOG_DEBUG("parquet_arrow_source table already emitted");
        return false;
      }

      auto buffer_result = SerializeTable(table_);
      if (!buffer_result.ok()) {
        FP_LOG_ERROR("parquet_arrow_source failed to serialize table: " +
                     buffer_result.status().ToString());
        return false;
      }
      buffer = *buffer_result;
      table_emitted_ = true;
    }

    auto payload_buffer = AllocatePayloadBuffer(buffer->size());
    if (!payload_buffer) {
      FP_LOG_ERROR("parquet_arrow_source failed to allocate payload");
      return false;
    }

    if (buffer->size() > 0) {
      std::memcpy(payload_buffer.get(), buffer->data(), buffer->size());
    }

    payload = Payload(std::move(payload_buffer), buffer->size());
    FP_LOG_DEBUG("parquet_arrow_source produced payload");
    return true;
  }

 private:
  ParquetArrowSourceConfig config_{};
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
  FP_LOG_INFO("creating parquet_arrow_source stage");
  return new ParquetArrowSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying parquet_arrow_source stage");
  delete stage;
}

}  // extern "C"
