#include <arrow/buffer.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/struct.pb.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "parquet_arrow_sink.pb.h"
#include "util/arrow.h"

using namespace flowpipe;

using ParquetArrowSinkConfig =
    flowpipe::v1::stages::parquet::arrow::sink::v1::ParquetArrowSinkConfig;

namespace {
parquet::Compression::type ResolveCompression(flowpipe_arrow::common::Compression compression) {
  switch (compression) {
    case flowpipe_arrow::common::COMPRESSION_SNAPPY:
      return parquet::Compression::SNAPPY;
    case flowpipe_arrow::common::COMPRESSION_GZIP:
      return parquet::Compression::GZIP;
    case flowpipe_arrow::common::COMPRESSION_BROTLI:
      return parquet::Compression::BROTLI;
    case flowpipe_arrow::common::COMPRESSION_ZSTD:
      return parquet::Compression::ZSTD;
    case flowpipe_arrow::common::COMPRESSION_LZ4:
      return parquet::Compression::LZ4;
    case flowpipe_arrow::common::COMPRESSION_LZ4_FRAME:
      return parquet::Compression::LZ4_FRAME;
    case flowpipe_arrow::common::COMPRESSION_LZO:
      return parquet::Compression::LZO;
    case flowpipe_arrow::common::COMPRESSION_BZ2:
      return parquet::Compression::BZ2;
    case flowpipe_arrow::common::COMPRESSION_UNCOMPRESSED:
    case flowpipe_arrow::common::COMPRESSION_AUTO:
    default:
      return parquet::Compression::UNCOMPRESSED;
  }
}

parquet::Encoding::type ResolveEncoding(ParquetArrowSinkConfig::Encoding encoding) {
  switch (encoding) {
    case ParquetArrowSinkConfig::ENCODING_PLAIN:
      return parquet::Encoding::PLAIN;
    case ParquetArrowSinkConfig::ENCODING_PLAIN_DICTIONARY:
      return parquet::Encoding::PLAIN_DICTIONARY;
    case ParquetArrowSinkConfig::ENCODING_RLE:
      return parquet::Encoding::RLE;
    case ParquetArrowSinkConfig::ENCODING_BIT_PACKED:
      return parquet::Encoding::BIT_PACKED;
    case ParquetArrowSinkConfig::ENCODING_DELTA_BINARY_PACKED:
      return parquet::Encoding::DELTA_BINARY_PACKED;
    case ParquetArrowSinkConfig::ENCODING_DELTA_LENGTH_BYTE_ARRAY:
      return parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY;
    case ParquetArrowSinkConfig::ENCODING_DELTA_BYTE_ARRAY:
      return parquet::Encoding::DELTA_BYTE_ARRAY;
    case ParquetArrowSinkConfig::ENCODING_RLE_DICTIONARY:
      return parquet::Encoding::RLE_DICTIONARY;
    case ParquetArrowSinkConfig::ENCODING_BYTE_STREAM_SPLIT:
      return parquet::Encoding::BYTE_STREAM_SPLIT;
    case ParquetArrowSinkConfig::ENCODING_UNSPECIFIED:
    default:
      return parquet::Encoding::UNKNOWN;
  }
}

parquet::ParquetVersion::type ResolveParquetVersion(
    ParquetArrowSinkConfig::ParquetVersion version) {
  switch (version) {
    case ParquetArrowSinkConfig::PARQUET_VERSION_1_0:
      return parquet::ParquetVersion::PARQUET_1_0;
    case ParquetArrowSinkConfig::PARQUET_VERSION_2_6:
    case ParquetArrowSinkConfig::PARQUET_VERSION_UNSPECIFIED:
    default:
      return parquet::ParquetVersion::PARQUET_2_6;
  }
}

parquet::ParquetDataPageVersion ResolveDataPageVersion(
    ParquetArrowSinkConfig::DataPageVersion version) {
  switch (version) {
    case ParquetArrowSinkConfig::DATA_PAGE_VERSION_V2:
      return parquet::ParquetDataPageVersion::V2;
    case ParquetArrowSinkConfig::DATA_PAGE_VERSION_V1:
    case ParquetArrowSinkConfig::DATA_PAGE_VERSION_UNSPECIFIED:
    default:
      return parquet::ParquetDataPageVersion::V1;
  }
}

parquet::SizeStatisticsLevel ResolveSizeStatisticsLevel(
    ParquetArrowSinkConfig::SizeStatisticsLevel level) {
  switch (level) {
    case ParquetArrowSinkConfig::SIZE_STATISTICS_LEVEL_NONE:
      return parquet::SizeStatisticsLevel::None;
    case ParquetArrowSinkConfig::SIZE_STATISTICS_LEVEL_COLUMN_CHUNK:
      return parquet::SizeStatisticsLevel::ColumnChunk;
    case ParquetArrowSinkConfig::SIZE_STATISTICS_LEVEL_PAGE_AND_COLUMN_CHUNK:
      return parquet::SizeStatisticsLevel::PageAndColumnChunk;
    case ParquetArrowSinkConfig::SIZE_STATISTICS_LEVEL_UNSPECIFIED:
    default:
      return parquet::SizeStatisticsLevel::PageAndColumnChunk;
  }
}

parquet::CdcOptions ResolveCdcOptions(const ParquetArrowSinkConfig::CdcOptions& options) {
  parquet::CdcOptions resolved;
  if (options.min_chunk_size() > 0) {
    resolved.min_chunk_size = options.min_chunk_size();
  }
  if (options.max_chunk_size() > 0) {
    resolved.max_chunk_size = options.max_chunk_size();
  }
  resolved.norm_level = options.norm_level();
  return resolved;
}

void ApplyColumnProperties(const ParquetArrowSinkConfig::WriterProperties& config,
                           parquet::WriterProperties::Builder* builder) {
  for (const auto& [path, column] : config.column_properties()) {
    if (column.has_dictionary_enabled()) {
      if (column.dictionary_enabled()) {
        builder->enable_dictionary(path);
      } else {
        builder->disable_dictionary(path);
      }
    }

    if (column.has_statistics_enabled()) {
      if (column.statistics_enabled()) {
        builder->enable_statistics(path);
      } else {
        builder->disable_statistics(path);
      }
    }

    if (column.has_page_index_enabled()) {
      if (column.page_index_enabled()) {
        builder->enable_write_page_index(path);
      } else {
        builder->disable_write_page_index(path);
      }
    }

    if (column.has_encoding()) {
      builder->encoding(path, ResolveEncoding(column.encoding()));
    }

    if (column.has_compression()) {
      builder->compression(path, ResolveCompression(column.compression()));
    }

    if (column.has_compression_level()) {
      builder->compression_level(path, column.compression_level());
    }
  }
}

void ApplyWriterProperties(const ParquetArrowSinkConfig& config,
                           parquet::WriterProperties::Builder* builder) {
  const auto& properties = config.writer_properties();

  if (properties.has_dictionary_page_size_limit()) {
    builder->dictionary_pagesize_limit(properties.dictionary_page_size_limit());
  }
  if (properties.has_write_batch_size()) {
    builder->write_batch_size(properties.write_batch_size());
  }
  if (properties.has_max_row_group_length()) {
    builder->max_row_group_length(properties.max_row_group_length());
  }
  if (properties.has_data_page_size()) {
    builder->data_pagesize(properties.data_page_size());
  }
  if (properties.has_max_rows_per_page()) {
    builder->max_rows_per_page(properties.max_rows_per_page());
  }
  if (properties.has_data_page_version()) {
    builder->data_page_version(ResolveDataPageVersion(properties.data_page_version()));
  }
  if (properties.has_version()) {
    builder->version(ResolveParquetVersion(properties.version()));
  }
  if (properties.has_created_by()) {
    builder->created_by(properties.created_by());
  }
  if (properties.has_store_decimal_as_integer()) {
    if (properties.store_decimal_as_integer()) {
      builder->enable_store_decimal_as_integer();
    } else {
      builder->disable_store_decimal_as_integer();
    }
  }
  if (properties.has_page_checksum_enabled()) {
    if (properties.page_checksum_enabled()) {
      builder->enable_page_checksum();
    } else {
      builder->disable_page_checksum();
    }
  }
  if (properties.has_size_statistics_level()) {
    builder->set_size_statistics_level(
        ResolveSizeStatisticsLevel(properties.size_statistics_level()));
  }
  if (properties.has_dictionary_enabled()) {
    if (properties.dictionary_enabled()) {
      builder->enable_dictionary();
    } else {
      builder->disable_dictionary();
    }
  }
  if (properties.has_statistics_enabled()) {
    if (properties.statistics_enabled()) {
      builder->enable_statistics();
    } else {
      builder->disable_statistics();
    }
  }
  if (properties.has_page_index_enabled()) {
    if (properties.page_index_enabled()) {
      builder->enable_write_page_index();
    } else {
      builder->disable_write_page_index();
    }
  }
  if (properties.has_encoding()) {
    builder->encoding(ResolveEncoding(properties.encoding()));
  }
  if (properties.has_compression()) {
    builder->compression(ResolveCompression(properties.compression()));
  } else {
    builder->compression(ResolveCompression(config.common().compression()));
  }
  if (properties.has_compression_level()) {
    builder->compression_level(properties.compression_level());
  }
  if (properties.has_max_statistics_size()) {
    builder->max_statistics_size(properties.max_statistics_size());
  }
  if (properties.has_content_defined_chunking_enabled()) {
    if (properties.content_defined_chunking_enabled()) {
      builder->enable_content_defined_chunking();
    } else {
      builder->disable_content_defined_chunking();
    }
  }
  if (properties.has_content_defined_chunking_options()) {
    builder->content_defined_chunking_options(
        ResolveCdcOptions(properties.content_defined_chunking_options()));
  }

  if (!properties.sorting_columns().empty()) {
    std::vector<parquet::SortingColumn> sorting_columns;
    sorting_columns.reserve(properties.sorting_columns().size());
    for (const auto& column : properties.sorting_columns()) {
      sorting_columns.push_back(
          parquet::SortingColumn{column.column_index(), column.descending(), column.nulls_first()});
    }
    builder->set_sorting_columns(std::move(sorting_columns));
  }

  ApplyColumnProperties(properties, builder);
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

std::string JoinPath(std::string base, const std::string& child) {
  if (base.empty()) {
    return child;
  }
  if (base.back() != '/') {
    base.push_back('/');
  }
  return base + child;
}

int64_t ResolveRowGroupSize(const ParquetArrowSinkConfig& config,
                            const std::shared_ptr<arrow::Table>& table) {
  int64_t row_group_size = static_cast<int64_t>(table->num_rows());
  if (config.has_row_group_size() && config.row_group_size() > 0) {
    row_group_size = config.row_group_size();
  } else if (config.has_writer_properties() &&
             config.writer_properties().has_max_row_group_length() &&
             config.writer_properties().max_row_group_length() > 0) {
    row_group_size = config.writer_properties().max_row_group_length();
  }
  return row_group_size;
}

arrow::Result<std::shared_ptr<arrow::Table>> BuildPartitionTable(
    const std::shared_ptr<arrow::Table>& table, const std::vector<int64_t>& rows) {
  std::vector<std::shared_ptr<arrow::Table>> slices;
  slices.reserve(rows.size());
  for (auto row : rows) {
    slices.push_back(table->Slice(row, 1));
  }
  return arrow::ConcatenateTables(slices);
}

arrow::Result<std::shared_ptr<arrow::Table>> DropPartitionColumns(
    const std::shared_ptr<arrow::Table>& table,
    const google::protobuf::RepeatedPtrField<std::string>& partition_columns) {
  if (partition_columns.empty()) {
    return table;
  }

  std::unordered_set<std::string> partition_names;
  partition_names.reserve(static_cast<size_t>(partition_columns.size()));
  for (const auto& name : partition_columns) {
    partition_names.insert(name);
  }

  std::vector<int> column_indices;
  column_indices.reserve(static_cast<size_t>(table->num_columns()));
  for (int index = 0; index < table->num_columns(); ++index) {
    if (partition_names.count(table->schema()->field(index)->name()) == 0) {
      column_indices.push_back(index);
    }
  }

  if (column_indices.size() == static_cast<size_t>(table->num_columns())) {
    return table;
  }

  return table->SelectColumns(column_indices);
}

arrow::Result<std::unordered_map<std::string, std::vector<int64_t>>> BuildPartitionRowMap(
    const std::shared_ptr<arrow::Table>& table,
    const google::protobuf::RepeatedPtrField<std::string>& partition_columns) {
  std::unordered_map<std::string, std::vector<int64_t>> partition_rows;
  const std::string kHiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";
  int64_t num_rows = table->num_rows();
  for (int64_t row = 0; row < num_rows; ++row) {
    std::vector<std::string> segments;
    segments.reserve(static_cast<size_t>(partition_columns.size()));
    for (const auto& name : partition_columns) {
      auto column = table->GetColumnByName(name);
      if (!column) {
        return arrow::Status::Invalid("parquet_arrow_sink missing partition column: ", name);
      }
      ARROW_ASSIGN_OR_RAISE(auto scalar, column->GetScalar(row));
      std::string value = scalar->is_valid ? scalar->ToString() : kHiveDefaultPartition;
      segments.push_back(name + "=" + value);
    }

    std::string key;
    for (size_t index = 0; index < segments.size(); ++index) {
      key = JoinPath(key, segments[index]);
    }
    partition_rows[key].push_back(row);
  }

  return partition_rows;
}

arrow::Status WriteHivePartitionedDataset(
    const ParquetArrowSinkConfig& config, const std::shared_ptr<arrow::fs::FileSystem>& filesystem,
    const std::string& base_dir, const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<parquet::WriterProperties>& properties) {
  const auto& write_opts = config.write_opts();
  if (write_opts.partition_columns().empty()) {
    return arrow::Status::Invalid(
        "parquet_arrow_sink write_opts.partition_columns is required for hive partitioning");
  }
  ARROW_ASSIGN_OR_RAISE(auto partition_rows,
                        BuildPartitionRowMap(table, write_opts.partition_columns()));

  int partition_index = 0;
  for (const auto& [partition_path, rows] : partition_rows) {
    ARROW_ASSIGN_OR_RAISE(auto partition_table, BuildPartitionTable(table, rows));
    ARROW_ASSIGN_OR_RAISE(auto data_table,
                          DropPartitionColumns(partition_table, write_opts.partition_columns()));

    std::string dir_path = JoinPath(base_dir, partition_path);
    ARROW_RETURN_NOT_OK(filesystem->CreateDir(dir_path, true));

    std::string filename = "part-" + std::to_string(partition_index) + ".parquet";
    if (write_opts.has_basename_template() && !write_opts.basename_template().empty()) {
      filename = write_opts.basename_template();
      auto replace_pos = filename.find("{i}");
      if (replace_pos != std::string::npos) {
        filename.replace(replace_pos, 3, std::to_string(partition_index));
      }
    }
    std::string file_path = JoinPath(dir_path, filename);

    ARROW_ASSIGN_OR_RAISE(auto output, filesystem->OpenOutputStream(file_path));
    auto status = parquet::arrow::WriteTable(*data_table, arrow::default_memory_pool(), output,
                                             ResolveRowGroupSize(config, data_table), properties);
    if (!status.ok()) {
      return status;
    }
    ARROW_RETURN_NOT_OK(output->Close());
    ++partition_index;
  }
  return arrow::Status::OK();
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
  bool configure(const google::protobuf::Struct& config) override {
    ParquetArrowSinkConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<ParquetArrowSinkConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("parquet_arrow_sink invalid config: " + error);
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

    auto fs_result = ResolveFileSystem(config_.path(), config_.common());
    if (!fs_result.ok()) {
      FP_LOG_ERROR("parquet_arrow_sink failed to resolve filesystem: " +
                   fs_result.status().ToString());
      return;
    }

    auto fs_and_path = *fs_result;

    parquet::WriterProperties::Builder builder;
    if (config_.has_writer_properties()) {
      ApplyWriterProperties(config_, &builder);
    } else {
      builder.compression(ResolveCompression(config_.common().compression()));
    }
    auto properties = builder.build();

    if (config_.has_write_opts()) {
      auto status = WriteHivePartitionedDataset(config_, fs_and_path.first, fs_and_path.second,
                                                *table_result, properties);
      if (!status.ok()) {
        FP_LOG_ERROR("parquet_arrow_sink failed to write parquet dataset: " + status.ToString());
        return;
      }

      FP_LOG_DEBUG("parquet_arrow_sink wrote arrow payload to parquet dataset");
      return;
    }

    auto output_result = fs_and_path.first->OpenOutputStream(fs_and_path.second);
    if (!output_result.ok()) {
      FP_LOG_ERROR("parquet_arrow_sink failed to open file: " + output_result.status().ToString());
      return;
    }

    auto status =
        parquet::arrow::WriteTable(**table_result, arrow::default_memory_pool(), *output_result,
                                   ResolveRowGroupSize(config_, *table_result), properties);
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
