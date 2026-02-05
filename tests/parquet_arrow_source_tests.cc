#include <arrow/compute/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/partition.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/writer.h>

#include <filesystem>
#include <unordered_map>

#include "arrow_stage_test_support.h"

#define flowpipe_create_stage flowpipe_create_stage_parquet_arrow_source
#define flowpipe_destroy_stage flowpipe_destroy_stage_parquet_arrow_source
#include "stages/parquet_arrow_source/parquet_arrow_source.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

using flowpipe_stage_tests::BuildPathConfig;
using flowpipe_stage_tests::BuildSampleTable;
using flowpipe_stage_tests::MakeTempPath;
using flowpipe_stage_tests::ReadTableFromPayload;

namespace {
std::shared_ptr<arrow::Table> SortTable(const std::shared_ptr<arrow::Table>& table) {
  arrow::compute::SortOptions options(
      {arrow::compute::SortKey("id", arrow::compute::SortOrder::Ascending),
       arrow::compute::SortKey("name", arrow::compute::SortOrder::Ascending)});
  auto indices = arrow::compute::SortIndices(table, options).ValueOrDie();
  return arrow::compute::Take(table, indices).ValueOrDie().table();
}

void WriteHivePartitionedDataset(const std::filesystem::path& path,
                                 const std::shared_ptr<arrow::Table>& table) {
  std::filesystem::create_directories(path);
  auto combined_result = table->CombineChunks(arrow::default_memory_pool());
  ASSERT_TRUE(combined_result.ok());
  auto combined = *combined_result;

  auto id_column = combined->GetColumnByName("id");
  ASSERT_NE(id_column, nullptr);
  ASSERT_EQ(id_column->num_chunks(), 1);
  auto id_array = std::static_pointer_cast<arrow::Int64Array>(id_column->chunk(0));

  std::unordered_map<int64_t, std::vector<int64_t>> partitions;
  for (int64_t row = 0; row < combined->num_rows(); ++row) {
    if (!id_array->IsValid(row)) {
      continue;
    }
    partitions[id_array->Value(row)].push_back(row);
  }

  int partition_index = 0;
  for (const auto& [id_value, rows] : partitions) {
    arrow::Int64Builder indices_builder;
    ASSERT_TRUE(indices_builder.AppendValues(rows).ok());
    std::shared_ptr<arrow::Array> indices;
    ASSERT_TRUE(indices_builder.Finish(&indices).ok());

    auto take_result = arrow::compute::Take(combined, indices);
    ASSERT_TRUE(take_result.ok());
    auto partition_table = (*take_result).table();

    auto partition_path = path / ("id=" + std::to_string(id_value));
    std::filesystem::create_directories(partition_path);
    auto file_path = partition_path / ("part-" + std::to_string(partition_index) + ".parquet");

    auto output_result = arrow::io::FileOutputStream::Open(file_path.string());
    ASSERT_TRUE(output_result.ok());
    auto output_stream = *output_result;

    auto status = parquet::arrow::WriteTable(*partition_table, arrow::default_memory_pool(),
                                             output_stream, partition_table->num_rows());
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(output_stream->Close().ok());
    ++partition_index;
  }
}
}  // namespace

TEST(ParquetArrowSourceTest, ReadsParquetToArrowTable) {
  auto path = MakeTempPath("input.parquet");
  auto expected = BuildSampleTable();

  auto output_result = arrow::io::FileOutputStream::Open(path.string());
  ASSERT_TRUE(output_result.ok());
  auto output_stream = *output_result;

  auto status = parquet::arrow::WriteTable(*expected, arrow::default_memory_pool(), output_stream,
                                           expected->num_rows());
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(output_stream->Close().ok());

  ParquetArrowSource stage;
  auto config = BuildPathConfig(path);

  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  flowpipe::Payload payload;
  ASSERT_TRUE(stage.produce(ctx, payload));

  auto table = ReadTableFromPayload(payload);
  EXPECT_TRUE(table->Equals(*expected));
}

TEST(ParquetArrowSourceTest, ReadsHivePartitionedParquetDataset) {
  auto path = MakeTempPath("input_dataset");
  auto expected = BuildSampleTable();

  WriteHivePartitionedDataset(path, expected);

  ParquetArrowSource stage;
  auto config = BuildPathConfig(path);

  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  flowpipe::Payload payload;
  ASSERT_TRUE(stage.produce(ctx, payload));

  auto table = ReadTableFromPayload(payload);
  EXPECT_TRUE(SortTable(table)->Equals(*SortTable(expected)));
}
