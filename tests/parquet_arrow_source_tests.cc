#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/partition.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/writer.h>

#include <filesystem>

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
void WriteHivePartitionedDataset(const std::filesystem::path& path,
                                 const std::shared_ptr<arrow::Table>& table) {
  std::filesystem::create_directories(path);
  auto filesystem = std::make_shared<arrow::fs::LocalFileSystem>();
  auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();

  auto factory = arrow::dataset::HivePartitioning::MakeFactory();
  auto partitioning_result =
      factory->Finish(arrow::schema({arrow::field("id", arrow::int64())}));
  ASSERT_TRUE(partitioning_result.ok());

  arrow::dataset::FileSystemDatasetWriteOptions write_options;
  write_options.file_write_options = format->DefaultWriteOptions();
  write_options.filesystem = filesystem;
  write_options.base_dir = path.string();
  write_options.partitioning = *partitioning_result;

  auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);
  auto scanner_builder_result = dataset->NewScan();
  ASSERT_TRUE(scanner_builder_result.ok());
  auto scanner_result = (*scanner_builder_result)->Finish();
  ASSERT_TRUE(scanner_result.ok());

  auto status = arrow::dataset::FileSystemDataset::Write(write_options, *scanner_result);
  ASSERT_TRUE(status.ok());
}
}  // namespace

TEST(ParquetArrowSourceTest, ReadsParquetToArrowTable) {
  auto path = MakeTempPath("input.parquet");
  auto expected = BuildSampleTable();

  auto output_result = arrow::io::FileOutputStream::Open(path.string());
  ASSERT_TRUE(output_result.ok());
  auto output_stream = *output_result;

  auto status = parquet::arrow::WriteTable(*expected, arrow::default_memory_pool(),
                                           output_stream, expected->num_rows());
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
  EXPECT_TRUE(table->Equals(*expected));
}
