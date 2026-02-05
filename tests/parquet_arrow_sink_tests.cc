#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/partition.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/reader.h>

#include <filesystem>

#include "arrow_stage_test_support.h"

#define flowpipe_create_stage flowpipe_create_stage_parquet_arrow_sink
#define flowpipe_destroy_stage flowpipe_destroy_stage_parquet_arrow_sink
#include "stages/parquet_arrow_sink/parquet_arrow_sink.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

using flowpipe_stage_tests::AddParquetPartitionColumns;
using flowpipe_stage_tests::BuildPathConfig;
using flowpipe_stage_tests::BuildSampleTable;
using flowpipe_stage_tests::EnsureArrowComputeInitialized;
using flowpipe_stage_tests::MakeTempPath;
using flowpipe_stage_tests::SerializeTablePayload;

TEST(ParquetArrowSinkTest, WritesArrowTableToParquet) {
  EnsureArrowComputeInitialized();
  auto path = MakeTempPath("output.parquet");
  auto expected = BuildSampleTable();

  ParquetArrowSink stage;
  auto config = BuildPathConfig(path);

  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  auto payload = SerializeTablePayload(expected);
  stage.consume(ctx, payload);

  auto input_result = arrow::io::ReadableFile::Open(path.string());
  ASSERT_TRUE(input_result.ok());

  auto reader_result = parquet::arrow::OpenFile(*input_result, arrow::default_memory_pool());
  ASSERT_TRUE(reader_result.ok());
  auto reader = std::move(reader_result).ValueOrDie();

  std::shared_ptr<arrow::Table> table;
  ASSERT_TRUE(reader->ReadTable(&table).ok());

  EXPECT_TRUE(table->Equals(*expected));
}

TEST(ParquetArrowSinkTest, WritesHivePartitionedParquetDataset) {
  EnsureArrowComputeInitialized();
  auto path = MakeTempPath("output_dataset");
  auto expected = BuildSampleTable();
  std::filesystem::create_directories(path);

  ParquetArrowSink stage;
  auto config = BuildPathConfig(path);
  AddParquetPartitionColumns(&config, {"id"});

  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  auto payload = SerializeTablePayload(expected);
  stage.consume(ctx, payload);

  auto filesystem = std::make_shared<arrow::fs::LocalFileSystem>();
  auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
  arrow::dataset::FileSystemFactoryOptions options;
  options.partitioning = arrow::dataset::HivePartitioning::MakeFactory();
  options.partition_base_dir = path.string();

  arrow::fs::FileSelector selector;
  selector.base_dir = path.string();
  selector.recursive = true;

  auto factory_result =
      arrow::dataset::FileSystemDatasetFactory::Make(filesystem, selector, format, options);
  ASSERT_TRUE(factory_result.ok());

  auto dataset_result = (*factory_result)->Finish();
  ASSERT_TRUE(dataset_result.ok());

  auto scanner_builder_result = (*dataset_result)->NewScan();
  ASSERT_TRUE(scanner_builder_result.ok());
  auto scanner_result = (*scanner_builder_result)->Finish();
  ASSERT_TRUE(scanner_result.ok());

  auto table_result = (*scanner_result)->ToTable();
  ASSERT_TRUE(table_result.ok());

  EXPECT_TRUE((*table_result)->Equals(*expected));
}
