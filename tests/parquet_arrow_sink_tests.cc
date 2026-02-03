#include <parquet/arrow/reader.h>

#include "arrow_stage_test_support.h"

#define flowpipe_create_stage flowpipe_create_stage_parquet_arrow_sink
#define flowpipe_destroy_stage flowpipe_destroy_stage_parquet_arrow_sink
#include "stages/parquet_arrow_sink/parquet_arrow_sink.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

using flowpipe_stage_tests::BuildPathConfig;
using flowpipe_stage_tests::BuildSampleTable;
using flowpipe_stage_tests::MakeTempPath;
using flowpipe_stage_tests::SerializeTablePayload;

TEST(ParquetArrowSinkTest, WritesArrowTableToParquet) {
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

  std::unique_ptr<parquet::arrow::FileReader> reader;
  auto status =
      parquet::arrow::OpenFile(*input_result, arrow::default_memory_pool(), &reader);
  ASSERT_TRUE(status.ok());

  std::shared_ptr<arrow::Table> table;
  ASSERT_TRUE(reader->ReadTable(&table).ok());

  EXPECT_TRUE(table->Equals(*expected));
}
