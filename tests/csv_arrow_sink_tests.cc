#include <arrow/csv/api.h>

#include "arrow_stage_test_support.h"

#define flowpipe_create_stage flowpipe_create_stage_csv_arrow_sink
#define flowpipe_destroy_stage flowpipe_destroy_stage_csv_arrow_sink
#include "stages/csv_arrow_sink/csv_arrow_sink.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

using flowpipe_stage_tests::BuildPathConfig;
using flowpipe_stage_tests::BuildSampleTable;
using flowpipe_stage_tests::MakeTempPath;
using flowpipe_stage_tests::SerializeTablePayload;

TEST(CsvArrowSinkTest, WritesArrowTableToCsv) {
  auto path = MakeTempPath("output.csv");
  auto expected = BuildSampleTable();

  CsvArrowSink stage;
  auto config = BuildPathConfig(path);

  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  auto payload = SerializeTablePayload(expected);
  stage.consume(ctx, payload);

  auto input_result = arrow::io::ReadableFile::Open(path.string());
  ASSERT_TRUE(input_result.ok());

  arrow::io::IOContext io_context = arrow::io::default_io_context();
  auto reader_result = arrow::csv::TableReader::Make(
      io_context, *input_result, arrow::csv::ReadOptions::Defaults(),
      arrow::csv::ParseOptions::Defaults(), arrow::csv::ConvertOptions::Defaults());
  ASSERT_TRUE(reader_result.ok());

  auto table_result = (*reader_result)->Read();
  ASSERT_TRUE(table_result.ok());

  EXPECT_TRUE((*table_result)->Equals(*expected));
}
