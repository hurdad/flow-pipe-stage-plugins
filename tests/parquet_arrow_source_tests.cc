#include <parquet/arrow/writer.h>

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
