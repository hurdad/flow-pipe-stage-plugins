#include <fstream>

#include "arrow_stage_test_support.h"

#define flowpipe_create_stage flowpipe_create_stage_csv_arrow_source
#define flowpipe_destroy_stage flowpipe_destroy_stage_csv_arrow_source
#include "stages/csv_arrow_source/csv_arrow_source.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

using flowpipe_stage_tests::BuildPathConfig;
using flowpipe_stage_tests::BuildSampleTable;
using flowpipe_stage_tests::MakeTempPath;
using flowpipe_stage_tests::ReadTableFromPayload;

TEST(CsvArrowSourceTest, ReadsCsvToArrowTable) {
  auto path = MakeTempPath("input.csv");
  std::ofstream output(path);
  output << "id,name\n1,alpha\n2,beta\n";
  output.close();

  CsvArrowSource stage;
  auto config = BuildPathConfig(path);

  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  flowpipe::Payload payload;
  ASSERT_TRUE(stage.produce(ctx, payload));

  auto table = ReadTableFromPayload(payload);
  auto expected = BuildSampleTable();
  EXPECT_TRUE(table->Equals(*expected));
}
