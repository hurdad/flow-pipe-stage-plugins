#include <fstream>

#include "arrow_stage_test_support.h"

#define flowpipe_create_stage flowpipe_create_stage_json_arrow_source
#define flowpipe_destroy_stage flowpipe_destroy_stage_json_arrow_source
#include "stages/json_arrow_source/json_arrow_source.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

using flowpipe_stage_tests::BuildPathConfig;
using flowpipe_stage_tests::BuildSampleTable;
using flowpipe_stage_tests::MakeTempPath;
using flowpipe_stage_tests::ReadTableFromPayload;

TEST(JsonArrowSourceTest, ReadsJsonToArrowTable) {
  auto path = MakeTempPath("input.json");
  std::ofstream output(path);
  output << R"({"id":1,"name":"alpha"})"
         << "\n"
         << R"({"id":2,"name":"beta"})"
         << "\n";
  output.close();

  JsonArrowSource stage;
  auto config = BuildPathConfig(path);

  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  flowpipe::Payload payload;
  ASSERT_TRUE(stage.produce(ctx, payload));

  auto table = ReadTableFromPayload(payload);
  auto expected = BuildSampleTable();
  EXPECT_TRUE(table->Equals(*expected));
}
