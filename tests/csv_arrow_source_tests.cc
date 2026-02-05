#include <arrow/io/compressed.h>
#include <arrow/util/compression.h>
#include <fstream>
#include <string>
#include <vector>

#include "arrow_stage_test_support.h"
#include "arrow_common.pb.h"

#define flowpipe_create_stage flowpipe_create_stage_csv_arrow_source
#define flowpipe_destroy_stage flowpipe_destroy_stage_csv_arrow_source
#include "stages/csv_arrow_source/csv_arrow_source.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

using flowpipe_stage_tests::BuildPathConfig;
using flowpipe_stage_tests::BuildPathConfigWithCompression;
using flowpipe_stage_tests::BuildSampleTable;
using flowpipe_stage_tests::MakeTempPath;
using flowpipe_stage_tests::ReadTableFromPayload;

namespace {
struct CompressionCase {
  flowpipe::v1::arrow::common::Compression config_compression;
  arrow::Compression::type arrow_compression;
  const char* name;
};

std::vector<CompressionCase> CompressionCases() {
  return {
      {flowpipe::v1::arrow::common::COMPRESSION_GZIP, arrow::Compression::GZIP, "gzip"},
      {flowpipe::v1::arrow::common::COMPRESSION_ZSTD, arrow::Compression::ZSTD, "zstd"},
  };
}

void WriteCompressedFile(const std::filesystem::path& path, arrow::Compression::type compression,
                         const std::string& contents) {
  auto codec_result = arrow::util::Codec::Create(compression);
  ASSERT_TRUE(codec_result.ok());
  auto codec = std::move(codec_result).ValueOrDie();

  auto output_result = arrow::io::FileOutputStream::Open(path.string());
  ASSERT_TRUE(output_result.ok());
  auto output_stream = *output_result;

  auto compressed_result = arrow::io::CompressedOutputStream::Make(codec.get(), output_stream);
  ASSERT_TRUE(compressed_result.ok());
  auto compressed_stream = *compressed_result;

  ASSERT_TRUE(compressed_stream->Write(contents.data(),
                                       static_cast<int64_t>(contents.size()))
                  .ok());
  ASSERT_TRUE(compressed_stream->Close().ok());
  ASSERT_TRUE(output_stream->Close().ok());
}
}  // namespace

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

TEST(CsvArrowSourceTest, ReadsCompressedCsvToArrowTable) {
  auto expected = BuildSampleTable();
  const std::string contents = "id,name\n1,alpha\n2,beta\n";
  bool ran_case = false;

  for (const auto& test_case : CompressionCases()) {
    SCOPED_TRACE(test_case.name);
    if (!arrow::util::Codec::IsAvailable(test_case.arrow_compression)) {
      continue;
    }
    ran_case = true;

    auto path = MakeTempPath(std::string("input_") + test_case.name + ".csv");
    WriteCompressedFile(path, test_case.arrow_compression, contents);

    CsvArrowSource stage;
    auto config = BuildPathConfigWithCompression(path, test_case.config_compression);

    ASSERT_TRUE(stage.configure(config));

    flowpipe::StageContext ctx;
    flowpipe::Payload payload;
    ASSERT_TRUE(stage.produce(ctx, payload));

    auto table = ReadTableFromPayload(payload);
    EXPECT_TRUE(table->Equals(*expected));
  }

  if (!ran_case) {
    GTEST_SKIP() << "No compression codecs available";
  }
}
