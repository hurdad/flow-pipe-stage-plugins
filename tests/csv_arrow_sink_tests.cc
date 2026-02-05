#include <arrow/csv/api.h>
#include <arrow/io/compressed.h>
#include <arrow/util/compression.h>
#include <string>
#include <vector>

#include "arrow_stage_test_support.h"
#include "arrow_common.pb.h"

#define flowpipe_create_stage flowpipe_create_stage_csv_arrow_sink
#define flowpipe_destroy_stage flowpipe_destroy_stage_csv_arrow_sink
#include "stages/csv_arrow_sink/csv_arrow_sink.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

using flowpipe_stage_tests::BuildPathConfig;
using flowpipe_stage_tests::BuildPathConfigWithCompression;
using flowpipe_stage_tests::BuildSampleTable;
using flowpipe_stage_tests::MakeTempPath;
using flowpipe_stage_tests::SerializeTablePayload;

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
}  // namespace

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

TEST(CsvArrowSinkTest, WritesCompressedCsv) {
  auto expected = BuildSampleTable();
  bool ran_case = false;

  for (const auto& test_case : CompressionCases()) {
    SCOPED_TRACE(test_case.name);
    if (!arrow::util::Codec::IsAvailable(test_case.arrow_compression)) {
      continue;
    }
    ran_case = true;

    auto path = MakeTempPath(std::string("output_") + test_case.name + ".csv");

    CsvArrowSink stage;
    auto config = BuildPathConfigWithCompression(path, test_case.config_compression);

    ASSERT_TRUE(stage.configure(config));

    flowpipe::StageContext ctx;
    auto payload = SerializeTablePayload(expected);
    stage.consume(ctx, payload);

    auto input_result = arrow::io::ReadableFile::Open(path.string());
    ASSERT_TRUE(input_result.ok());

    auto codec_result = arrow::util::Codec::Create(test_case.arrow_compression);
    ASSERT_TRUE(codec_result.ok());
    auto codec = std::move(codec_result).ValueOrDie();

    auto compressed_result = arrow::io::CompressedInputStream::Make(codec.get(), *input_result);
    ASSERT_TRUE(compressed_result.ok());
    auto compressed_stream = *compressed_result;

    arrow::io::IOContext io_context = arrow::io::default_io_context();
    auto reader_result = arrow::csv::TableReader::Make(
        io_context, compressed_stream, arrow::csv::ReadOptions::Defaults(),
        arrow::csv::ParseOptions::Defaults(), arrow::csv::ConvertOptions::Defaults());
    ASSERT_TRUE(reader_result.ok());

    auto table_result = (*reader_result)->Read();
    ASSERT_TRUE(table_result.ok());

    EXPECT_TRUE((*table_result)->Equals(*expected));
  }

  if (!ran_case) {
    GTEST_SKIP() << "No compression codecs available";
  }
}
