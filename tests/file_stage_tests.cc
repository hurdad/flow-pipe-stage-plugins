#include <google/protobuf/struct.pb.h>
#include <gtest/gtest.h>
#include <zlib.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "arrow_stage_test_support.h"
#include "file_sink.pb.h"
#include "file_source.pb.h"
#include "flowpipe/stage.h"

#define flowpipe_create_stage flowpipe_create_stage_file_source
#define flowpipe_destroy_stage flowpipe_destroy_stage_file_source
#include "stages/file_source/file_source.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

#define flowpipe_create_stage flowpipe_create_stage_file_sink
#define flowpipe_destroy_stage flowpipe_destroy_stage_file_sink
#include "stages/file_sink/file_sink.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

using flowpipe_stage_tests::MakeTempPath;

namespace {
google::protobuf::Struct BuildFileSourceConfig(
    const std::filesystem::path& path,
    flowpipe::v1::stages::file::source::v1::FileSourceConfig::CompressionType compression,
    uint64_t max_bytes = 0) {
  google::protobuf::Struct config;
  (*config.mutable_fields())["path"].set_string_value(path.string());
  (*config.mutable_fields())["compression"].set_string_value(
      flowpipe::v1::stages::file::source::v1::FileSourceConfig::CompressionType_Name(compression));
  (*config.mutable_fields())["max_bytes"].set_number_value(static_cast<double>(max_bytes));
  return config;
}

google::protobuf::Struct BuildFileSinkConfig(
    const std::filesystem::path& path, bool append,
    flowpipe::v1::stages::file::sink::v1::FileSinkConfig::CompressionType compression,
    int compression_level = 0) {
  google::protobuf::Struct config;
  (*config.mutable_fields())["path"].set_string_value(path.string());
  (*config.mutable_fields())["append"].set_bool_value(append);
  (*config.mutable_fields())["compression"].set_string_value(
      flowpipe::v1::stages::file::sink::v1::FileSinkConfig::CompressionType_Name(compression));
  (*config.mutable_fields())["compression_level"].set_number_value(compression_level);
  return config;
}

flowpipe::Payload BuildPayload(const std::string& contents) {
  auto buffer = flowpipe::AllocatePayloadBuffer(contents.size());
  EXPECT_TRUE(buffer != nullptr);
  if (!contents.empty()) {
    std::memcpy(buffer.get(), contents.data(), contents.size());
  }
  return flowpipe::Payload(std::move(buffer), contents.size());
}

void WriteGzipFile(const std::filesystem::path& path, const std::string& contents) {
  gzFile file = gzopen(path.string().c_str(), "wb");
  ASSERT_TRUE(file != nullptr);
  int written = gzwrite(file, contents.data(), static_cast<unsigned int>(contents.size()));
  ASSERT_EQ(written, static_cast<int>(contents.size()));
  ASSERT_EQ(gzclose(file), Z_OK);
}

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  EXPECT_TRUE(input.is_open());
  std::string contents((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
  return contents;
}

std::string ReadGzipFile(const std::filesystem::path& path) {
  gzFile file = gzopen(path.string().c_str(), "rb");
  EXPECT_TRUE(file != nullptr);
  std::string contents;
  std::vector<char> buffer(4096);
  while (true) {
    int read = gzread(file, buffer.data(), static_cast<unsigned int>(buffer.size()));
    if (read > 0) {
      contents.append(buffer.data(), static_cast<size_t>(read));
      continue;
    }
    EXPECT_EQ(read, 0);
    break;
  }
  EXPECT_EQ(gzclose(file), Z_OK);
  return contents;
}
}  // namespace

TEST(FileSourceTest, ReadsRawFile) {
  auto path = MakeTempPath("input.txt");
  const std::string contents = "hello world";
  std::ofstream output(path, std::ios::binary);
  output << contents;
  output.close();

  FileSource stage;
  auto config = BuildFileSourceConfig(
      path, flowpipe::v1::stages::file::source::v1::FileSourceConfig::COMPRESSION_NONE);
  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  flowpipe::Payload payload;
  ASSERT_TRUE(stage.produce(ctx, payload));
  ASSERT_EQ(payload.size, contents.size());
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(payload.data()), payload.size), contents);
}

TEST(FileSourceTest, ReadsGzipFile) {
  auto path = MakeTempPath("input.txt.gz");
  const std::string contents = "compressed-data";
  WriteGzipFile(path, contents);

  FileSource stage;
  auto config = BuildFileSourceConfig(
      path, flowpipe::v1::stages::file::source::v1::FileSourceConfig::COMPRESSION_GZIP);
  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  flowpipe::Payload payload;
  ASSERT_TRUE(stage.produce(ctx, payload));
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(payload.data()), payload.size), contents);
}

TEST(FileSourceTest, AutoDetectsGzip) {
  auto path = MakeTempPath("auto.gz");
  const std::string contents = "auto-detect";
  WriteGzipFile(path, contents);

  FileSource stage;
  auto config = BuildFileSourceConfig(
      path, flowpipe::v1::stages::file::source::v1::FileSourceConfig::COMPRESSION_AUTO);
  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  flowpipe::Payload payload;
  ASSERT_TRUE(stage.produce(ctx, payload));
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(payload.data()), payload.size), contents);
}

TEST(FileSourceTest, EnforcesMaxBytes) {
  auto path = MakeTempPath("limited.txt");
  const std::string contents = "12345";
  std::ofstream output(path, std::ios::binary);
  output << contents;
  output.close();

  FileSource stage;
  auto config = BuildFileSourceConfig(
      path, flowpipe::v1::stages::file::source::v1::FileSourceConfig::COMPRESSION_NONE, 4);
  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  flowpipe::Payload payload;
  EXPECT_FALSE(stage.produce(ctx, payload));
}

TEST(FileSinkTest, WritesRawFile) {
  auto path = MakeTempPath("output.txt");
  const std::string contents = "payload";

  FileSink stage;
  auto config = BuildFileSinkConfig(
      path, false, flowpipe::v1::stages::file::sink::v1::FileSinkConfig::COMPRESSION_NONE);
  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  auto payload = BuildPayload(contents);
  stage.consume(ctx, payload);

  EXPECT_EQ(ReadFile(path), contents);
}

TEST(FileSinkTest, WritesGzipFile) {
  auto path = MakeTempPath("output.txt.gz");
  const std::string contents = "gzip-payload";

  FileSink stage;
  auto config = BuildFileSinkConfig(
      path, false, flowpipe::v1::stages::file::sink::v1::FileSinkConfig::COMPRESSION_GZIP, 6);
  ASSERT_TRUE(stage.configure(config));

  flowpipe::StageContext ctx;
  auto payload = BuildPayload(contents);
  stage.consume(ctx, payload);

  EXPECT_EQ(ReadGzipFile(path), contents);
}
