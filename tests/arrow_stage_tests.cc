#include <arrow/buffer.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <gtest/gtest.h>
#include <google/protobuf/struct.pb.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "flowpipe/stage.h"

#define flowpipe_create_stage flowpipe_create_stage_csv_arrow_source
#define flowpipe_destroy_stage flowpipe_destroy_stage_csv_arrow_source
#include "stages/csv_arrow_source/csv_arrow_source.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

#define flowpipe_create_stage flowpipe_create_stage_csv_arrow_sink
#define flowpipe_destroy_stage flowpipe_destroy_stage_csv_arrow_sink
#include "stages/csv_arrow_sink/csv_arrow_sink.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

#define flowpipe_create_stage flowpipe_create_stage_json_arrow_source
#define flowpipe_destroy_stage flowpipe_destroy_stage_json_arrow_source
#include "stages/json_arrow_source/json_arrow_source.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

#define flowpipe_create_stage flowpipe_create_stage_parquet_arrow_source
#define flowpipe_destroy_stage flowpipe_destroy_stage_parquet_arrow_source
#include "stages/parquet_arrow_source/parquet_arrow_source.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

#define flowpipe_create_stage flowpipe_create_stage_parquet_arrow_sink
#define flowpipe_destroy_stage flowpipe_destroy_stage_parquet_arrow_sink
#include "stages/parquet_arrow_sink/parquet_arrow_sink.cc"
#undef flowpipe_create_stage
#undef flowpipe_destroy_stage

namespace {
std::filesystem::path MakeTempPath(const std::string& filename) {
  auto base = std::filesystem::temp_directory_path();
  auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  auto dir = base / "flowpipe_arrow_stage_tests" / unique;
  std::filesystem::create_directories(dir);
  return dir / filename;
}

google::protobuf::Struct BuildPathConfig(const std::filesystem::path& path) {
  google::protobuf::Struct config;
  (*config.mutable_fields())["path"].set_string_value(path.string());
  return config;
}

std::shared_ptr<arrow::Table> BuildSampleTable() {
  arrow::Int64Builder id_builder;
  arrow::StringBuilder name_builder;

  EXPECT_TRUE(id_builder.Append(1).ok());
  EXPECT_TRUE(id_builder.Append(2).ok());
  EXPECT_TRUE(name_builder.Append("alpha").ok());
  EXPECT_TRUE(name_builder.Append("beta").ok());

  std::shared_ptr<arrow::Array> id_array;
  std::shared_ptr<arrow::Array> name_array;
  EXPECT_TRUE(id_builder.Finish(&id_array).ok());
  EXPECT_TRUE(name_builder.Finish(&name_array).ok());

  auto schema = arrow::schema({arrow::field("id", arrow::int64()),
                               arrow::field("name", arrow::utf8())});
  return arrow::Table::Make(schema, {id_array, name_array});
}

flowpipe::Payload SerializeTablePayload(const std::shared_ptr<arrow::Table>& table) {
  auto buffer_output = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto writer = arrow::ipc::MakeStreamWriter(buffer_output, table->schema()).ValueOrDie();

  arrow::TableBatchReader reader(*table);
  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    EXPECT_TRUE(reader.ReadNext(&batch).ok());
    if (!batch) {
      break;
    }
    EXPECT_TRUE(writer->WriteRecordBatch(*batch).ok());
  }

  EXPECT_TRUE(writer->Close().ok());
  auto buffer = buffer_output->Finish().ValueOrDie();

  auto payload_buffer = flowpipe::AllocatePayloadBuffer(buffer->size());
  EXPECT_TRUE(payload_buffer != nullptr);
  if (buffer->size() > 0) {
    std::memcpy(payload_buffer.get(), buffer->data(), buffer->size());
  }

  return flowpipe::Payload(std::move(payload_buffer), buffer->size());
}

std::shared_ptr<arrow::Table> ReadTableFromPayload(const flowpipe::Payload& payload) {
  auto buffer = arrow::Buffer::Wrap(payload.data(), static_cast<int64_t>(payload.size));
  auto reader = std::make_shared<arrow::io::BufferReader>(buffer);
  auto stream_reader = arrow::ipc::RecordBatchStreamReader::Open(reader).ValueOrDie();

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  while (true) {
    auto batch = stream_reader->Next().ValueOrDie();
    if (!batch) {
      break;
    }
    batches.push_back(batch);
  }

  return arrow::Table::FromRecordBatches(stream_reader->schema(), batches).ValueOrDie();
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
