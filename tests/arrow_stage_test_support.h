#pragma once

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <gtest/gtest.h>
#include <google/protobuf/struct.pb.h>

#include "arrow_common.pb.h"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "flowpipe/stage.h"

namespace flowpipe_stage_tests {

inline std::filesystem::path MakeTempPath(const std::string& filename) {
  auto base = std::filesystem::temp_directory_path();
  auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  auto dir = base / "flowpipe_arrow_stage_tests" / unique;
  std::filesystem::create_directories(dir);
  return dir / filename;
}

inline google::protobuf::Struct BuildPathConfig(const std::filesystem::path& path) {
  google::protobuf::Struct config;
  (*config.mutable_fields())["path"].set_string_value(path.string());
  return config;
}

inline void AddCommonCompression(google::protobuf::Struct* config,
                                 flowpipe::v1::arrow::common::Compression compression) {
  auto* common_value = (*config->mutable_fields())["common"].mutable_struct_value();
  (*common_value->mutable_fields())["compression"].set_string_value(
      flowpipe::v1::arrow::common::Compression_Name(compression));
}

inline google::protobuf::Struct BuildPathConfigWithCompression(
    const std::filesystem::path& path,
    flowpipe::v1::arrow::common::Compression compression) {
  auto config = BuildPathConfig(path);
  AddCommonCompression(&config, compression);
  return config;
}

inline void AddParquetPartitionColumns(google::protobuf::Struct* config,
                                       const std::vector<std::string>& columns) {
  auto* write_opts = (*config->mutable_fields())["write_opts"].mutable_struct_value();
  auto* list_value = (*write_opts->mutable_fields())["partition_columns"].mutable_list_value();
  for (const auto& column : columns) {
    list_value->add_values()->set_string_value(column);
  }
}

inline std::shared_ptr<arrow::Table> BuildSampleTable() {
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

inline flowpipe::Payload SerializeTablePayload(const std::shared_ptr<arrow::Table>& table) {
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

inline std::shared_ptr<arrow::Table> ReadTableFromPayload(const flowpipe::Payload& payload) {
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

}  // namespace flowpipe_stage_tests
