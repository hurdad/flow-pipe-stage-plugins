#include <gtest/gtest.h>

#include <arrow/type.h>

#include "arrow/arrow_schema.pb.h"
#include "util/arrow.h"

namespace {

using flowpipe::v1::arrow::schema::ColumnType;

void ExpectColumnTypeEqual(const ColumnType& actual, const ColumnType& expected) {
  EXPECT_EQ(actual.type(), expected.type());
  EXPECT_EQ(actual.time_unit(), expected.time_unit());
  EXPECT_EQ(actual.has_fixed_size_binary_length(), expected.has_fixed_size_binary_length());
  if (expected.has_fixed_size_binary_length()) {
    EXPECT_EQ(actual.fixed_size_binary_length(), expected.fixed_size_binary_length());
  }
  EXPECT_EQ(actual.has_decimal_precision(), expected.has_decimal_precision());
  if (expected.has_decimal_precision()) {
    EXPECT_EQ(actual.decimal_precision(), expected.decimal_precision());
  }
  EXPECT_EQ(actual.has_decimal_scale(), expected.has_decimal_scale());
  if (expected.has_decimal_scale()) {
    EXPECT_EQ(actual.decimal_scale(), expected.decimal_scale());
  }
}

ColumnType MakeColumnType(ColumnType::DataType type) {
  ColumnType proto;
  proto.set_type(type);
  return proto;
}

ColumnType MakeTimeColumnType(ColumnType::DataType type, ColumnType::TimeUnit unit) {
  ColumnType proto;
  proto.set_type(type);
  proto.set_time_unit(unit);
  return proto;
}

ColumnType MakeDecimalColumnType(ColumnType::DataType type, int32_t precision, int32_t scale) {
  ColumnType proto;
  proto.set_type(type);
  proto.set_decimal_precision(precision);
  proto.set_decimal_scale(scale);
  return proto;
}

ColumnType MakeFixedSizeBinaryColumnType(int32_t length) {
  ColumnType proto;
  proto.set_type(ColumnType::DATA_TYPE_FIXED_SIZE_BINARY);
  proto.set_fixed_size_binary_length(length);
  return proto;
}

TEST(ArrowSchemaProtoTest, ProtoToArrowToProtoRoundTrip) {
  struct TestCase {
    ColumnType proto;
    std::shared_ptr<arrow::DataType> expected_type;
  };

  std::vector<TestCase> cases = {
      {MakeColumnType(ColumnType::DATA_TYPE_NULL), arrow::null()},
      {MakeColumnType(ColumnType::DATA_TYPE_BOOL), arrow::boolean()},
      {MakeColumnType(ColumnType::DATA_TYPE_INT8), arrow::int8()},
      {MakeColumnType(ColumnType::DATA_TYPE_INT16), arrow::int16()},
      {MakeColumnType(ColumnType::DATA_TYPE_INT32), arrow::int32()},
      {MakeColumnType(ColumnType::DATA_TYPE_INT64), arrow::int64()},
      {MakeColumnType(ColumnType::DATA_TYPE_UINT8), arrow::uint8()},
      {MakeColumnType(ColumnType::DATA_TYPE_UINT16), arrow::uint16()},
      {MakeColumnType(ColumnType::DATA_TYPE_UINT32), arrow::uint32()},
      {MakeColumnType(ColumnType::DATA_TYPE_UINT64), arrow::uint64()},
      {MakeColumnType(ColumnType::DATA_TYPE_FLOAT16), arrow::float16()},
      {MakeColumnType(ColumnType::DATA_TYPE_FLOAT32), arrow::float32()},
      {MakeColumnType(ColumnType::DATA_TYPE_FLOAT64), arrow::float64()},
      {MakeColumnType(ColumnType::DATA_TYPE_STRING), arrow::utf8()},
      {MakeColumnType(ColumnType::DATA_TYPE_LARGE_STRING), arrow::large_utf8()},
      {MakeColumnType(ColumnType::DATA_TYPE_BINARY), arrow::binary()},
      {MakeColumnType(ColumnType::DATA_TYPE_LARGE_BINARY), arrow::large_binary()},
      {MakeColumnType(ColumnType::DATA_TYPE_DATE32), arrow::date32()},
      {MakeColumnType(ColumnType::DATA_TYPE_DATE64), arrow::date64()},
      {MakeTimeColumnType(ColumnType::DATA_TYPE_TIMESTAMP, ColumnType::TIME_UNIT_MILLI),
       arrow::timestamp(arrow::TimeUnit::MILLI)},
      {MakeTimeColumnType(ColumnType::DATA_TYPE_TIME32, ColumnType::TIME_UNIT_SECOND),
       arrow::time32(arrow::TimeUnit::SECOND)},
      {MakeTimeColumnType(ColumnType::DATA_TYPE_TIME64, ColumnType::TIME_UNIT_NANO),
       arrow::time64(arrow::TimeUnit::NANO)},
      {MakeTimeColumnType(ColumnType::DATA_TYPE_DURATION, ColumnType::TIME_UNIT_MICRO),
       arrow::duration(arrow::TimeUnit::MICRO)},
      {MakeDecimalColumnType(ColumnType::DATA_TYPE_DECIMAL128, 12, 4),
       arrow::decimal128(12, 4)},
      {MakeDecimalColumnType(ColumnType::DATA_TYPE_DECIMAL256, 28, 6),
       arrow::decimal256(28, 6)},
      {MakeFixedSizeBinaryColumnType(16), arrow::fixed_size_binary(16)},
  };

  for (const auto& test_case : cases) {
    auto type_result = ConvertColumnType(test_case.proto);
    ASSERT_TRUE(type_result.ok()) << type_result.status().ToString();
    EXPECT_TRUE(type_result.ValueOrDie()->Equals(test_case.expected_type));

    auto proto_result = ConvertDataTypeToColumnType(test_case.expected_type);
    ASSERT_TRUE(proto_result.ok()) << proto_result.status().ToString();
    ExpectColumnTypeEqual(*proto_result, test_case.proto);
  }
}

TEST(ArrowSchemaProtoTest, ProtoToArrowRejectsInvalidConfigs) {
  ColumnType missing_time_unit;
  missing_time_unit.set_type(ColumnType::DATA_TYPE_TIMESTAMP);
  auto missing_time_result = ConvertColumnType(missing_time_unit);
  EXPECT_FALSE(missing_time_result.ok());

  ColumnType missing_decimal_scale;
  missing_decimal_scale.set_type(ColumnType::DATA_TYPE_DECIMAL128);
  missing_decimal_scale.set_decimal_precision(10);
  auto missing_scale_result = ConvertColumnType(missing_decimal_scale);
  EXPECT_FALSE(missing_scale_result.ok());

  ColumnType missing_fixed_size;
  missing_fixed_size.set_type(ColumnType::DATA_TYPE_FIXED_SIZE_BINARY);
  auto missing_fixed_result = ConvertColumnType(missing_fixed_size);
  EXPECT_FALSE(missing_fixed_result.ok());
}

TEST(ArrowSchemaProtoTest, ArrowToProtoRejectsUnsupportedTypes) {
  auto list_type = arrow::list(arrow::int32());
  auto list_result = ConvertDataTypeToColumnType(list_type);
  EXPECT_FALSE(list_result.ok());
}

}  // namespace
