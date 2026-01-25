#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

#include "csv_sink.pb.h"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

using namespace flowpipe;

using CsvSinkConfig = flowpipe::stages::csv::sink::v1::CsvSinkConfig;

namespace {
char ResolveDelimiter(const std::string& delimiter) {
  if (delimiter.empty()) {
    return ',';
  }
  return delimiter[0];
}

bool FileHasContent(const std::string& path) {
  std::ifstream input(path, std::ios::binary | std::ios::ate);
  if (!input.is_open()) {
    return false;
  }
  return input.tellg() > 0;
}

std::string EscapeCsvField(const std::string& value,
                           char delimiter,
                           bool quote_all) {
  bool needs_quotes = quote_all;
  for (char ch : value) {
    if (ch == '"' || ch == '\n' || ch == '\r' || ch == delimiter) {
      needs_quotes = true;
      break;
    }
  }

  if (!needs_quotes) {
    return value;
  }

  std::string escaped;
  escaped.reserve(value.size() + 2);
  escaped.push_back('"');
  for (char ch : value) {
    if (ch == '"') {
      escaped.push_back('"');
    }
    escaped.push_back(ch);
  }
  escaped.push_back('"');
  return escaped;
}

std::string ValueToString(const google::protobuf::Value& value) {
  switch (value.kind_case()) {
    case google::protobuf::Value::kNullValue:
      return "";
    case google::protobuf::Value::kNumberValue: {
      std::ostringstream stream;
      stream << value.number_value();
      return stream.str();
    }
    case google::protobuf::Value::kStringValue:
      return value.string_value();
    case google::protobuf::Value::kBoolValue:
      return value.bool_value() ? "true" : "false";
    case google::protobuf::Value::kStructValue:
    case google::protobuf::Value::kListValue: {
      std::string json;
      auto status = google::protobuf::util::MessageToJsonString(value, &json);
      if (!status.ok()) {
        return "";
      }
      return json;
    }
    case google::protobuf::Value::KIND_NOT_SET:
    default:
      return "";
  }
}
}  // namespace

// ============================================================
// CsvSink
// ============================================================
class CsvSink final
    : public ISinkStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "csv_sink";
  }

  CsvSink() {
    FP_LOG_INFO("csv_sink constructed");
  }

  ~CsvSink() override {
    FP_LOG_INFO("csv_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("csv_sink failed to serialize config");
      return false;
    }

    CsvSinkConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("csv_sink invalid config");
      return false;
    }

    if (cfg.path().empty()) {
      FP_LOG_ERROR("csv_sink requires path");
      return false;
    }

    config_ = std::move(cfg);
    delimiter_ = ResolveDelimiter(config_.delimiter());
    headers_.assign(config_.headers().begin(), config_.headers().end());
    header_written_ = false;

    if (config_.append() && config_.include_header()) {
      header_written_ = FileHasContent(config_.path());
    }

    FP_LOG_INFO("csv_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("csv_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("csv_sink received empty payload");
      return;
    }

    std::string payload_str(reinterpret_cast<const char*>(payload.data()),
                            payload.size);

    google::protobuf::Struct row;
    auto status = google::protobuf::util::JsonStringToMessage(payload_str, &row);
    if (!status.ok()) {
      FP_LOG_ERROR("csv_sink failed to parse payload JSON");
      return;
    }

    if (headers_.empty()) {
      headers_.reserve(row.fields_size());
      for (const auto& entry : row.fields()) {
        headers_.push_back(entry.first);
      }
      std::sort(headers_.begin(), headers_.end());
    }

    std::ios_base::openmode mode = std::ios::binary;
    mode |= config_.append() ? std::ios::app : std::ios::trunc;
    std::ofstream output(config_.path(), mode);
    if (!output.is_open()) {
      FP_LOG_ERROR("csv_sink failed to open file: " + config_.path());
      return;
    }

    if (config_.include_header() && !header_written_) {
      WriteRow(output, headers_);
      header_written_ = true;
    }

    std::vector<std::string> values;
    values.reserve(headers_.size());
    for (const auto& header : headers_) {
      auto it = row.fields().find(header);
      if (it == row.fields().end()) {
        values.emplace_back("");
        continue;
      }
      values.push_back(ValueToString(it->second));
    }

    WriteRow(output, values);

    if (!output.good()) {
      FP_LOG_ERROR("csv_sink failed to write file: " + config_.path());
      return;
    }

    FP_LOG_DEBUG("csv_sink wrote payload to file");
  }

private:
  void WriteRow(std::ofstream& output, const std::vector<std::string>& values) {
    for (size_t i = 0; i < values.size(); ++i) {
      if (i > 0) {
        output << delimiter_;
      }
      output << EscapeCsvField(values[i], delimiter_, config_.quote_all_fields());
    }
    output << '\n';
  }

  CsvSinkConfig config_{};
  std::vector<std::string> headers_{};
  bool header_written_{false};
  char delimiter_{','};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating csv_sink stage");
  return new CsvSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying csv_sink stage");
  delete stage;
}

}  // extern "C"
