#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "csv_source.pb.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"

using namespace flowpipe;

using CsvSourceConfig = flowpipe::stages::csv::source::v1::CsvSourceConfig;

namespace {
std::string Trim(std::string value) {
  auto begin = std::find_if_not(value.begin(), value.end(),
                                [](unsigned char ch) { return std::isspace(ch) != 0; });
  auto end = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char ch) {
               return std::isspace(ch) != 0;
             }).base();
  if (begin >= end) {
    return "";
  }
  return std::string(begin, end);
}

bool ParseCsvLine(const std::string& line, char delimiter, std::vector<std::string>& fields,
                  std::string& error) {
  fields.clear();
  std::string current;
  bool in_quotes = false;

  for (size_t i = 0; i < line.size(); ++i) {
    char ch = line[i];
    if (in_quotes) {
      if (ch == '"') {
        if (i + 1 < line.size() && line[i + 1] == '"') {
          current.push_back('"');
          ++i;
        } else {
          in_quotes = false;
        }
      } else {
        current.push_back(ch);
      }
      continue;
    }

    if (ch == '"') {
      in_quotes = true;
      continue;
    }

    if (ch == delimiter) {
      fields.push_back(current);
      current.clear();
      continue;
    }

    current.push_back(ch);
  }

  if (in_quotes) {
    error = "unterminated quote";
    return false;
  }

  fields.push_back(current);
  return true;
}

char ResolveDelimiter(const std::string& delimiter) {
  if (delimiter.empty()) {
    return ',';
  }
  return delimiter[0];
}

bool ReadNextDataLine(std::ifstream& input, bool skip_empty_lines, std::string& out_line) {
  while (std::getline(input, out_line)) {
    if (!out_line.empty() && out_line.back() == '\r') {
      out_line.pop_back();
    }

    if (skip_empty_lines && out_line.empty()) {
      continue;
    }

    return true;
  }

  return false;
}
}  // namespace

// ============================================================
// CsvSource
// ============================================================
class CsvSource final : public ISourceStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "csv_source";
  }

  CsvSource() {
    FP_LOG_INFO("csv_source constructed");
  }

  ~CsvSource() override {
    if (input_.is_open()) {
      input_.close();
    }
    FP_LOG_INFO("csv_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("csv_source failed to serialize config");
      return false;
    }

    CsvSourceConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("csv_source invalid config");
      return false;
    }

    if (cfg.path().empty()) {
      FP_LOG_ERROR("csv_source requires path");
      return false;
    }

    config_ = std::move(cfg);
    delimiter_ = ResolveDelimiter(config_.delimiter());
    headers_.clear();
    header_ready_ = false;

    if (input_.is_open()) {
      input_.close();
    }

    input_.open(config_.path());
    if (!input_.is_open()) {
      FP_LOG_ERROR("csv_source failed to open file: " + config_.path());
      return false;
    }

    if (config_.has_header()) {
      std::string line;
      if (ReadNextDataLine(input_, config_.skip_empty_lines(), line)) {
        std::vector<std::string> fields;
        std::string error;
        if (!ParseCsvLine(line, delimiter_, fields, error)) {
          FP_LOG_ERROR("csv_source failed to parse header: " + error);
          return false;
        }
        headers_ = fields;
        header_ready_ = true;
      }
    }

    FP_LOG_INFO("csv_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("csv_source stop requested, skipping produce");
      return false;
    }

    if (!input_.is_open()) {
      FP_LOG_ERROR("csv_source file not open");
      return false;
    }

    std::string line;
    while (ReadNextDataLine(input_, config_.skip_empty_lines(), line)) {
      std::vector<std::string> fields;
      std::string error;
      if (!ParseCsvLine(line, delimiter_, fields, error)) {
        FP_LOG_ERROR("csv_source failed to parse line: " + error);
        continue;
      }

      if (config_.trim_whitespace()) {
        for (auto& field : fields) {
          field = Trim(field);
        }
      }

      if (!header_ready_) {
        headers_.clear();
        headers_.reserve(fields.size());
        for (size_t i = 0; i < fields.size(); ++i) {
          headers_.push_back("column" + std::to_string(i + 1));
        }
        header_ready_ = true;
      }

      if (fields.size() > headers_.size()) {
        size_t start_index = headers_.size();
        for (size_t i = start_index; i < fields.size(); ++i) {
          headers_.push_back("column" + std::to_string(i + 1));
        }
      }

      google::protobuf::Struct row;
      auto* row_fields = row.mutable_fields();
      for (size_t i = 0; i < headers_.size(); ++i) {
        const std::string& header = headers_[i];
        std::string value;
        if (i < fields.size()) {
          value = fields[i];
        }
        (*row_fields)[header].set_string_value(value);
      }

      std::string json;
      auto status = google::protobuf::util::MessageToJsonString(row, &json);
      if (!status.ok()) {
        FP_LOG_ERROR("csv_source failed to serialize row to JSON");
        continue;
      }

      auto buffer = AllocatePayloadBuffer(json.size());
      if (!buffer) {
        FP_LOG_ERROR("csv_source failed to allocate payload");
        return false;
      }

      if (!json.empty()) {
        std::memcpy(buffer.get(), json.data(), json.size());
      }

      payload = Payload(std::move(buffer), json.size());
      FP_LOG_DEBUG("csv_source produced payload from row");
      return true;
    }

    FP_LOG_DEBUG("csv_source reached end of file");
    return false;
  }

 private:
  CsvSourceConfig config_{};
  std::ifstream input_{};
  std::vector<std::string> headers_{};
  bool header_ready_{false};
  char delimiter_{','};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating csv_source stage");
  return new CsvSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying csv_source stage");
  delete stage;
}

}  // extern "C"
