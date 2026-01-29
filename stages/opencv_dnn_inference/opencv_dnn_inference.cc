#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <opencv2/dnn.hpp>
#include <opencv2/imgcodecs.hpp>
#include <opencv2/imgproc.hpp>

#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "opencv_dnn_inference.pb.h"

using namespace flowpipe;

using OpenCVDnnInferenceConfig =
    flowpipe::stages::opencv::dnn::inference::v1::OpenCVDnnInferenceConfig;

namespace {
constexpr int kMaxMeanValues = 3;

cv::Scalar BuildMean(const std::vector<float>& values) {
  if (values.empty()) {
    return cv::Scalar(0.0, 0.0, 0.0);
  }

  if (values.size() == 1) {
    return cv::Scalar(values[0], values[0], values[0]);
  }

  std::array<double, kMaxMeanValues> mean{0.0, 0.0, 0.0};
  for (size_t i = 0; i < std::min(values.size(), mean.size()); ++i) {
    mean[i] = values[i];
  }

  return cv::Scalar(mean[0], mean[1], mean[2]);
}

std::vector<int> MatShape(const cv::Mat& mat) {
  std::vector<int> shape;
  shape.reserve(static_cast<size_t>(mat.dims));
  for (int i = 0; i < mat.dims; ++i) {
    shape.push_back(mat.size[i]);
  }
  if (shape.empty()) {
    shape.push_back(mat.rows);
    shape.push_back(mat.cols);
  }
  return shape;
}

void AppendJsonArray(std::ostringstream& stream, const std::vector<int>& values) {
  stream << '[';
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      stream << ',';
    }
    stream << values[i];
  }
  stream << ']';
}

void AppendJsonArray(std::ostringstream& stream, const float* values, size_t count) {
  stream << '[';
  for (size_t i = 0; i < count; ++i) {
    if (i > 0) {
      stream << ',';
    }
    stream << values[i];
  }
  stream << ']';
}

void AppendOutputJson(std::ostringstream& stream, const std::string& name,
                      const cv::Mat& output) {
  cv::Mat float_output;
  if (output.depth() == CV_32F) {
    float_output = output;
  } else {
    output.convertTo(float_output, CV_32F);
  }

  const auto shape = MatShape(float_output);
  const size_t total = float_output.total();
  const float* data = float_output.ptr<float>();

  stream << "\"" << name << "\":{";
  stream << "\"shape\":";
  AppendJsonArray(stream, shape);
  stream << ",\"data\":";
  AppendJsonArray(stream, data, total);
  stream << '}';
}
}  // namespace

// ============================================================
// OpenCVDnnInference
// ============================================================
class OpenCVDnnInference final : public ITransformStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "opencv_dnn_inference";
  }

  OpenCVDnnInference() {
    FP_LOG_INFO("opencv_dnn_inference constructed");
  }

  ~OpenCVDnnInference() override {
    FP_LOG_INFO("opencv_dnn_inference destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("opencv_dnn_inference failed to serialize config");
      return false;
    }

    OpenCVDnnInferenceConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("opencv_dnn_inference invalid config");
      return false;
    }

    if (cfg.model_path().empty()) {
      FP_LOG_ERROR("opencv_dnn_inference requires model_path");
      return false;
    }

    if (!std::filesystem::exists(cfg.model_path())) {
      FP_LOG_ERROR("opencv_dnn_inference model_path does not exist: " +
                   cfg.model_path());
      return false;
    }

    if (!cfg.config_path().empty() &&
        !std::filesystem::exists(cfg.config_path())) {
      FP_LOG_ERROR("opencv_dnn_inference config_path does not exist: " +
                   cfg.config_path());
      return false;
    }

    try {
      net_ = cv::dnn::readNet(cfg.model_path(), cfg.config_path(), cfg.framework());
    } catch (const cv::Exception& ex) {
      FP_LOG_ERROR("opencv_dnn_inference failed to load network: " +
                   std::string(ex.what()));
      return false;
    }

    config_ = std::move(cfg);
    FP_LOG_INFO("opencv_dnn_inference configured");
    return true;
  }

  // ------------------------------------------------------------
  // ITransformStage
  // ------------------------------------------------------------
  bool transform(StageContext& ctx, const Payload& input, Payload& output) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("opencv_dnn_inference stop requested, skipping transform");
      return false;
    }

    if (input.empty()) {
      FP_LOG_DEBUG("opencv_dnn_inference received empty payload");
      return false;
    }

    cv::Mat encoded(1, static_cast<int>(input.size), CV_8U,
                    const_cast<uint8_t*>(input.data()));
    cv::Mat image = cv::imdecode(encoded, cv::IMREAD_COLOR);
    if (image.empty()) {
      FP_LOG_ERROR("opencv_dnn_inference failed to decode image bytes");
      return false;
    }

    const int width = config_.input_width() > 0 ? config_.input_width() : image.cols;
    const int height =
        config_.input_height() > 0 ? config_.input_height() : image.rows;

    const cv::Scalar mean = BuildMean({config_.mean_values().begin(),
                                       config_.mean_values().end()});
    const float scale = config_.scale() == 0.0f ? 1.0f : config_.scale();
    cv::Mat blob = cv::dnn::blobFromImage(image, scale,
                                          cv::Size(width, height), mean,
                                          config_.swap_rb(), config_.crop());

    try {
      if (config_.input_name().empty()) {
        net_.setInput(blob);
      } else {
        net_.setInput(blob, config_.input_name());
      }
    } catch (const cv::Exception& ex) {
      FP_LOG_ERROR("opencv_dnn_inference failed to set input: " +
                   std::string(ex.what()));
      return false;
    }

    std::ostringstream json_stream;
    json_stream << '{';

    try {
      if (config_.output_names().empty()) {
        cv::Mat output_mat = net_.forward();
        AppendOutputJson(json_stream, "output", output_mat);
      } else {
        std::vector<cv::Mat> outputs;
        std::vector<std::string> names(config_.output_names().begin(),
                                       config_.output_names().end());
        net_.forward(outputs, names);
        for (size_t i = 0; i < outputs.size(); ++i) {
          if (i > 0) {
            json_stream << ',';
          }
          AppendOutputJson(json_stream, names[i], outputs[i]);
        }
      }
    } catch (const cv::Exception& ex) {
      FP_LOG_ERROR("opencv_dnn_inference forward failed: " + std::string(ex.what()));
      return false;
    }

    json_stream << '}';
    const std::string json = json_stream.str();

    auto buffer = AllocatePayloadBuffer(json.size());
    if (!buffer) {
      FP_LOG_ERROR("opencv_dnn_inference failed to allocate payload");
      return false;
    }

    if (!json.empty()) {
      std::memcpy(buffer.get(), json.data(), json.size());
    }

    output = Payload(std::move(buffer), json.size());
    FP_LOG_DEBUG("opencv_dnn_inference emitted inference payload");
    return true;
  }

 private:
  OpenCVDnnInferenceConfig config_{};
  cv::dnn::Net net_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating opencv_dnn_inference stage");
  return new OpenCVDnnInference();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying opencv_dnn_inference stage");
  delete stage;
}
}
