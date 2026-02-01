#include <google/protobuf/struct.pb.h>
#include "flowpipe/protobuf_config.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <opencv2/imgcodecs.hpp>
#include <opencv2/imgproc.hpp>
#include <string>
#include <utility>
#include <vector>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "opencv_resize_letterbox_transform.pb.h"

using namespace flowpipe;

using OpenCVResizeLetterboxConfig =
    flowpipe::stages::opencv::resize::letterbox::v1::OpenCVResizeLetterboxConfig;

namespace {
constexpr int kMaxColorChannels = 3;

cv::Scalar BuildPadColor(const std::vector<float>& values) {
  if (values.empty()) {
    return cv::Scalar(0.0, 0.0, 0.0);
  }

  if (values.size() == 1) {
    return cv::Scalar(values[0], values[0], values[0]);
  }

  std::array<double, kMaxColorChannels> color{0.0, 0.0, 0.0};
  for (size_t i = 0; i < std::min(values.size(), color.size()); ++i) {
    color[i] = values[i];
  }

  return cv::Scalar(color[0], color[1], color[2]);
}

int ParseInterpolation(OpenCVResizeLetterboxConfig::Interpolation value) {
  switch (value) {
    case OpenCVResizeLetterboxConfig::INTERPOLATION_NEAREST:
      return cv::INTER_NEAREST;
    case OpenCVResizeLetterboxConfig::INTERPOLATION_AREA:
      return cv::INTER_AREA;
    case OpenCVResizeLetterboxConfig::INTERPOLATION_CUBIC:
      return cv::INTER_CUBIC;
    case OpenCVResizeLetterboxConfig::INTERPOLATION_LANCZOS4:
      return cv::INTER_LANCZOS4;
    case OpenCVResizeLetterboxConfig::INTERPOLATION_LINEAR:
    case OpenCVResizeLetterboxConfig::INTERPOLATION_UNSPECIFIED:
    default:
      return cv::INTER_LINEAR;
  }
}

bool UseLetterbox(const std::string& mode) {
  return mode.empty() || mode == "letterbox";
}

std::string NormalizeOutputFormat(const std::string& format) {
  if (format.empty()) {
    return ".jpg";
  }
  if (format.front() == '.') {
    return format;
  }
  return "." + format;
}
}  // namespace

class OpenCVResizeLetterbox final : public ITransformStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "opencv_resize_letterbox_transform";
  }

  OpenCVResizeLetterbox() {
    FP_LOG_INFO("opencv_resize_letterbox_transform constructed");
  }

  ~OpenCVResizeLetterbox() override {
    FP_LOG_INFO("opencv_resize_letterbox_transform destroyed");
  }

  bool configure(const google::protobuf::Struct& config) override {
    OpenCVResizeLetterboxConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<OpenCVResizeLetterboxConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("opencv_resize_letterbox_transform invalid config: " + error);
      return false;
    }

    if (cfg.output_width() <= 0 || cfg.output_height() <= 0) {
      FP_LOG_ERROR("opencv_resize_letterbox_transform requires output_width and output_height");
      return false;
    }

    pad_color_ = BuildPadColor({cfg.pad_color().begin(), cfg.pad_color().end()});
    interpolation_ = ParseInterpolation(cfg.interpolation());
    resize_mode_ = cfg.resize_mode();
    output_format_ = NormalizeOutputFormat(cfg.output_format());
    output_params_.assign(cfg.output_params().begin(), cfg.output_params().end());
    config_ = std::move(cfg);

    FP_LOG_INFO("opencv_resize_letterbox_transform configured");
    return true;
  }

  void process(StageContext& ctx, const Payload& input, Payload& output) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("opencv_resize_letterbox_transform stop requested, skipping transform");
      return;
    }

    if (input.empty()) {
      FP_LOG_DEBUG("opencv_resize_letterbox_transform received empty payload");
      return;
    }

    cv::Mat encoded(1, static_cast<int>(input.size), CV_8U, const_cast<uint8_t*>(input.data()));
    cv::Mat image = cv::imdecode(encoded, cv::IMREAD_COLOR);
    if (image.empty()) {
      FP_LOG_ERROR("opencv_resize_letterbox_transform failed to decode image bytes");
      return;
    }

    const int target_width = config_.output_width();
    const int target_height = config_.output_height();

    cv::Mat resized;
    if (UseLetterbox(resize_mode_)) {
      const float scale = std::min(static_cast<float>(target_width) / image.cols,
                                   static_cast<float>(target_height) / image.rows);
      const int resized_width = std::max(1, static_cast<int>(std::round(image.cols * scale)));
      const int resized_height = std::max(1, static_cast<int>(std::round(image.rows * scale)));

      cv::resize(image, resized, cv::Size(resized_width, resized_height), 0.0, 0.0,
                 interpolation_);

      cv::Mat canvas(target_height, target_width, resized.type(), pad_color_);
      const int offset_x = (target_width - resized_width) / 2;
      const int offset_y = (target_height - resized_height) / 2;
      resized.copyTo(canvas(cv::Rect(offset_x, offset_y, resized_width, resized_height)));
      resized = std::move(canvas);
    } else {
      cv::resize(image, resized, cv::Size(target_width, target_height), 0.0, 0.0, interpolation_);
    }

    std::vector<uint8_t> encoded_output;
    if (!cv::imencode(output_format_, resized, encoded_output, output_params_)) {
      FP_LOG_ERROR("opencv_resize_letterbox_transform failed to encode image");
      return;
    }

    auto buffer = AllocatePayloadBuffer(encoded_output.size());
    if (!buffer) {
      FP_LOG_ERROR("opencv_resize_letterbox_transform failed to allocate payload");
      return;
    }

    if (!encoded_output.empty()) {
      std::memcpy(buffer.get(), encoded_output.data(), encoded_output.size());
    }

    output = Payload(std::move(buffer), encoded_output.size());
    FP_LOG_DEBUG("opencv_resize_letterbox_transform emitted resized payload");
  }

 private:
  OpenCVResizeLetterboxConfig config_{};
  cv::Scalar pad_color_{0.0, 0.0, 0.0};
  int interpolation_{cv::INTER_LINEAR};
  std::string resize_mode_;
  std::string output_format_{".jpg"};
  std::vector<int> output_params_{};
};

extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating opencv_resize_letterbox_transform stage");
  return new OpenCVResizeLetterbox();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying opencv_resize_letterbox_transform stage");
  delete stage;
}
}
