#include <google/protobuf/struct.pb.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <string>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "udp_sink.pb.h"

using namespace flowpipe;

using UdpSinkConfig = flowpipe::v1::stages::udp::sink::v1::UdpSinkConfig;

namespace {
void CloseSocket(int& fd) {
  if (fd >= 0) {
    close(fd);
    fd = -1;
  }
}

bool ResolveDestination(const std::string& host, uint32_t port, addrinfo** out_result) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;

  std::string port_str = std::to_string(port);
  int result = getaddrinfo(host.c_str(), port_str.c_str(), &hints, out_result);
  if (result != 0) {
    FP_LOG_ERROR("udp_sink getaddrinfo failed: " + std::string(gai_strerror(result)));
    return false;
  }

  return true;
}

bool BuildUnixDestination(const std::string& path, sockaddr_storage& destination,
                          socklen_t& destination_len) {
  sockaddr_un addr{};
  if (path.size() >= sizeof(addr.sun_path)) {
    FP_LOG_ERROR("udp_sink unix socket path too long");
    return false;
  }
  addr.sun_family = AF_UNIX;
  std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", path.c_str());
  destination_len = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + path.size() + 1);
  std::memcpy(&destination, &addr, sizeof(addr));
  return true;
}
}  // namespace

// ============================================================
// UdpSink
// ============================================================
class UdpSink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "udp_sink";
  }

  UdpSink() {
    FP_LOG_INFO("udp_sink constructed");
  }

  ~UdpSink() override {
    CloseSocket(socket_fd_);
    FP_LOG_INFO("udp_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    UdpSinkConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<UdpSinkConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("udp_sink invalid config: " + error);
      return false;
    }

    const bool use_unix_socket = !cfg.unix_socket_path().empty();
    if (!use_unix_socket && cfg.port() == 0) {
      FP_LOG_ERROR("udp_sink requires port");
      return false;
    }

    std::string host = cfg.host();
    if (host.empty()) {
      host = "127.0.0.1";
    }

    int new_fd = -1;
    sockaddr_storage destination{};
    socklen_t destination_len = 0;
    if (use_unix_socket) {
      new_fd = socket(AF_UNIX, SOCK_DGRAM, 0);
      if (new_fd >= 0 &&
          !BuildUnixDestination(cfg.unix_socket_path(), destination, destination_len)) {
        CloseSocket(new_fd);
        new_fd = -1;
      }
    } else {
      addrinfo* results = nullptr;
      if (!ResolveDestination(host, cfg.port(), &results)) {
        return false;
      }

      for (addrinfo* info = results; info != nullptr; info = info->ai_next) {
        int fd = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
        if (fd < 0) {
          continue;
        }

        std::memcpy(&destination, info->ai_addr, info->ai_addrlen);
        destination_len = static_cast<socklen_t>(info->ai_addrlen);
        new_fd = fd;
        break;
      }

      freeaddrinfo(results);
    }

    if (new_fd < 0) {
      FP_LOG_ERROR("udp_sink failed to resolve destination");
      return false;
    }

    CloseSocket(socket_fd_);
    socket_fd_ = new_fd;
    destination_ = destination;
    destination_len_ = destination_len;
    config_ = std::move(cfg);

    FP_LOG_INFO("udp_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("udp_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("udp_sink received empty payload");
      return;
    }

    if (socket_fd_ < 0) {
      FP_LOG_ERROR("udp_sink socket not configured");
      return;
    }

    ssize_t sent = sendto(socket_fd_, payload.data(), payload.size, 0,
                          reinterpret_cast<const sockaddr*>(&destination_), destination_len_);
    if (sent < 0) {
      FP_LOG_ERROR(std::string("udp_sink sendto failed: ") + strerror(errno));
      return;
    }

    if (static_cast<size_t>(sent) != payload.size) {
      FP_LOG_ERROR("udp_sink truncated send");
      return;
    }

    FP_LOG_DEBUG("udp_sink sent payload");
  }

 private:
  UdpSinkConfig config_{};
  int socket_fd_{-1};
  sockaddr_storage destination_{};
  socklen_t destination_len_{0};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating udp_sink stage");
  return new UdpSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying udp_sink stage");
  delete stage;
}

}  // extern "C"
