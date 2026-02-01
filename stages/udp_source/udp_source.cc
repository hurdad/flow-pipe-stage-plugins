#include <google/protobuf/struct.pb.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "udp_source.pb.h"

using namespace flowpipe;

using UdpSourceConfig = flowpipe::stages::udp::source::v1::UdpSourceConfig;

namespace {
constexpr size_t kMaxUdpPayloadSize = 65507;
constexpr int kDefaultPollTimeoutMs = 1000;

void CloseSocket(int& fd) {
  if (fd >= 0) {
    close(fd);
    fd = -1;
  }
}

void UnlinkUnixSocket(const std::string& path) {
  if (!path.empty()) {
    unlink(path.c_str());
  }
}

bool BindUnixSocket(const std::string& path, int& out_fd) {
  int fd = socket(AF_UNIX, SOCK_DGRAM, 0);
  if (fd < 0) {
    return false;
  }

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", path.c_str());

  UnlinkUnixSocket(path);
  if (bind(fd, reinterpret_cast<const sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) !=
      0) {
    CloseSocket(fd);
    return false;
  }

  out_fd = fd;
  return true;
}

bool ResolveBindAddress(const std::string& address, uint32_t port, addrinfo** out_result) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_flags = AI_PASSIVE;

  std::string port_str = std::to_string(port);
  int result = getaddrinfo(address.empty() ? nullptr : address.c_str(), port_str.c_str(), &hints,
                           out_result);
  if (result != 0) {
    FP_LOG_ERROR("udp_source getaddrinfo failed: " + std::string(gai_strerror(result)));
    return false;
  }

  return true;
}

bool BindSocket(const addrinfo* info, int& out_fd) {
  int fd = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
  if (fd < 0) {
    return false;
  }

  int reuse = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    FP_LOG_ERROR("udp_source failed to set SO_REUSEADDR");
  }

  if (bind(fd, info->ai_addr, info->ai_addrlen) != 0) {
    CloseSocket(fd);
    return false;
  }

  out_fd = fd;
  return true;
}
}  // namespace

// ============================================================
// UdpSource
// ============================================================
class UdpSource final : public ISourceStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "udp_source";
  }

  UdpSource() {
    FP_LOG_INFO("udp_source constructed");
  }

  ~UdpSource() override {
    CloseSocket(socket_fd_);
    UnlinkUnixSocket(unix_socket_path_);
    FP_LOG_INFO("udp_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    UdpSourceConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<UdpSourceConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("udp_source invalid config: " + error);
      return false;
    }

    const bool use_unix_socket = !cfg.unix_socket_path().empty();
    if (!use_unix_socket && cfg.port() == 0) {
      FP_LOG_ERROR("udp_source requires port");
      return false;
    }

    std::string bind_address = cfg.bind_address();
    if (bind_address.empty()) {
      bind_address = "0.0.0.0";
    }

    size_t max_payload_size = cfg.max_payload_size() == 0
                                  ? kMaxUdpPayloadSize
                                  : static_cast<size_t>(cfg.max_payload_size());
    if (max_payload_size > kMaxUdpPayloadSize) {
      FP_LOG_INFO("udp_source max_payload_size capped at 65507 bytes");
      max_payload_size = kMaxUdpPayloadSize;
    }

    int poll_timeout_ms = cfg.poll_timeout_ms() > 0 ? cfg.poll_timeout_ms() : kDefaultPollTimeoutMs;

    int new_fd = -1;
    if (use_unix_socket) {
      if (!BindUnixSocket(cfg.unix_socket_path(), new_fd)) {
        FP_LOG_ERROR("udp_source failed to bind unix socket");
        return false;
      }
    } else {
      addrinfo* results = nullptr;
      if (!ResolveBindAddress(bind_address, cfg.port(), &results)) {
        return false;
      }

      for (addrinfo* info = results; info != nullptr; info = info->ai_next) {
        if (BindSocket(info, new_fd)) {
          break;
        }
      }

      freeaddrinfo(results);
    }

    if (new_fd < 0) {
      FP_LOG_ERROR("udp_source failed to bind socket");
      return false;
    }

    CloseSocket(socket_fd_);
    if (unix_socket_path_ != cfg.unix_socket_path()) {
      UnlinkUnixSocket(unix_socket_path_);
    }
    socket_fd_ = new_fd;
    config_ = std::move(cfg);
    max_payload_size_ = max_payload_size;
    poll_timeout_ms_ = poll_timeout_ms;
    unix_socket_path_ = config_.unix_socket_path();

    FP_LOG_INFO("udp_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("udp_source stop requested, skipping produce");
      return false;
    }

    if (socket_fd_ < 0) {
      FP_LOG_ERROR("udp_source socket not configured");
      return false;
    }

    pollfd poll_fd{};
    poll_fd.fd = socket_fd_;
    poll_fd.events = POLLIN;

    int poll_result = poll(&poll_fd, 1, poll_timeout_ms_);
    if (poll_result < 0) {
      FP_LOG_ERROR(std::string("udp_source poll failed: ") + strerror(errno));
      return false;
    }

    if (poll_result == 0 || (poll_fd.revents & POLLIN) == 0) {
      return false;
    }

    std::vector<char> buffer(max_payload_size_);
    ssize_t received = recvfrom(socket_fd_, buffer.data(), buffer.size(), 0, nullptr, nullptr);
    if (received < 0) {
      FP_LOG_ERROR(std::string("udp_source recvfrom failed: ") + strerror(errno));
      return false;
    }

    auto payload_buffer = AllocatePayloadBuffer(static_cast<size_t>(received));
    if (!payload_buffer) {
      FP_LOG_ERROR("udp_source failed to allocate payload");
      return false;
    }

    if (received > 0) {
      std::memcpy(payload_buffer.get(), buffer.data(), static_cast<size_t>(received));
    }

    payload = Payload(std::move(payload_buffer), static_cast<size_t>(received));
    return true;
  }

 private:
  UdpSourceConfig config_{};
  int socket_fd_{-1};
  size_t max_payload_size_{kMaxUdpPayloadSize};
  int poll_timeout_ms_{kDefaultPollTimeoutMs};
  std::string unix_socket_path_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating udp_source stage");
  return new UdpSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying udp_source stage");
  delete stage;
}

}  // extern "C"
