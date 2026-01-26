#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <fcntl.h>
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
#include "flowpipe/stage.h"
#include "tcp_source.pb.h"

using namespace flowpipe;

using TcpSourceConfig = flowpipe::stages::tcp::source::v1::TcpSourceConfig;

namespace {
constexpr size_t kDefaultTcpPayloadSize = 65536;
constexpr size_t kMaxTcpPayloadSize = 1048576;
constexpr int kDefaultPollTimeoutMs = 1000;
constexpr int kListenBacklog = 16;

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

bool SetSocketNonBlocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return false;
  }
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool BindUnixSocket(const std::string& path, int& out_fd) {
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
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

  if (listen(fd, kListenBacklog) != 0) {
    CloseSocket(fd);
    return false;
  }

  out_fd = fd;
  return true;
}

bool ResolveBindAddress(const std::string& address, uint32_t port, addrinfo** out_result) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  std::string port_str = std::to_string(port);
  int result = getaddrinfo(address.empty() ? nullptr : address.c_str(), port_str.c_str(), &hints,
                           out_result);
  if (result != 0) {
    FP_LOG_ERROR("tcp_source getaddrinfo failed: " + std::string(gai_strerror(result)));
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
    FP_LOG_ERROR("tcp_source failed to set SO_REUSEADDR");
  }

  if (bind(fd, info->ai_addr, info->ai_addrlen) != 0) {
    CloseSocket(fd);
    return false;
  }

  if (listen(fd, kListenBacklog) != 0) {
    CloseSocket(fd);
    return false;
  }

  out_fd = fd;
  return true;
}
}  // namespace

// ============================================================
// TcpSource
// ============================================================
class TcpSource final : public ISourceStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "tcp_source";
  }

  TcpSource() {
    FP_LOG_INFO("tcp_source constructed");
  }

  ~TcpSource() override {
    CloseSocket(client_fd_);
    CloseSocket(listen_fd_);
    UnlinkUnixSocket(unix_socket_path_);
    FP_LOG_INFO("tcp_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(config, &json);

    if (!status.ok()) {
      FP_LOG_ERROR("tcp_source failed to serialize config");
      return false;
    }

    TcpSourceConfig cfg;
    status = google::protobuf::util::JsonStringToMessage(json, &cfg);

    if (!status.ok()) {
      FP_LOG_ERROR("tcp_source invalid config");
      return false;
    }

    const bool use_unix_socket = !cfg.unix_socket_path().empty();
    if (!use_unix_socket && cfg.port() == 0) {
      FP_LOG_ERROR("tcp_source requires port");
      return false;
    }

    std::string bind_address = cfg.bind_address();
    if (bind_address.empty()) {
      bind_address = "0.0.0.0";
    }

    size_t max_payload_size = cfg.max_payload_size() == 0
                                  ? kDefaultTcpPayloadSize
                                  : static_cast<size_t>(cfg.max_payload_size());
    if (max_payload_size > kMaxTcpPayloadSize) {
      FP_LOG_INFO("tcp_source max_payload_size capped at 1048576 bytes");
      max_payload_size = kMaxTcpPayloadSize;
    }

    int poll_timeout_ms = cfg.poll_timeout_ms() > 0 ? cfg.poll_timeout_ms() : kDefaultPollTimeoutMs;

    int new_fd = -1;
    if (use_unix_socket) {
      if (!BindUnixSocket(cfg.unix_socket_path(), new_fd)) {
        FP_LOG_ERROR("tcp_source failed to bind unix socket");
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
      FP_LOG_ERROR("tcp_source failed to bind socket");
      return false;
    }

    CloseSocket(client_fd_);
    CloseSocket(listen_fd_);
    if (unix_socket_path_ != cfg.unix_socket_path()) {
      UnlinkUnixSocket(unix_socket_path_);
    }
    listen_fd_ = new_fd;
    config_ = std::move(cfg);
    max_payload_size_ = max_payload_size;
    poll_timeout_ms_ = poll_timeout_ms;
    unix_socket_path_ = config_.unix_socket_path();

    FP_LOG_INFO("tcp_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("tcp_source stop requested, skipping produce");
      return false;
    }

    if (listen_fd_ < 0) {
      FP_LOG_ERROR("tcp_source socket not configured");
      return false;
    }

    pollfd poll_fds[2]{};
    nfds_t poll_count = 0;

    poll_fds[poll_count].fd = listen_fd_;
    poll_fds[poll_count].events = POLLIN;
    poll_count++;

    if (client_fd_ >= 0) {
      poll_fds[poll_count].fd = client_fd_;
      poll_fds[poll_count].events = POLLIN | POLLHUP | POLLERR;
      poll_count++;
    }

    int poll_result = poll(poll_fds, poll_count, poll_timeout_ms_);
    if (poll_result < 0) {
      FP_LOG_ERROR(std::string("tcp_source poll failed: ") + strerror(errno));
      return false;
    }

    if (poll_result == 0) {
      return false;
    }

    if (poll_fds[0].revents & POLLIN) {
      if (client_fd_ < 0) {
        int accepted_fd = accept(listen_fd_, nullptr, nullptr);
        if (accepted_fd < 0) {
          FP_LOG_ERROR(std::string("tcp_source accept failed: ") + strerror(errno));
          return false;
        }
        if (!SetSocketNonBlocking(accepted_fd)) {
          FP_LOG_ERROR(std::string("tcp_source failed to set non-blocking client socket: ") +
                       strerror(errno));
          CloseSocket(accepted_fd);
          return false;
        }
        client_fd_ = accepted_fd;
      }
    }

    if (client_fd_ < 0) {
      return false;
    }

    if (poll_count > 1) {
      const short revents = poll_fds[1].revents;
      if (revents & (POLLHUP | POLLERR)) {
        CloseSocket(client_fd_);
        return false;
      }

      if ((revents & POLLIN) == 0) {
        return false;
      }
    }

    std::vector<char> buffer(max_payload_size_);
    ssize_t received = recv(client_fd_, buffer.data(), buffer.size(), 0);
    if (received <= 0) {
      if (received < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          return false;
        }
        FP_LOG_ERROR(std::string("tcp_source recv failed: ") + strerror(errno));
      }
      CloseSocket(client_fd_);
      return false;
    }

    auto payload_buffer = AllocatePayloadBuffer(static_cast<size_t>(received));
    if (!payload_buffer) {
      FP_LOG_ERROR("tcp_source failed to allocate payload");
      return false;
    }

    std::memcpy(payload_buffer.get(), buffer.data(), static_cast<size_t>(received));

    payload = Payload(std::move(payload_buffer), static_cast<size_t>(received));
    return true;
  }

 private:
  TcpSourceConfig config_{};
  int listen_fd_{-1};
  int client_fd_{-1};
  size_t max_payload_size_{kDefaultTcpPayloadSize};
  int poll_timeout_ms_{kDefaultPollTimeoutMs};
  std::string unix_socket_path_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating tcp_source stage");
  return new TcpSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying tcp_source stage");
  delete stage;
}

}  // extern "C"
