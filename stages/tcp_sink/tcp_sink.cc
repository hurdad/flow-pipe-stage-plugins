#include <google/protobuf/struct.pb.h>
#include "flowpipe/protobuf_config.h"
#include <netdb.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/stage.h"
#include "tcp_sink.pb.h"

using namespace flowpipe;

using TcpSinkConfig = flowpipe::stages::tcp::sink::v1::TcpSinkConfig;

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
  hints.ai_socktype = SOCK_STREAM;

  std::string port_str = std::to_string(port);
  int result = getaddrinfo(host.c_str(), port_str.c_str(), &hints, out_result);
  if (result != 0) {
    FP_LOG_ERROR("tcp_sink getaddrinfo failed: " + std::string(gai_strerror(result)));
    return false;
  }

  return true;
}

bool ConnectUnixSocket(const std::string& path, int& out_fd) {
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return false;
  }

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", path.c_str());

  if (connect(fd, reinterpret_cast<const sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) !=
      0) {
    CloseSocket(fd);
    return false;
  }

  out_fd = fd;
  return true;
}

bool ConnectInetSocket(const std::string& host, uint32_t port, int& out_fd) {
  addrinfo* results = nullptr;
  if (!ResolveDestination(host, port, &results)) {
    return false;
  }

  int fd = -1;
  for (addrinfo* info = results; info != nullptr; info = info->ai_next) {
    int candidate = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
    if (candidate < 0) {
      continue;
    }

    if (connect(candidate, info->ai_addr, info->ai_addrlen) == 0) {
      fd = candidate;
      break;
    }

    CloseSocket(candidate);
  }

  freeaddrinfo(results);

  if (fd < 0) {
    return false;
  }

  out_fd = fd;
  return true;
}

int SendFlags() {
#ifdef MSG_NOSIGNAL
  return MSG_NOSIGNAL;
#else
  return 0;
#endif
}
}  // namespace

// ============================================================
// TcpSink
// ============================================================
class TcpSink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "tcp_sink";
  }

  TcpSink() {
    FP_LOG_INFO("tcp_sink constructed");
  }

  ~TcpSink() override {
    CloseSocket(socket_fd_);
    FP_LOG_INFO("tcp_sink destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool Configure(const google::protobuf::Struct& config) override {
    TcpSinkConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<TcpSinkConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("tcp_sink invalid config: " + error);
      return false;
    }

    const bool use_unix_socket = !cfg.unix_socket_path().empty();
    if (!use_unix_socket && cfg.port() == 0) {
      FP_LOG_ERROR("tcp_sink requires port");
      return false;
    }

    std::string host = cfg.host();
    if (host.empty()) {
      host = "127.0.0.1";
    }

    int new_fd = -1;
    if (use_unix_socket) {
      if (!ConnectUnixSocket(cfg.unix_socket_path(), new_fd)) {
        FP_LOG_ERROR("tcp_sink failed to connect unix socket");
        return false;
      }
    } else {
      if (!ConnectInetSocket(host, cfg.port(), new_fd)) {
        FP_LOG_ERROR("tcp_sink failed to connect destination");
        return false;
      }
    }

    CloseSocket(socket_fd_);
    socket_fd_ = new_fd;
    config_ = std::move(cfg);

    FP_LOG_INFO("tcp_sink configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISinkStage
  // ------------------------------------------------------------
  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("tcp_sink stop requested, skipping payload");
      return;
    }

    if (payload.empty()) {
      FP_LOG_DEBUG("tcp_sink received empty payload");
      return;
    }

    if (socket_fd_ < 0) {
      FP_LOG_ERROR("tcp_sink socket not configured");
      return;
    }

    size_t total_sent = 0;
    const auto* data = static_cast<const uint8_t*>(payload.data());
    while (total_sent < payload.size) {
      ssize_t sent = send(socket_fd_, data + total_sent, payload.size - total_sent, SendFlags());
      if (sent <= 0) {
        FP_LOG_ERROR(std::string("tcp_sink send failed: ") + strerror(errno));
        CloseSocket(socket_fd_);
        return;
      }
      total_sent += static_cast<size_t>(sent);
    }

    FP_LOG_DEBUG("tcp_sink sent payload");
  }

 private:
  TcpSinkConfig config_{};
  int socket_fd_{-1};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating tcp_sink stage");
  return new TcpSink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying tcp_sink stage");
  delete stage;
}

}  // extern "C"
