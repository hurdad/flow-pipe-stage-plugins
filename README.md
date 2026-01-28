# Flow Pipe Stage Plugins

Collection of Flow-Pipe stage plugins implemented in C++20.

## Plugins

- Kafka source
- Kafka sink
- CSV source
- CSV sink
- ORC source
- ORC sink
- Redis pub/sub source
- Redis pub/sub sink
- Redis stream source
- Redis stream sink
- TCP source
- TCP sink
- UDP source
- UDP sink
- HTTP sink
- NATS source
- NATS sink
- NATS JetStream source
- NATS JetStream sink

## Requirements

- Flow-Pipe runtime installed at `/opt/flow-pipe` (headers + runtime shared library).
- CMake 3.20+ and a C++20 compiler.
- Protobuf development libraries.
- libcurlpp development libraries.
- librdkafka development libraries (Kafka stages).
- hiredis development libraries (Redis stages).
- libnats development libraries (NATS stages).

## Build

```sh
cmake -S . -B build
cmake --build build
```

### Format

The CMake build provides a `format` target when `clang-format` is available.

```sh
cmake --build build --target format
```

## Install

The shared libraries install to `/opt/flow-pipe/plugins`.

```sh
cmake --install build
```
