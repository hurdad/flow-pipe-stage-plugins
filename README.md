# Flow Pipe Stage Plugins

Collection of Flow-Pipe stage plugins implemented in C++20.

## Plugins

- CSV Arrow source
- CSV Arrow sink
- File source
- File sink
- HTTP sink
- JSON Arrow source
- Kafka source
- Kafka sink
- NATS source
- NATS sink
- NATS JetStream source
- NATS JetStream sink
- ORC Arrow source
- ORC Arrow sink
- Parquet Arrow source
- Parquet Arrow sink
- Redis pub/sub source
- Redis pub/sub sink
- Redis stream source
- Redis stream sink
- TCP source
- TCP sink
- UDP source
- UDP sink

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
