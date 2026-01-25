# Flow Pipe Stage Plugins

Collection of Flow-Pipe stage plugins implemented in C++20.

## Plugins

- Kafka source
- Kafka sink
- S3 source
- S3 sink
- SNS sink
- SQS source
- SQS sink

## Requirements

- Flow-Pipe runtime installed at `/opt/flow-pipe` (headers + runtime shared library).
- CMake 3.20+ and a C++20 compiler.
- Protobuf development libraries.
- librdkafka development libraries (Kafka stages).
- AWS SDK for C++ (S3/SNS/SQS stages). The build uses a FetchContent fallback if it is not installed.

## Build

```sh
cmake -S . -B build
cmake --build build
```

## Install

The shared libraries install to `/opt/flow-pipe/plugins`.

```sh
cmake --install build
```
