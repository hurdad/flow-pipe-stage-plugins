# ============================================================
# Build stage plugin
# ============================================================
ARG FLOW_PIPE_TAG=main-ubuntu24.04

FROM ghcr.io/hurdad/flow-pipe-dev:${FLOW_PIPE_TAG} AS build

RUN apt-get update \
 && apt-get install -y --no-install-recommends librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cmake -S . -B build -G Ninja \
 && cmake --build build \
 && ctest --test-dir build --output-on-failure \
 && cmake --install build


# ============================================================
# Runtime image with plugin
# ============================================================
FROM ghcr.io/hurdad/flow-pipe-runtime:${FLOW_PIPE_TAG}

RUN apt-get update \
 && apt-get install -y --no-install-recommends librdkafka1 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build \
  /opt/flow-pipe/plugins/*.so \
  /opt/flow-pipe/plugins/

ENV FLOW_PIPE_PLUGIN_PATH=/opt/flow-pipe/plugins
