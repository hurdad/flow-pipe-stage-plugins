# ============================================================
# Build stage plugin (dev image target)
# ============================================================
ARG FLOW_PIPE_TAG=main-ubuntu24.04

FROM ghcr.io/hurdad/flow-pipe-dev:${FLOW_PIPE_TAG} AS dev

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    libhiredis-dev \
    librdkafka-dev \
    libcurlpp-dev \
    libnats-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/opt/flow-pipe \
 && cmake --build build \
 && ctest --test-dir build --output-on-failure \
 && cmake --install build


# ============================================================
# Runtime image with plugin
# ============================================================
FROM ghcr.io/hurdad/flow-pipe-runtime:${FLOW_PIPE_TAG} AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    libhiredis1.1.0 \
    librdkafka1 \
    libcurlpp0t64 \
    libnats3.7t64 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=dev \
  /opt/flow-pipe/plugins/*.so \
  /opt/flow-pipe/plugins/

COPY --from=dev \
  /opt/flow-pipe/lib/ \
  /opt/flow-pipe/lib/

ENV FLOW_PIPE_PLUGIN_PATH=/opt/flow-pipe/plugins
