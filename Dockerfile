# ============================================================
# Build stage plugin (dev image target)
# ============================================================
ARG FLOW_PIPE_TAG=main-ubuntu24.04

FROM ghcr.io/hurdad/flow-pipe-dev:${FLOW_PIPE_TAG} AS dev

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    ca-certificates \
    lsb-release \
    wget \
 && wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
 && apt-get install -y --no-install-recommends \
    ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
 && rm apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
    libarrow-dev \
    libparquet-dev \
    libhiredis-dev \
    librdkafka-dev \
    libcurlpp-dev \
    libnats-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cmake -S . -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=${CMAKE_PREFIX_PATH} \
 && cmake --build build \
 && ctest --test-dir build --output-on-failure \
 && cmake --install build


# ============================================================
# Runtime image with plugin
# ============================================================
FROM ghcr.io/hurdad/flow-pipe-runtime:${FLOW_PIPE_TAG} AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    ca-certificates \
    lsb-release \
    wget \
 && wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
 && apt-get install -y --no-install-recommends \
    ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
 && rm apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
    libarrow2300 \
    libparquet2300 \
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
