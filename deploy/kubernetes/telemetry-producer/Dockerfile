# Telemetry producer + lab sink image (no Holylog git dep; no SSH mount).
#
#   DOCKER_BUILDKIT=1 docker build \
#     -f deploy/kubernetes/telemetry-producer/Dockerfile \
#     -t scripture-telemetry-producer:0.1.0 \
#     .
#
# Import onto the schedule node (imagePullPolicy: Never) before applying
# lab-sink.yaml / producer.yaml.
# syntax=docker/dockerfile:1.7

FROM rust:1.97-bookworm AS builder
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN CARGO_BUILD_JOBS=2 cargo build --release -p scripture-telemetry-producer --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 scripture
COPY --from=builder /src/target/release/scripture-telemetry-producer /usr/local/bin/scripture-telemetry-producer
COPY --from=builder /src/target/release/raw-lines-lab-sink /usr/local/bin/raw-lines-lab-sink
USER scripture
ENTRYPOINT ["scripture-telemetry-producer"]
