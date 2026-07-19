# Release image for `scripture serve` (WP09).
#
# Must NOT enable campaign-faults. Campaign/test actor images live under
# deploy/kubernetes/correctness/.
#
# The builder resolves the exact Holylog RC only from Kellnr. Supply a token as
# a BuildKit secret; it is neither copied into the image nor retained in a layer:
#   DOCKER_BUILDKIT=1 docker build \
#     --secret id=fleet_token,src=/path/to/fleet-token \
#     -f deploy/kubernetes/Dockerfile -t scripture:<version> .
#
# No credentials or fleet topology are embedded in this image.
# syntax=docker/dockerfile:1.7

FROM rust:1.97-bookworm AS builder
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY . .
# Release binary: no campaign-faults feature.
RUN --mount=type=secret,id=fleet_token,required=true \
    set -eu; \
    cargo_home="${CARGO_HOME:-/usr/local/cargo}"; \
    mkdir -p "$cargo_home"; \
    printf '%s\n' \
      '[registries.fleet]' \
      'index = "sparse+http://10.0.0.5:30990/api/v1/crates/"' \
      '' \
      '[registry]' \
      'global-credential-providers = ["cargo:token"]' \
      > "$cargo_home/config.toml"; \
    CARGO_REGISTRIES_FLEET_TOKEN="$(cat /run/secrets/fleet_token)" \
      CARGO_BUILD_JOBS=2 cargo build --release -p scripture-cli --locked; \
    rm -f "$cargo_home/config.toml"

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 scripture
COPY --from=builder /src/target/release/scripture /usr/local/bin/scripture
USER scripture
ENTRYPOINT ["scripture"]
CMD ["serve", "--config", "/etc/scripture/scripture.yaml"]
