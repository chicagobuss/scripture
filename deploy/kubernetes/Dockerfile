# Build the versioned Scripture process image (product binary: scripture).
#
# Requires BuildKit SSH agent forwarding for the pinned Holylog git dependency:
#
#   DOCKER_BUILDKIT=1 docker build --ssh default \
#     -f deploy/kubernetes/Dockerfile \
#     -t scripture:0.1.0 \
#     .
#
# Image tag should match the package version you are shipping. This Dockerfile
# does not embed credentials, inventory hostnames, or operator overlays.

# syntax=docker/dockerfile:1.7

FROM rust:1.97-bookworm AS builder
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev openssh-client git \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p -m 0700 /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts
COPY . .
RUN --mount=type=ssh \
    cargo build --release -p scripture-cli --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 scripture
COPY --from=builder /src/target/release/scripture /usr/local/bin/scripture
USER scripture
ENTRYPOINT ["scripture"]
CMD ["serve", "--config", "/etc/scripture/scripture.yaml"]
