# Build the separate experimental correctness-campaign tool image.
#
# Requires BuildKit SSH agent forwarding for the pinned Holylog git dependency:
#
#   DOCKER_BUILDKIT=1 docker build --ssh default \
#     -f deploy/kubernetes/correctness/Dockerfile \
#     -t scripture:correctness-0.1.0 \
#     .
#
# This intentionally is not the normal `scripture` process image. Credentials,
# inventory hostnames, and operator overlays are never embedded.

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
    cargo build --release -p scripture-campaign --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 scripture
COPY --from=builder /src/target/release/scripture-campaign /usr/local/bin/scripture-campaign
USER scripture
ENTRYPOINT ["scripture-campaign"]
CMD ["--help"]
