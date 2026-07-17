# Versioned product image for `scripture`.
#
# Requires BuildKit SSH forwarding for the pinned Holylog git dependency:
#   DOCKER_BUILDKIT=1 docker build --ssh default \
#     -f deploy/kubernetes/Dockerfile -t scripture:<version> .
#
# No credentials or fleet topology are embedded in this image.
# syntax=docker/dockerfile:1.7

FROM rust:1.97-bookworm AS builder
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev openssh-client git \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p -m 0700 /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts
COPY . .
# Home-lab builders are shared with correctness work; cap Rust parallelism so
# an image build does not exhaust the host while compiling the Git dependency.
RUN --mount=type=ssh CARGO_BUILD_JOBS=2 cargo build --release -p scripture-cli --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 scripture
COPY --from=builder /src/target/release/scripture /usr/local/bin/scripture
USER scripture
ENTRYPOINT ["scripture"]
CMD ["serve", "--config", "/etc/scripture/scripture.yaml"]
