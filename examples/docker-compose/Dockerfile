# A deliberately simple local demo image. Unlike the Kubernetes/release image,
# this builder resolves the public pinned Holylog tag over HTTPS so `docker
# compose up --build` needs no SSH agent, private registry, or cloud secret.
FROM rust:1.97-bookworm AS builder
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev git \
    && rm -rf /var/lib/apt/lists/* \
    && git config --global url."https://github.com/".insteadOf "ssh://git@github.com/"
COPY . .
RUN CARGO_NET_GIT_FETCH_WITH_CLI=true CARGO_BUILD_JOBS=2 \
    cargo build --release -p scripture-cli --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 scripture \
    && mkdir -p /var/lib/scripture \
    && chown scripture:scripture /var/lib/scripture
COPY --from=builder /src/target/release/scripture /usr/local/bin/scripture
VOLUME ["/var/lib/scripture"]
USER scripture
ENTRYPOINT ["scripture"]
CMD ["serve", "--config", "/etc/scripture/scripture.yaml"]
