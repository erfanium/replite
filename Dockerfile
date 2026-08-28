# syntax=docker/dockerfile:1
FROM rust:1.85-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/sqld /usr/local/bin/sqld
ENV SQLD_ADDR=0.0.0.0:8080 \
    SQLD_DB_PATH=/data
VOLUME /data
EXPOSE 8080
CMD ["sqld"]
