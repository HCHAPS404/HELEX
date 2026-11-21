FROM rust:1.90-slim-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/*
RUN rustup target add x86_64-unknown-linux-musl

ENV RUSTFLAGS="-C target-cpu=x86-64-v2" \
    CC_x86_64_unknown_linux_musl="musl-gcc" \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="musl-gcc"
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release --target x86_64-unknown-linux-musl && rm -rf src

COPY src/ ./src/
RUN touch src/main.rs && cargo build --release --target x86_64-unknown-linux-musl

FROM scratch

COPY --from=builder --chmod=0755 \
    /build/target/x86_64-unknown-linux-musl/release/shopping-sessions \
    /app/shopping-sessions
COPY --chmod=0755 run.sh /app/run.sh

WORKDIR /app
USER 1000
CMD ["/app/shopping-sessions"]
