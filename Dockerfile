FROM rust:1.90-slim-bookworm AS builder

ENV RUSTFLAGS="-C target-cpu=x86-64-v2"
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

COPY src/ ./src/
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim

RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY --from=builder /build/target/release/shopping-sessions /app/shopping-sessions
COPY run.sh /app/run.sh
RUN chmod +x /app/shopping-sessions /app/run.sh

WORKDIR /app
CMD ["/app/shopping-sessions"]
