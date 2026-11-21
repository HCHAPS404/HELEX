FROM rust:1.90-slim-bookworm AS builder

# Static glibc (not musl): glibc's memcpy/memset are markedly faster for the
# engine's bulk copies/fills, and a static-pie binary still runs FROM scratch.
ENV RUSTFLAGS="-C target-cpu=x86-64-v2 -C target-feature=+crt-static"
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release --target x86_64-unknown-linux-gnu && rm -rf src

COPY src/ ./src/
RUN touch src/main.rs && cargo build --release --target x86_64-unknown-linux-gnu

FROM scratch

COPY --from=builder --chmod=0755 \
    /build/target/x86_64-unknown-linux-gnu/release/shopping-sessions \
    /app/shopping-sessions
COPY --chmod=0755 run.sh /app/run.sh

WORKDIR /app
USER 1000
CMD ["/app/shopping-sessions"]
