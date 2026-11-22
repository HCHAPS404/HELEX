FROM rust:1.90-slim-bookworm AS builder

# Static glibc (not musl): glibc's memcpy/memset are markedly faster for the
# engine's bulk copies/fills, and a static-pie binary still runs FROM scratch.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release --target x86_64-unknown-linux-gnu && rm -rf src

COPY src/ ./src/
COPY scripts/pgo_gen.sh /build/pgo_gen.sh

# PGO: instrumented build -> profile run on synthetic data -> merge -> final.
RUN touch src/main.rs \
    && RUSTFLAGS="$RUSTFLAGS -C profile-generate=/build/pgo" \
       cargo build --release --target x86_64-unknown-linux-gnu \
    && sed -i 's/\r$//' /build/pgo_gen.sh \
    && sh /build/pgo_gen.sh /build/pgdata \
    && mkdir -p /tmp/po /tmp/ps \
    && DATA_DIR=/build/pgdata OUTPUT_DIR=/tmp/po SCRATCH_DIR=/tmp/ps NCPU=4 \
       ./target/x86_64-unknown-linux-gnu/release/shopping-sessions \
    && rustup component add llvm-tools-preview \
    && "$(rustc --print sysroot)/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata" \
       merge -o /build/merged.profdata /build/pgo \
    && rm -rf /build/pgo /build/pgdata /tmp/po /tmp/ps \
    && touch src/main.rs \
    && RUSTFLAGS="$RUSTFLAGS -C profile-use=/build/merged.profdata" \
       cargo build --release --target x86_64-unknown-linux-gnu

FROM scratch

COPY --from=builder --chmod=0755 \
    /build/target/x86_64-unknown-linux-gnu/release/shopping-sessions \
    /app/shopping-sessions
COPY --chmod=0755 run.sh /app/run.sh

WORKDIR /app
USER 1000
CMD ["/app/shopping-sessions"]
