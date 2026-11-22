FROM rust:1.90-slim-bookworm AS builder

# Static glibc (musl's memcpy/memset were ~15% slower); x86-64-v3 = AVX2/BMI2,
# always present on the scorer's GCP n2 (Cascade/Ice Lake) hosts.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
ENV TARGET=x86_64-unknown-linux-gnu
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release --target $TARGET && rm -rf src

COPY src/ ./src/
COPY tools/pgo/generate /build/generate

# Profile-guided build: instrumented binary -> run on a small official-shaped
# dataset (the challenge generator) -> llvm-profdata merge -> optimized build.
# Measured ~12% less CPU on the official 20M data. Best effort: if any PGO
# step fails, fall back to a plain release build.
RUN set -e; \
    pgo() { \
      rustup component add llvm-tools \
      && touch src/main.rs \
      && RUSTFLAGS="$RUSTFLAGS -Cprofile-generate=/build/pgo" cargo build --release --target $TARGET \
      && chmod +x /build/generate \
      && /build/generate --seed 7 --events 2000000 --clicks 500000 --output /build/pgdata \
      && mkdir -p /tmp/po \
      && DATA_DIR=/build/pgdata OUTPUT_DIR=/tmp/po SCRATCH_DIR=/tmp NCPU=4 \
         ./target/$TARGET/release/shopping-sessions \
      && "$(rustc --print sysroot)/lib/rustlib/$TARGET/bin/llvm-profdata" merge \
         -o /build/merged.profdata /build/pgo \
      && touch src/main.rs \
      && RUSTFLAGS="$RUSTFLAGS -Cprofile-use=/build/merged.profdata" cargo build --release --target $TARGET; \
    }; \
    if pgo; then echo "PGO build OK"; \
    else echo "PGO failed; plain build"; touch src/main.rs; cargo build --release --target $TARGET; fi; \
    rm -rf /build/pgo /build/pgdata /tmp/po

FROM scratch

COPY --from=builder --chmod=0755 \
    /build/target/x86_64-unknown-linux-gnu/release/shopping-sessions \
    /app/shopping-sessions
COPY --chmod=0755 run.sh /app/run.sh

WORKDIR /app
USER 1000
CMD ["/app/shopping-sessions"]
