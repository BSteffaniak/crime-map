# syntax=docker/dockerfile:1

# ── Stage 1: Rust builder ──────────────────────────────────────────
FROM rust:1-bookworm AS rust-builder

WORKDIR /app

# Install DuckDB build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY packages packages
COPY config config
COPY migrations migrations

# Build release binary with bundled DuckDB (no system libduckdb needed)
RUN cargo build --release --bin crime_map_server --features duckdb-bundled

# ── Stage 2: Frontend builder ──────────────────────────────────────
FROM oven/bun:1 AS frontend-builder

WORKDIR /app

# Copy frontend source and shared config
COPY app/package.json app/bun.lock ./app/
COPY app/ ./app/
COPY config/ ./config/

WORKDIR /app/app
RUN bun install --frozen-lockfile

# Build with the tiles URL passed as a build arg
ARG VITE_TILES_URL=""
ENV VITE_TILES_URL=${VITE_TILES_URL}
RUN bun run build

# ── Stage 3: Minimal runtime ──────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the server binary
COPY --from=rust-builder /app/target/release/crime_map_server ./crime_map_server

# Copy the frontend build
COPY --from=frontend-builder /app/app/dist ./app/dist

# data/generated/ is mounted as a volume at runtime (not baked in)
# data/conversations.db is created at runtime

EXPOSE 8080

ENV BIND_ADDR=0.0.0.0
ENV PORT=8080
ENV RUST_LOG=info

CMD ["./crime_map_server"]
