FROM rust:slim AS builder

# Build arguments for versioning
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
ARG TARGETPLATFORM
ARG BUILDPLATFORM

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    make \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Display build information
RUN echo "Building rLightning ${VERSION} (${COMMIT}) on ${BUILDPLATFORM} for ${TARGETPLATFORM}"

WORKDIR /usr/src/rlightning

# Copy the dependencies and build them first
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs and bench files structure to build dependencies
RUN mkdir -p src benches && \
    echo "fn main() {}" > src/main.rs && \
    echo "fn main() {}" > benches/storage_bench.rs && \
    echo "fn main() {}" > benches/protocol_bench.rs && \
    echo "fn main() {}" > benches/throughput_bench.rs && \
    echo "fn main() {}" > benches/redis_comparison_bench.rs && \
    echo "fn main() {}" > benches/auth_bench.rs && \
    echo "fn main() {}" > benches/replication_bench.rs && \
    cargo build --release && \
    rm -rf src benches

# Now copy the actual source code
COPY . .

# Build the final binary
RUN cargo build --release

# Create a lean runtime image
FROM debian:bookworm-slim AS runtime

# Build arguments for labels
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

# Install minimal runtime dependencies and clean up in one layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /usr/src/rlightning/target/release/rlightning /usr/local/bin/

# Set up a non-root user for better security
RUN groupadd -r rlightning && useradd -r -g rlightning rlightning
USER rlightning

# Expose the Redis-compatible port
EXPOSE 6379

# Set environment variable to bind to all interfaces
ENV RLIGHTNING_BIND=0.0.0.0

# Add metadata labels
LABEL org.opencontainers.image.title="rLightning" \
    org.opencontainers.image.description="High-performance Redis-compatible in-memory key-value store" \
    org.opencontainers.image.version="${VERSION}" \
    org.opencontainers.image.created="${BUILD_DATE}" \
    org.opencontainers.image.licenses="MIT"

# Set the default command
CMD ["rlightning"]
