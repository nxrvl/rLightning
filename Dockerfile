# Stage 1: Plan dependencies with cargo-chef
FROM rust:slim AS planner

WORKDIR /usr/src/rlightning

# Install cargo-chef
RUN cargo install cargo-chef --locked

# Copy all source to generate recipe
COPY . .

# Generate dependency recipe
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Build dependencies (this layer will be cached!)
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

# Install cargo-chef
RUN cargo install cargo-chef --locked

# Copy dependency recipe from planner
COPY --from=planner /usr/src/rlightning/recipe.json recipe.json

# Build dependencies - this is the cached layer that saves time!
RUN cargo chef cook --release --recipe-path recipe.json

# Now copy the actual source code
COPY . .

# Build the final binary (only rebuilds changed code, not dependencies)
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
