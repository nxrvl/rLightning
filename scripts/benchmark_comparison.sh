#!/usr/bin/env bash
#
# benchmark_comparison.sh - Automated benchmark comparison: rLightning vs Redis
#
# Runs both Redis and rLightning in Docker containers, executes the full
# benchmark suite against both, and generates a comparison report.
#
# Usage:
#   ./scripts/benchmark_comparison.sh [options]
#
# Options:
#   --quick       Run only a subset of benchmarks (faster)
#   --no-docker   Skip Docker-based comparison, run only local benchmarks
#   --output DIR  Output directory for reports (default: target/benchmark-report)
#   --help        Show this help message

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
QUICK_MODE=false
NO_DOCKER=false
OUTPUT_DIR="$PROJECT_DIR/target/benchmark-report"
REDIS_PORT=6379
RLIGHTNING_PORT=6380
REDIS_CONTAINER="bench-redis"
RLIGHTNING_CONTAINER="bench-rlightning"
DOCKER_MEMORY="128m"
DOCKER_CPU="1.0"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            QUICK_MODE=true
            shift
            ;;
        --no-docker)
            NO_DOCKER=true
            shift
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --help)
            head -20 "$0" | tail -15
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() { echo -e "${BLUE}[BENCH]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

cleanup() {
    log "Cleaning up Docker containers..."
    docker stop "$REDIS_CONTAINER" "$RLIGHTNING_CONTAINER" 2>/dev/null || true
    docker rm "$REDIS_CONTAINER" "$RLIGHTNING_CONTAINER" 2>/dev/null || true
}

trap cleanup EXIT

# Create output directory
mkdir -p "$OUTPUT_DIR"

REPORT_FILE="$OUTPUT_DIR/benchmark-report-$(date +%Y%m%d-%H%M%S).txt"

{
    echo "=============================================="
    echo "  rLightning vs Redis Benchmark Report"
    echo "  Generated: $(date)"
    echo "  System: $(uname -sr)"
    echo "  CPU: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || lscpu 2>/dev/null | grep 'Model name' | cut -d: -f2 | xargs || echo 'unknown')"
    echo "  Memory: $(sysctl -n hw.memsize 2>/dev/null | awk '{printf "%.1f GB", $1/1073741824}' || free -h 2>/dev/null | awk '/^Mem:/{print $2}' || echo 'unknown')"
    echo "=============================================="
    echo ""
} | tee "$REPORT_FILE"

# Step 1: Build rLightning in release mode
log "Building rLightning in release mode..."
cd "$PROJECT_DIR"
if cargo build --release 2>&1; then
    success "Build completed"
else
    error "Build failed"
    exit 1
fi

# Step 2: Run local benchmarks (in-process, no Docker needed)
log "Running local benchmark suite..."

LOCAL_BENCHES=("storage_bench" "throughput_bench" "command_category_bench" "advanced_features_bench" "cluster_bench" "protocol_bench" "replication_bench" "auth_bench")

if [ "$QUICK_MODE" = true ]; then
    LOCAL_BENCHES=("storage_bench" "command_category_bench" "cluster_bench")
fi

for bench in "${LOCAL_BENCHES[@]}"; do
    log "Running $bench..."
    {
        echo ""
        echo "--- $bench ---"
    } | tee -a "$REPORT_FILE"

    if cargo bench --bench "$bench" 2>&1 | tee -a "$REPORT_FILE"; then
        success "$bench completed"
    else
        warn "$bench had issues (may be non-fatal)"
    fi
done

# Step 3: Docker-based comparison benchmarks (optional)
if [ "$NO_DOCKER" = false ]; then
    log "Starting Docker-based comparison benchmarks..."

    # Check Docker availability
    if ! command -v docker &>/dev/null; then
        warn "Docker not found. Skipping Docker-based comparison."
    else
        # Build rLightning Docker image
        log "Building rLightning Docker image..."
        if docker build -q -t rlightning:latest "$PROJECT_DIR" 2>&1; then
            success "Docker image built"
        else
            warn "Failed to build Docker image. Skipping Docker benchmarks."
            NO_DOCKER=true
        fi
    fi

    if [ "$NO_DOCKER" = false ]; then
        # Start Redis
        log "Starting Redis container..."
        docker stop "$REDIS_CONTAINER" 2>/dev/null || true
        docker rm "$REDIS_CONTAINER" 2>/dev/null || true
        docker run -d \
            --name "$REDIS_CONTAINER" \
            -p "$REDIS_PORT":6379 \
            --memory "$DOCKER_MEMORY" \
            --cpus "$DOCKER_CPU" \
            redis:7-alpine \
            redis-server --maxmemory 100mb --maxmemory-policy allkeys-lru --protected-mode no

        # Start rLightning
        log "Starting rLightning container..."
        docker stop "$RLIGHTNING_CONTAINER" 2>/dev/null || true
        docker rm "$RLIGHTNING_CONTAINER" 2>/dev/null || true
        docker run -d \
            --name "$RLIGHTNING_CONTAINER" \
            -p "$RLIGHTNING_PORT":6379 \
            --memory "$DOCKER_MEMORY" \
            --cpus "$DOCKER_CPU" \
            rlightning:latest

        # Wait for startup
        log "Waiting for containers to start..."
        sleep 5

        # Run comparison benchmark
        log "Running redis_comparison_bench..."
        {
            echo ""
            echo "--- Docker Comparison Benchmarks ---"
        } | tee -a "$REPORT_FILE"

        if cargo bench --bench redis_comparison_bench 2>&1 | tee -a "$REPORT_FILE"; then
            success "Comparison benchmarks completed"
        else
            warn "Comparison benchmarks had issues"
        fi
    fi
fi

# Step 4: Summary
{
    echo ""
    echo "=============================================="
    echo "  Benchmark Report Complete"
    echo "  Report saved to: $REPORT_FILE"
    echo "  Criterion HTML reports: $PROJECT_DIR/target/criterion/"
    echo "=============================================="
} | tee -a "$REPORT_FILE"

success "All benchmarks completed. Report: $REPORT_FILE"
