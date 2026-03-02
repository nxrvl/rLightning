#!/usr/bin/env bash
set -euo pipefail

# Multi-language Docker compatibility test runner
# Runs Go, JS, and Python Redis client tests against both Redis 7 and rLightning,
# then generates a comparison report highlighting compatibility gaps.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RESULTS_DIR="$SCRIPT_DIR/results"
COMPOSE="docker compose"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

CLIENTS=("go-client" "js-client" "python-client")
SERVERS=("redis" "rlightning")

cleanup() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} Cleaning up containers..."
    $COMPOSE down -v --remove-orphans 2>/dev/null || true
}

trap cleanup EXIT

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARN:${NC} $1"
}

# Parse arguments
SKIP_BUILD=false
ONLY_CLIENT=""
ONLY_SERVER=""
SKIP_REPORT=false
NO_CLEANUP=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-build) SKIP_BUILD=true; shift ;;
        --client) ONLY_CLIENT="$2"; shift 2 ;;
        --server) ONLY_SERVER="$2"; shift 2 ;;
        --skip-report) SKIP_REPORT=true; shift ;;
        --no-cleanup) NO_CLEANUP=true; shift ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --skip-build    Skip Docker image building"
            echo "  --client NAME   Only run specified client (go-client, js-client, python-client)"
            echo "  --server NAME   Only test against specified server (redis, rlightning)"
            echo "  --skip-report   Skip comparison report generation"
            echo "  --no-cleanup    Leave containers running after tests"
            echo "  --help          Show this help"
            exit 0
            ;;
        *) error "Unknown option: $1"; exit 1 ;;
    esac
done

# Apply filters
if [[ -n "$ONLY_CLIENT" ]]; then
    CLIENTS=("$ONLY_CLIENT")
fi
if [[ -n "$ONLY_SERVER" ]]; then
    SERVERS=("$ONLY_SERVER")
fi

if [[ "$NO_CLEANUP" == true ]]; then
    trap - EXIT
fi

# Check Docker availability
if ! command -v docker &>/dev/null; then
    error "Docker is not installed or not in PATH"
    exit 1
fi
if ! docker info &>/dev/null; then
    error "Docker daemon is not running"
    exit 1
fi

echo ""
echo "=========================================="
echo "  Multi-Language Redis Compatibility Tests"
echo "=========================================="
echo ""
log "Servers: ${SERVERS[*]}"
log "Clients: ${CLIENTS[*]}"
echo ""

# Step 1: Build images
if [[ "$SKIP_BUILD" != true ]]; then
    log "Building Docker images..."

    log "  Building rLightning image..."
    $COMPOSE build rlightning

    for client in "${CLIENTS[@]}"; do
        if [[ -d "$SCRIPT_DIR/${client}" ]]; then
            log "  Building ${client} image..."
            $COMPOSE --profile clients build "$client"
        else
            warn "  ${client} directory not found, skipping build"
        fi
    done

    success "Image build complete"
    echo ""
fi

# Step 2: Prepare results directory
mkdir -p "$RESULTS_DIR"
rm -f "$RESULTS_DIR"/*.json "$RESULTS_DIR"/*.log

# Step 3: Start server infrastructure
log "Starting server infrastructure..."
$COMPOSE up -d redis rlightning

log "Waiting for Redis 7 to be healthy..."
for i in $(seq 1 30); do
    if $COMPOSE exec -T redis redis-cli -a test_password --no-auth-warning ping 2>/dev/null | grep -q PONG; then
        success "  Redis 7 is ready"
        break
    fi
    if [[ $i -eq 30 ]]; then
        error "Redis 7 failed to start within 30 seconds"
        $COMPOSE logs redis
        exit 1
    fi
    sleep 1
done

log "Waiting for rLightning to be healthy..."
for i in $(seq 1 30); do
    if $COMPOSE exec -T redis redis-cli -h rlightning -a test_password --no-auth-warning ping 2>/dev/null | grep -q PONG; then
        success "  rLightning is ready"
        break
    fi
    if [[ $i -eq 30 ]]; then
        error "rLightning failed to start within 30 seconds"
        $COMPOSE logs rlightning
        exit 1
    fi
    sleep 1
done

echo ""

# Step 4: Run tests
FAILED=false
RUN_COUNT=0
FAIL_COUNT=0

for server in "${SERVERS[@]}"; do
    log "Running tests against ${server}..."

    for client in "${CLIENTS[@]}"; do
        if [[ ! -d "$SCRIPT_DIR/${client}" ]]; then
            warn "  Skipping ${client} (directory not found)"
            continue
        fi

        log "  ${client} -> ${server}..."

        result_file="${RESULTS_DIR}/${client}-${server}.json"
        log_file="${RESULTS_DIR}/${client}-${server}.log"
        RUN_COUNT=$((RUN_COUNT + 1))

        # Run client container with target server override
        if TARGET_HOST="$server" $COMPOSE --profile clients run --rm \
            -e "REDIS_HOST=$server" \
            "$client" > "$result_file" 2>"$log_file"; then
            :
        else
            exit_code=$?
            error "  ${client} exited with code ${exit_code}"
            if [[ -f "$log_file" ]]; then
                echo "  --- stderr ---"
                tail -20 "$log_file" | sed 's/^/  /'
                echo "  --- end ---"
            fi
            FAIL_COUNT=$((FAIL_COUNT + 1))
            FAILED=true
            continue
        fi

        # Validate and summarize JSON output
        if [[ -s "$result_file" ]] && python3 -c "import json; json.load(open('$result_file'))" 2>/dev/null; then
            total=$(python3 -c "import json; r=json.load(open('$result_file')); print(r.get('summary', {}).get('total', len(r.get('results', []))))")
            passed=$(python3 -c "import json; r=json.load(open('$result_file')); print(r.get('summary', {}).get('passed', sum(1 for t in r.get('results', []) if t.get('status') == 'pass')))")
            failed=$(python3 -c "import json; r=json.load(open('$result_file')); print(r.get('summary', {}).get('failed', 0))")

            if [[ "$failed" -gt 0 ]]; then
                warn "  Results: ${passed}/${total} passed, ${failed} failed"
            else
                success "  Results: ${passed}/${total} passed"
            fi
        else
            error "  Invalid or empty JSON output"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            FAILED=true
        fi
    done

    # Flush databases between server tests to avoid key pollution
    if [[ "$server" == "redis" ]]; then
        $COMPOSE exec -T redis redis-cli -a test_password --no-auth-warning FLUSHALL 2>/dev/null || true
    elif [[ "$server" == "rlightning" ]]; then
        $COMPOSE exec -T rlightning sh -c 'echo -e "AUTH test_password\r\nFLUSHALL\r\n" | nc localhost 6379' 2>/dev/null || true
    fi

    echo ""
done

# Step 5: Generate comparison report
if [[ "$SKIP_REPORT" != true ]]; then
    if [[ -f "$SCRIPT_DIR/report/generate-report.py" ]]; then
        log "Generating comparison report..."
        if python3 "$SCRIPT_DIR/report/generate-report.py" \
            --results-dir "$RESULTS_DIR" \
            --output "$RESULTS_DIR/comparison-report.txt"; then

            if [[ -f "$RESULTS_DIR/comparison-report.txt" ]]; then
                echo ""
                echo "=========================================="
                echo "  COMPATIBILITY COMPARISON REPORT"
                echo "=========================================="
                echo ""
                cat "$RESULTS_DIR/comparison-report.txt"
            fi
        else
            warn "Report generation failed"
        fi
    else
        log "Skipping report generation (report/generate-report.py not found)"
    fi
fi

# Summary
echo ""
echo "=========================================="
echo "  TEST RUN SUMMARY"
echo "=========================================="
echo ""
echo "  Runs completed: ${RUN_COUNT}"
echo "  Run failures:   ${FAIL_COUNT}"
echo "  Results dir:    ${RESULTS_DIR}/"
echo ""

if ls "$RESULTS_DIR"/*.json &>/dev/null; then
    echo "  Result files:"
    for f in "$RESULTS_DIR"/*.json; do
        size=$(wc -c < "$f" | tr -d ' ')
        echo "    $(basename "$f") (${size} bytes)"
    done
fi

echo ""

if [[ "$FAILED" == true ]]; then
    error "Some test runs failed - check logs in ${RESULTS_DIR}/"
    exit 1
else
    success "All test runs completed successfully"
fi
