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
LOCAL_MODE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-build) SKIP_BUILD=true; shift ;;
        --client) ONLY_CLIENT="$2"; shift 2 ;;
        --server) ONLY_SERVER="$2"; shift 2 ;;
        --skip-report) SKIP_REPORT=true; shift ;;
        --no-cleanup) NO_CLEANUP=true; shift ;;
        --local) LOCAL_MODE=true; shift ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --skip-build    Skip Docker image building"
            echo "  --client NAME   Only run specified client (go-client, js-client, python-client)"
            echo "  --server NAME   Only test against specified server (redis, rlightning)"
            echo "  --skip-report   Skip comparison report generation"
            echo "  --no-cleanup    Leave containers running after tests"
            echo "  --local         Run test clients locally instead of in Docker containers"
            echo "                  (requires Go, Node.js, Python installed; servers still use Docker)"
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

# Check dependencies
if ! command -v docker &>/dev/null; then
    error "Docker is not installed or not in PATH"
    exit 1
fi
if ! docker info &>/dev/null; then
    error "Docker daemon is not running"
    exit 1
fi
if ! command -v python3 &>/dev/null; then
    error "python3 is required but not installed (used for JSON parsing and report generation)"
    exit 1
fi

# Helper: resolve server to host:port for local mode
resolve_host_port() {
    local server="$1"
    if [[ "$LOCAL_MODE" == true ]]; then
        if [[ "$server" == "redis" ]]; then
            echo "localhost ${REDIS_HOST_PORT:-6399}"
        else
            echo "localhost ${RLIGHTNING_HOST_PORT:-6400}"
        fi
    else
        echo "$server 6379"
    fi
}

# Helper: run a test client
run_test_client() {
    local client="$1" server="$2" result_file="$3" log_file="$4"
    local host_port
    host_port=$(resolve_host_port "$server")
    local host=${host_port% *}
    local port=${host_port#* }

    if [[ "$LOCAL_MODE" == true ]]; then
        case "$client" in
            go-client)
                REDIS_HOST="$host" REDIS_PORT="$port" REDIS_PASSWORD=test_password TEST_PREFIX="gotest:" \
                    "$SCRIPT_DIR/go-client/test-runner" > "$result_file" 2>"$log_file" || true
                ;;
            js-client)
                REDIS_HOST="$host" REDIS_PORT="$port" REDIS_PASSWORD=test_password TEST_PREFIX="jstest:" \
                    node "$SCRIPT_DIR/js-client/index.js" > "$result_file" 2>"$log_file" || true
                ;;
            python-client)
                REDIS_HOST="$host" REDIS_PORT="$port" REDIS_PASSWORD=test_password TEST_PREFIX="pytest:" \
                    python3 "$SCRIPT_DIR/python-client/test_compat.py" > "$result_file" 2>"$log_file" || true
                ;;
        esac
    else
        TARGET_HOST="$server" $COMPOSE --profile clients run --rm \
            -e "REDIS_HOST=$server" \
            "$client" > "$result_file" 2>"$log_file" || true
    fi
}

echo ""
echo "=========================================="
echo "  Multi-Language Redis Compatibility Tests"
echo "=========================================="
echo ""
log "Servers: ${SERVERS[*]}"
log "Clients: ${CLIENTS[*]}"
if [[ "$LOCAL_MODE" == true ]]; then
    log "Mode: local (clients run on host, servers in Docker)"
fi
echo ""

# Step 1: Build images (skip in local mode)
if [[ "$SKIP_BUILD" != true && "$LOCAL_MODE" != true ]]; then
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
elif [[ "$LOCAL_MODE" == true ]]; then
    log "Local mode: building test clients locally..."

    if [[ -d "$SCRIPT_DIR/go-client" ]]; then
        log "  Building go-client binary..."
        (cd "$SCRIPT_DIR/go-client" && go build -o test-runner . 2>&1) || { error "Failed to build go-client"; exit 1; }
    fi

    success "Local build complete"
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

        # Run test client (Docker container or local, depending on mode)
        # Note: test clients may exit non-zero when test failures occur.
        # This is expected when testing against rLightning (compatibility gaps).
        # We check the JSON output to determine actual success.
        run_test_client "$client" "$server" "$result_file" "$log_file"

        # Validate and summarize JSON output (pass file path as argument to avoid injection)
        if [[ -s "$result_file" ]] && python3 -c "import json,sys; json.load(open(sys.argv[1]))" "$result_file" 2>/dev/null; then
            read -r total passed failed <<< "$(python3 -c "
import json, sys
r = json.load(open(sys.argv[1]))
s = r.get('summary', {})
t = s.get('total', len(r.get('results', [])))
p = s.get('passed', sum(1 for x in r.get('results', []) if x.get('status') == 'pass'))
f = s.get('failed', 0)
print(t, p, f)
" "$result_file")"

            if [[ "$failed" -gt 0 ]]; then
                warn "  Results: ${passed}/${total} passed, ${failed} failed"
                # Only count as a run failure if testing against Redis (should be 100%)
                if [[ "$server" == "redis" ]]; then
                    FAIL_COUNT=$((FAIL_COUNT + 1))
                    FAILED=true
                fi
            else
                success "  Results: ${passed}/${total} passed"
            fi
        else
            error "  No valid JSON output produced"
            if [[ -f "$log_file" ]]; then
                echo "  --- stderr ---"
                tail -20 "$log_file" | sed 's/^/  /'
                echo "  --- end ---"
            fi
            FAIL_COUNT=$((FAIL_COUNT + 1))
            FAILED=true
        fi
    done

    # Flush databases between server tests to avoid key pollution
    if [[ "$server" == "redis" ]]; then
        $COMPOSE exec -T redis redis-cli -a test_password --no-auth-warning FLUSHALL 2>/dev/null || true
    elif [[ "$server" == "rlightning" ]]; then
        # Use redis-cli from the redis container to flush rLightning over the Docker network
        $COMPOSE exec -T redis redis-cli -h rlightning -a test_password --no-auth-warning FLUSHALL 2>/dev/null || true
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
