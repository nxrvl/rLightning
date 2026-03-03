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

# Helper: resolve replica host:port for local mode
resolve_replica_host_port() {
    local server="$1"
    if [[ "$LOCAL_MODE" == true ]]; then
        if [[ "$server" == "redis" ]]; then
            echo "localhost ${REDIS_REPLICA_HOST_PORT:-6401}"
        else
            echo "localhost ${RLIGHTNING_REPLICA_HOST_PORT:-6402}"
        fi
    else
        echo "${server}-replica 6379"
    fi
}

# Helper: resolve cluster node 1 host:port for local mode
resolve_cluster_host_port() {
    local server="$1"
    if [[ "$LOCAL_MODE" == true ]]; then
        if [[ "$server" == "redis" ]]; then
            echo "localhost ${REDIS_CLUSTER_PORT_1:-6403}"
        else
            echo "localhost ${RLIGHTNING_CLUSTER_PORT_1:-6406}"
        fi
    else
        echo "${server}-cluster-1 6379"
    fi
}

# Helper: run a test client
run_test_client() {
    local client="$1" server="$2" result_file="$3" log_file="$4"
    local host_port
    host_port=$(resolve_host_port "$server")
    local host=${host_port% *}
    local port=${host_port#* }

    # Resolve replica host:port if replicas are running
    local replica_host="" replica_port=""
    local replica_host_port
    replica_host_port=$(resolve_replica_host_port "$server")
    replica_host=${replica_host_port% *}
    replica_port=${replica_host_port#* }

    # Resolve cluster host:port if cluster is running
    local cluster_host="" cluster_port=""
    local cluster_host_port
    cluster_host_port=$(resolve_cluster_host_port "$server")
    cluster_host=${cluster_host_port% *}
    cluster_port=${cluster_host_port#* }

    if [[ "$LOCAL_MODE" == true ]]; then
        case "$client" in
            go-client)
                REDIS_HOST="$host" REDIS_PORT="$port" REDIS_PASSWORD=test_password TEST_PREFIX="gotest:" \
                    REDIS_REPLICA_HOST="$replica_host" REDIS_REPLICA_PORT="$replica_port" \
                    REDIS_CLUSTER_HOST="$cluster_host" REDIS_CLUSTER_PORT="$cluster_port" \
                    "$SCRIPT_DIR/go-client/test-runner" > "$result_file" 2>"$log_file" || true
                ;;
            js-client)
                REDIS_HOST="$host" REDIS_PORT="$port" REDIS_PASSWORD=test_password TEST_PREFIX="jstest:" \
                    REDIS_REPLICA_HOST="$replica_host" REDIS_REPLICA_PORT="$replica_port" \
                    REDIS_CLUSTER_HOST="$cluster_host" REDIS_CLUSTER_PORT="$cluster_port" \
                    node "$SCRIPT_DIR/js-client/index.js" > "$result_file" 2>"$log_file" || true
                ;;
            python-client)
                local py_bin="python3"
                if [[ -x "$SCRIPT_DIR/python-client/.venv/bin/python" ]]; then
                    py_bin="$SCRIPT_DIR/python-client/.venv/bin/python"
                fi
                REDIS_HOST="$host" REDIS_PORT="$port" REDIS_PASSWORD=test_password TEST_PREFIX="pytest:" \
                    REDIS_REPLICA_HOST="$replica_host" REDIS_REPLICA_PORT="$replica_port" \
                    REDIS_CLUSTER_HOST="$cluster_host" REDIS_CLUSTER_PORT="$cluster_port" \
                    "$py_bin" "$SCRIPT_DIR/python-client/test_compat.py" > "$result_file" 2>"$log_file" || true
                ;;
        esac
    else
        TARGET_HOST="$server" $COMPOSE --profile clients run --rm \
            -e "REDIS_HOST=$server" \
            -e "REDIS_REPLICA_HOST=${server}-replica" \
            -e "REDIS_REPLICA_PORT=6379" \
            -e "REDIS_CLUSTER_HOST=${server}-cluster-1" \
            -e "REDIS_CLUSTER_PORT=6379" \
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

    # Only build rlightning image if it's in the server list
    for server in "${SERVERS[@]}"; do
        if [[ "$server" == "rlightning" ]]; then
            log "  Building rLightning image..."
            $COMPOSE build rlightning
            break
        fi
    done

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

    if [[ -d "$SCRIPT_DIR/js-client" ]]; then
        log "  Installing js-client dependencies..."
        (cd "$SCRIPT_DIR/js-client" && npm install --production 2>&1) || { error "Failed to install js-client dependencies"; exit 1; }
    fi

    if [[ -d "$SCRIPT_DIR/python-client" ]]; then
        log "  Installing python-client dependencies..."
        VENV_DIR="$SCRIPT_DIR/python-client/.venv"
        if [[ ! -d "$VENV_DIR" ]]; then
            python3 -m venv "$VENV_DIR" 2>&1 || { error "Failed to create Python venv"; exit 1; }
        fi
        ("$VENV_DIR/bin/pip" install -r "$SCRIPT_DIR/python-client/requirements.txt" -q 2>&1) || { error "Failed to install python-client dependencies"; exit 1; }
    fi

    success "Local build complete"
    echo ""
fi

# Step 2: Prepare results directory
mkdir -p "$RESULTS_DIR"
rm -f "$RESULTS_DIR"/*.json "$RESULTS_DIR"/*.log

# Step 3: Start server infrastructure (only those requested by --server filter)
log "Starting server infrastructure..."
$COMPOSE up -d "${SERVERS[@]}"

for server in "${SERVERS[@]}"; do
    if [[ "$server" == "redis" ]]; then
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
    elif [[ "$server" == "rlightning" ]]; then
        # Need redis container for its redis-cli to probe rlightning over Docker network
        if ! $COMPOSE ps --status running redis 2>/dev/null | grep -q redis; then
            $COMPOSE up -d redis
            for i in $(seq 1 15); do
                if $COMPOSE exec -T redis redis-cli -a test_password --no-auth-warning ping 2>/dev/null | grep -q PONG; then
                    break
                fi
                if [[ $i -eq 15 ]]; then
                    error "Helper Redis container failed to start within 15 seconds"
                    $COMPOSE logs redis
                    exit 1
                fi
                sleep 1
            done
        fi
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
    fi
done

# Step 3b: Start replica infrastructure for replication tests
log "Starting replica infrastructure..."
for server in "${SERVERS[@]}"; do
    replica="${server}-replica"
    $COMPOSE --profile replication up -d "$replica" 2>/dev/null || true

    if [[ "$server" == "redis" ]]; then
        log "Waiting for Redis replica to be healthy..."
        for i in $(seq 1 30); do
            if $COMPOSE exec -T redis redis-cli -h redis-replica -a test_password --no-auth-warning ping 2>/dev/null | grep -q PONG; then
                success "  Redis replica is ready"
                break
            fi
            if [[ $i -eq 30 ]]; then
                warn "  Redis replica failed to start — replication tests will be skipped"
            fi
            sleep 1
        done
    elif [[ "$server" == "rlightning" ]]; then
        log "Waiting for rLightning replica to be healthy..."
        for i in $(seq 1 30); do
            if $COMPOSE exec -T redis redis-cli -h rlightning-replica -a test_password --no-auth-warning ping 2>/dev/null | grep -q PONG; then
                success "  rLightning replica is ready"
                break
            fi
            if [[ $i -eq 30 ]]; then
                warn "  rLightning replica failed to start — replication tests will be skipped"
            fi
            sleep 1
        done
    fi
done

# Step 3c: Start cluster infrastructure for cluster tests
log "Starting cluster infrastructure..."
for server in "${SERVERS[@]}"; do
    # Start the 3 cluster nodes
    $COMPOSE --profile cluster up -d "${server}-cluster-1" "${server}-cluster-2" "${server}-cluster-3" 2>/dev/null || true

    if [[ "$server" == "redis" ]]; then
        log "Waiting for Redis cluster nodes to be healthy..."
        all_cluster_ready=true
        for node_num in 1 2 3; do
            node_name="redis-cluster-${node_num}"
            for i in $(seq 1 30); do
                if $COMPOSE exec -T "${node_name}" redis-cli -a test_password --no-auth-warning ping 2>/dev/null | grep -q PONG; then
                    success "  ${node_name} is ready"
                    break
                fi
                if [[ $i -eq 30 ]]; then
                    warn "  ${node_name} failed to start — cluster tests will be skipped"
                    all_cluster_ready=false
                fi
                sleep 1
            done
        done

        # Initialize the Redis cluster if all nodes are ready
        if [[ "$all_cluster_ready" == true ]]; then
            log "Initializing Redis cluster..."
            $COMPOSE exec -T redis-cluster-1 redis-cli -a test_password --no-auth-warning \
                --cluster create redis-cluster-1:6379 redis-cluster-2:6379 redis-cluster-3:6379 \
                --cluster-replicas 0 --cluster-yes 2>/dev/null || warn "  Redis cluster init returned non-zero (may already be initialized)"

            # Wait for cluster to converge
            for i in $(seq 1 30); do
                cluster_info=$($COMPOSE exec -T redis-cluster-1 redis-cli -a test_password --no-auth-warning cluster info 2>/dev/null || true)
                if echo "$cluster_info" | grep -q "cluster_state:ok"; then
                    success "  Redis cluster is ready (cluster_state:ok)"
                    break
                fi
                if [[ $i -eq 30 ]]; then
                    warn "  Redis cluster did not converge — cluster tests may fail"
                fi
                sleep 1
            done
        fi
    elif [[ "$server" == "rlightning" ]]; then
        log "Waiting for rLightning cluster nodes to be healthy..."
        all_cluster_ready=true
        for node_num in 1 2 3; do
            node_name="rlightning-cluster-${node_num}"
            for i in $(seq 1 30); do
                if $COMPOSE exec -T redis redis-cli -h "${node_name}" -a test_password --no-auth-warning ping 2>/dev/null | grep -q PONG; then
                    success "  ${node_name} is ready"
                    break
                fi
                if [[ $i -eq 30 ]]; then
                    warn "  ${node_name} failed to start — cluster tests will be skipped"
                    all_cluster_ready=false
                fi
                sleep 1
            done
        done

        # Initialize the rLightning cluster using CLUSTER MEET + ADDSLOTS
        if [[ "$all_cluster_ready" == true ]]; then
            log "Initializing rLightning cluster..."
            # Node 1 meets nodes 2 and 3
            $COMPOSE exec -T redis redis-cli -h rlightning-cluster-1 -a test_password --no-auth-warning \
                CLUSTER MEET rlightning-cluster-2 6379 2>/dev/null || true
            $COMPOSE exec -T redis redis-cli -h rlightning-cluster-1 -a test_password --no-auth-warning \
                CLUSTER MEET rlightning-cluster-3 6379 2>/dev/null || true

            sleep 2

            # Assign slots: node1 gets 0-5460, node2 gets 5461-10922, node3 gets 10923-16383
            log "  Assigning slots to rLightning cluster nodes..."
            slots_1=$(seq 0 5460 | tr '\n' ' ')
            slots_2=$(seq 5461 10922 | tr '\n' ' ')
            slots_3=$(seq 10923 16383 | tr '\n' ' ')

            $COMPOSE exec -T redis redis-cli -h rlightning-cluster-1 -a test_password --no-auth-warning \
                CLUSTER ADDSLOTS $slots_1 2>/dev/null || true
            $COMPOSE exec -T redis redis-cli -h rlightning-cluster-2 -a test_password --no-auth-warning \
                CLUSTER ADDSLOTS $slots_2 2>/dev/null || true
            $COMPOSE exec -T redis redis-cli -h rlightning-cluster-3 -a test_password --no-auth-warning \
                CLUSTER ADDSLOTS $slots_3 2>/dev/null || true

            # Wait for cluster to converge
            for i in $(seq 1 30); do
                cluster_info=$($COMPOSE exec -T redis redis-cli -h rlightning-cluster-1 -a test_password --no-auth-warning cluster info 2>/dev/null || true)
                if echo "$cluster_info" | grep -q "cluster_state:ok"; then
                    success "  rLightning cluster is ready (cluster_state:ok)"
                    break
                fi
                if [[ $i -eq 30 ]]; then
                    warn "  rLightning cluster did not converge — cluster tests may fail"
                fi
                sleep 1
            done
        fi
    fi
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
            read -r total passed failed errored <<< "$(python3 -c "
import json, sys
r = json.load(open(sys.argv[1]))
s = r.get('summary', {})
t = s.get('total', len(r.get('results', [])))
p = s.get('passed', sum(1 for x in r.get('results', []) if x.get('status') == 'pass'))
f = s.get('failed', 0)
e = s.get('errored', sum(1 for x in r.get('results', []) if x.get('status') == 'error'))
print(t, p, f, e)
" "$result_file")"

            if [[ "$failed" -gt 0 || "$errored" -gt 0 ]]; then
                _detail="${passed}/${total} passed, ${failed} failed"
                if [[ "$errored" -gt 0 ]]; then
                    _detail="${_detail}, ${errored} errored"
                fi
                warn "  Results: ${_detail}"
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

if [[ "$RUN_COUNT" -eq 0 ]]; then
    error "No test runs were executed - check --client and --server values"
    exit 1
elif [[ "$FAILED" == true ]]; then
    error "Some test runs failed - check logs in ${RESULTS_DIR}/"
    exit 1
else
    success "All test runs completed successfully"
fi
