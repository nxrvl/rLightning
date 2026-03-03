"""Main entry point for Python Redis compatibility tests."""

import json
import os
import sys
import time

import redis

from tests.framework import run_all

# Import all test modules to register them
import tests.connection
import tests.strings
import tests.hashes
import tests.lists
import tests.sets
import tests.sorted_sets
import tests.keys
import tests.transactions
import tests.pubsub
import tests.pipeline
import tests.scripting
import tests.streams
import tests.advanced
import tests.edge_cases
import tests.server
import tests.persistence
import tests.acl
import tests.blocking
import tests.memory
import tests.replication
import tests.cluster
import tests.sentinel
import tests.python_specific


def main():
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    password = os.environ.get("REDIS_PASSWORD", "")
    prefix = os.environ.get("TEST_PREFIX", "pytest:")

    r = redis.Redis(
        host=host,
        port=port,
        password=password or None,
        db=0,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=10,
    )

    # Wait for server to be ready
    for i in range(30):
        try:
            r.ping()
            break
        except Exception:
            time.sleep(1)
    else:
        try:
            r.ping()
        except Exception as e:
            print(f"Failed to connect to Redis at {host}:{port}: {e}", file=sys.stderr)
            sys.exit(1)

    # Detect server type
    server_type = "unknown"
    server_version = ""
    try:
        info = r.info("server")
        server_version = info.get("redis_version", "")
        raw_info = r.execute_command("INFO", "server")
        if isinstance(raw_info, str) and ("rlightning" in raw_info or "rLightning" in raw_info):
            server_type = "rlightning"
        else:
            server_type = "redis"
    except Exception:
        pass

    print(f"Connected to {server_type} at {host}:{port} (version: {server_version})", file=sys.stderr)
    print(f"Running tests with prefix: {prefix}", file=sys.stderr)

    start_time = time.time()
    results = run_all(r, prefix)
    total_duration = round((time.time() - start_time) * 1000)

    # Build summary
    summary = {
        "total": 0,
        "passed": 0,
        "failed": 0,
        "skipped": 0,
        "errored": 0,
        "pass_rate": 0,
        "categories": {},
    }

    cat_totals = {}
    cat_passed = {}

    for result in results:
        summary["total"] += 1
        cat = result["category"]
        cat_totals[cat] = cat_totals.get(cat, 0) + 1
        status = result["status"]
        if status == "pass":
            summary["passed"] += 1
            cat_passed[cat] = cat_passed.get(cat, 0) + 1
        elif status == "fail":
            summary["failed"] += 1
        elif status == "skip":
            summary["skipped"] += 1
        elif status == "error":
            summary["errored"] += 1

    if summary["total"] > 0:
        summary["pass_rate"] = (summary["passed"] / summary["total"]) * 100

    for cat, total in cat_totals.items():
        passed = cat_passed.get(cat, 0)
        summary["categories"][cat] = {
            "total": total,
            "passed": passed,
            "pass_rate": (passed / total * 100) if total > 0 else 0,
        }

    report = {
        "client": {
            "language": "python",
            "library": "redis-py",
            "version": redis.__version__,
        },
        "server": {
            "host": host,
            "port": port,
            "type": server_type,
            "version": server_version,
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "duration_ms": total_duration,
        "results": results,
        "summary": summary,
    }

    # Output JSON report to stdout
    print(json.dumps(report, indent=2))

    # Print summary to stderr
    print(f"\n=== Test Summary ===", file=sys.stderr)
    print(
        f"Total: {summary['total']} | Pass: {summary['passed']} | Fail: {summary['failed']} "
        f"| Skip: {summary['skipped']} | Error: {summary['errored']} "
        f"| Rate: {summary['pass_rate']:.1f}%",
        file=sys.stderr,
    )
    for cat, cs in summary["categories"].items():
        print(f"  {cat:<20} {cs['passed']}/{cs['total']} ({cs['pass_rate']:.1f}%)", file=sys.stderr)

    r.close()
    sys.exit(1 if summary["failed"] > 0 or summary["errored"] > 0 else 0)


if __name__ == "__main__":
    main()
