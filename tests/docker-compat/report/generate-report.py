#!/usr/bin/env python3
"""
Comparison report generator for multi-language Redis compatibility tests.

Reads JSON test results from Go, JS, and Python clients run against both
Redis 7 and rLightning, then generates a comparison report highlighting
compatibility gaps.

Usage:
    python3 generate-report.py --results-dir ./results --output report.txt
    python3 generate-report.py --results-dir ./results  # prints to stdout
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime


def load_results(results_dir):
    """Load all JSON result files from the results directory.

    Returns dict keyed by (client, server) tuples.
    """
    results = {}
    if not os.path.isdir(results_dir):
        print(f"Error: results directory not found: {results_dir}", file=sys.stderr)
        return results

    for filename in sorted(os.listdir(results_dir)):
        if not filename.endswith(".json"):
            continue
        # Expected pattern: {client}-{server}.json
        # e.g., go-client-redis.json, js-client-rlightning.json
        base = filename[:-5]  # strip .json
        # Split on last occurrence of "-redis" or "-rlightning"
        server = None
        client = None
        for srv in ("rlightning", "redis"):
            suffix = f"-{srv}"
            if base.endswith(suffix):
                server = srv
                client = base[: -len(suffix)]
                break

        if not client or not server:
            print(f"Warning: skipping unrecognized file: {filename}", file=sys.stderr)
            continue

        filepath = os.path.join(results_dir, filename)
        try:
            with open(filepath) as f:
                data = json.load(f)
            results[(client, server)] = data
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: failed to load {filename}: {e}", file=sys.stderr)

    return results


def extract_test_map(report_data):
    """Build a dict of test_name -> result from a report's results array."""
    tests = {}
    for r in report_data.get("results", []):
        key = f"{r['category']}/{r['name']}"
        tests[key] = r
    return tests


def get_categories(report_data):
    """Get sorted list of unique categories from a report."""
    cats = set()
    for r in report_data.get("results", []):
        cats.add(r.get("category", "unknown"))
    return sorted(cats)


def compute_category_stats(report_data):
    """Compute per-category pass/fail/total counts."""
    stats = defaultdict(lambda: {"total": 0, "passed": 0, "failed": 0, "skipped": 0, "errored": 0})
    for r in report_data.get("results", []):
        cat = r.get("category", "unknown")
        stats[cat]["total"] += 1
        status = r.get("status", "error")
        if status == "pass":
            stats[cat]["passed"] += 1
        elif status == "fail":
            stats[cat]["failed"] += 1
        elif status == "skip":
            stats[cat]["skipped"] += 1
        else:
            stats[cat]["errored"] += 1
    return dict(stats)


def generate_report(results):
    """Generate the full comparison report text."""
    lines = []

    # Discover available clients and servers
    clients = sorted(set(c for c, s in results.keys()))
    servers = sorted(set(s for c, s in results.keys()))

    if not clients or not servers:
        return "No test results found.\n"

    lines.append("=" * 72)
    lines.append("  rLightning vs Redis - Multi-Language Compatibility Report")
    lines.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 72)
    lines.append("")

    # --- Section 1: Overall Summary Table ---
    lines.append("-" * 72)
    lines.append("  OVERALL SUMMARY")
    lines.append("-" * 72)
    lines.append("")

    # Build summary rows
    header = f"  {'Client':<20} {'Server':<15} {'Total':>6} {'Pass':>6} {'Fail':>6} {'Skip':>6} {'Rate':>8}"
    lines.append(header)
    lines.append("  " + "-" * 67)

    for client in clients:
        for server in servers:
            key = (client, server)
            if key not in results:
                continue
            data = results[key]
            summary = data.get("summary", {})
            total = summary.get("total", 0)
            passed = summary.get("passed", 0)
            failed = summary.get("failed", 0)
            skipped = summary.get("skipped", 0)
            rate = summary.get("pass_rate", (passed / total * 100) if total > 0 else 0)
            lines.append(
                f"  {client:<20} {server:<15} {total:>6} {passed:>6} {failed:>6} {skipped:>6} {rate:>7.1f}%"
            )
        lines.append("")

    # --- Section 2: Per-Category Pass Rates ---
    lines.append("-" * 72)
    lines.append("  PER-CATEGORY PASS RATES")
    lines.append("-" * 72)
    lines.append("")

    # Collect all categories across all results
    all_categories = set()
    for data in results.values():
        all_categories.update(get_categories(data))
    all_categories = sorted(all_categories)

    for client in clients:
        lang_label = client.replace("-client", "").upper()
        lines.append(f"  [{lang_label}]")

        cat_header = f"    {'Category':<25}"
        for server in servers:
            cat_header += f" {server:>15}"
        cat_header += "    Delta"
        lines.append(cat_header)
        lines.append("    " + "-" * (25 + 15 * len(servers) + 10))

        redis_key = (client, "redis")
        rl_key = (client, "rlightning")

        redis_cats = compute_category_stats(results[redis_key]) if redis_key in results else {}
        rl_cats = compute_category_stats(results[rl_key]) if rl_key in results else {}

        for cat in all_categories:
            row = f"    {cat:<25}"
            redis_rate = None
            rl_rate = None

            for server in servers:
                key = (client, server)
                if key in results:
                    cat_stats = redis_cats if server == "redis" else rl_cats
                    s = cat_stats.get(cat, {"total": 0, "passed": 0})
                    total = s["total"]
                    passed = s["passed"]
                    if total > 0:
                        rate = passed / total * 100
                        row += f" {passed:>3}/{total:<3} ({rate:>5.1f}%)"
                        if server == "redis":
                            redis_rate = rate
                        else:
                            rl_rate = rate
                    else:
                        row += f" {'N/A':>15}"
                else:
                    row += f" {'---':>15}"

            # Delta column
            if redis_rate is not None and rl_rate is not None:
                delta = rl_rate - redis_rate
                if delta < 0:
                    row += f"  {delta:>+.1f}% !!"
                elif delta == 0:
                    row += f"  {delta:>+.1f}%"
                else:
                    row += f"  {delta:>+.1f}%"
            else:
                row += "       ---"
            lines.append(row)

        lines.append("")

    # --- Section 3: Compatibility Gaps ---
    lines.append("-" * 72)
    lines.append("  COMPATIBILITY GAPS (Pass on Redis, Fail on rLightning)")
    lines.append("-" * 72)
    lines.append("")

    gap_count = 0
    for client in clients:
        redis_key = (client, "redis")
        rl_key = (client, "rlightning")

        if redis_key not in results or rl_key not in results:
            continue

        redis_tests = extract_test_map(results[redis_key])
        rl_tests = extract_test_map(results[rl_key])

        client_gaps = []
        for test_key, redis_result in sorted(redis_tests.items()):
            if redis_result.get("status") != "pass":
                continue
            rl_result = rl_tests.get(test_key)
            if rl_result and rl_result.get("status") != "pass":
                client_gaps.append((test_key, rl_result))

        lang_label = client.replace("-client", "").upper()
        if client_gaps:
            lines.append(f"  [{lang_label}] {len(client_gaps)} compatibility gap(s):")
            lines.append("")
            for test_key, rl_result in client_gaps:
                category, name = test_key.split("/", 1)
                status = rl_result.get("status", "unknown")
                error = rl_result.get("error", "no error message")
                lines.append(f"    - [{category}] {name}")
                lines.append(f"      Status: {status}")
                lines.append(f"      Error: {error}")
                lines.append("")
                gap_count += 1
        else:
            lines.append(f"  [{lang_label}] No compatibility gaps found!")
            lines.append("")

    # --- Section 4: Reverse Gaps (fail on Redis, pass on rLightning) ---
    lines.append("-" * 72)
    lines.append("  REVERSE GAPS (Fail on Redis, Pass on rLightning)")
    lines.append("-" * 72)
    lines.append("")

    reverse_gap_count = 0
    for client in clients:
        redis_key = (client, "redis")
        rl_key = (client, "rlightning")

        if redis_key not in results or rl_key not in results:
            continue

        redis_tests = extract_test_map(results[redis_key])
        rl_tests = extract_test_map(results[rl_key])

        reverse_gaps = []
        for test_key, rl_result in sorted(rl_tests.items()):
            if rl_result.get("status") != "pass":
                continue
            redis_result = redis_tests.get(test_key)
            if redis_result and redis_result.get("status") != "pass":
                reverse_gaps.append((test_key, redis_result))

        lang_label = client.replace("-client", "").upper()
        if reverse_gaps:
            lines.append(f"  [{lang_label}] {len(reverse_gaps)} reverse gap(s):")
            lines.append("")
            for test_key, redis_result in reverse_gaps:
                category, name = test_key.split("/", 1)
                lines.append(f"    - [{category}] {name}")
                lines.append("")
                reverse_gap_count += 1
        else:
            lines.append(f"  [{lang_label}] None")
            lines.append("")

    # --- Section 5: Final Verdict ---
    lines.append("=" * 72)
    lines.append("  VERDICT")
    lines.append("=" * 72)
    lines.append("")

    total_tests_per_client = {}
    for client in clients:
        rl_key = (client, "rlightning")
        if rl_key in results:
            total_tests_per_client[client] = results[rl_key].get("summary", {}).get("total", 0)

    if gap_count == 0:
        lines.append("  FULL COMPATIBILITY: All tests that pass on Redis 7 also pass on rLightning.")
    else:
        lines.append(f"  {gap_count} COMPATIBILITY GAP(S) DETECTED across {len(clients)} client(s).")
        lines.append("  Review the gaps above and consider fixing the rLightning implementation.")

    if reverse_gap_count > 0:
        lines.append(f"  {reverse_gap_count} reverse gap(s) found (rLightning passes where Redis fails).")

    lines.append("")
    lines.append("=" * 72)
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate compatibility comparison report from test results"
    )
    parser.add_argument(
        "--results-dir",
        required=True,
        help="Directory containing JSON result files",
    )
    parser.add_argument(
        "--output",
        help="Output file path (default: stdout)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output structured JSON instead of text",
    )
    args = parser.parse_args()

    results = load_results(args.results_dir)
    if not results:
        print("Error: no valid result files found", file=sys.stderr)
        sys.exit(1)

    if args.json:
        report = generate_json_report(results)
        output = json.dumps(report, indent=2)
    else:
        output = generate_report(results)

    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(output)


def generate_json_report(results):
    """Generate a structured JSON report for programmatic consumption."""
    clients = sorted(set(c for c, s in results.keys()))
    servers = sorted(set(s for c, s in results.keys()))

    report = {
        "generated": datetime.now().isoformat(),
        "clients": clients,
        "servers": servers,
        "summary": {},
        "category_rates": {},
        "compatibility_gaps": {},
        "reverse_gaps": {},
    }

    # Summary per client/server
    for client in clients:
        report["summary"][client] = {}
        for server in servers:
            key = (client, server)
            if key in results:
                s = results[key].get("summary", {})
                report["summary"][client][server] = {
                    "total": s.get("total", 0),
                    "passed": s.get("passed", 0),
                    "failed": s.get("failed", 0),
                    "skipped": s.get("skipped", 0),
                    "pass_rate": s.get("pass_rate", 0),
                }

    # Category rates
    for client in clients:
        report["category_rates"][client] = {}
        for server in servers:
            key = (client, server)
            if key in results:
                report["category_rates"][client][server] = compute_category_stats(results[key])

    # Compatibility gaps
    for client in clients:
        redis_key = (client, "redis")
        rl_key = (client, "rlightning")
        gaps = []

        if redis_key in results and rl_key in results:
            redis_tests = extract_test_map(results[redis_key])
            rl_tests = extract_test_map(results[rl_key])

            for test_key, redis_result in sorted(redis_tests.items()):
                if redis_result.get("status") != "pass":
                    continue
                rl_result = rl_tests.get(test_key)
                if rl_result and rl_result.get("status") != "pass":
                    gaps.append({
                        "test": test_key,
                        "redis_status": "pass",
                        "rlightning_status": rl_result.get("status"),
                        "error": rl_result.get("error", ""),
                    })

        report["compatibility_gaps"][client] = gaps

    # Reverse gaps
    for client in clients:
        redis_key = (client, "redis")
        rl_key = (client, "rlightning")
        gaps = []

        if redis_key in results and rl_key in results:
            redis_tests = extract_test_map(results[redis_key])
            rl_tests = extract_test_map(results[rl_key])

            for test_key, rl_result in sorted(rl_tests.items()):
                if rl_result.get("status") != "pass":
                    continue
                redis_result = redis_tests.get(test_key)
                if redis_result and redis_result.get("status") != "pass":
                    gaps.append({
                        "test": test_key,
                        "redis_status": redis_result.get("status"),
                        "rlightning_status": "pass",
                    })

        report["reverse_gaps"][client] = gaps

    return report


if __name__ == "__main__":
    main()
