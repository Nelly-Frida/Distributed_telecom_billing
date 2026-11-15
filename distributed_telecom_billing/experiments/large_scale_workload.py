"""Large-scale workload experiment.

Runs a real workload (not simulated data - every request is a genuine
socket round trip through Node1 -> Apache -> billing_api -> NODE-2 ->
dispatcher -> a rating node) at scale, e.g. 100,000 requests, using a
concurrent workload generator so it finishes in a reasonable time and
models many simultaneous customers.

Usage:
    python experiments/large_scale_workload.py --n 100000 --concurrency 50 --tag large_scale
    python experiments/large_scale_workload.py --n 100000 --concurrency 50 --tag baseline
    python experiments/large_scale_workload.py --n 100000 --concurrency 50 --tag proposed

The --tag value is written into every transaction row (run_tag column
in transactions.csv), so Power BI can filter/slice by run - handy for
the brief's required Baseline vs Proposed comparison (section 6).
"""
import argparse
import csv
import os
import statistics
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from nodes.node1_customer import run_workload_concurrent
from monitoring.performance import PerformanceMonitor
from common.config import RESULTS_DIR, USE_HTTP_GATEWAY, APACHE_HOST, APACHE_PORT
from common.http_client import get_json

os.makedirs(RESULTS_DIR, exist_ok=True)


def load_balance_cv(dist):
    vals = list(dist.values())
    if len(vals) < 2:
        return 0.0
    avg = statistics.mean(vals)
    return statistics.pstdev(vals) / avg if avg else 0.0


def main():
    parser = argparse.ArgumentParser(
        description="Run a large-scale billing workload through the real distributed pipeline."
    )
    parser.add_argument("--n", type=int, default=100_000,
                         help="Number of requests to generate (default: 100000)")
    parser.add_argument("--concurrency", type=int, default=50,
                         help="Number of simultaneous simulated customers (default: 50)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--tag", type=str, default="large_scale",
                         help="Run tag written to every transaction row, for filtering in Power BI")
    args = parser.parse_args()

    print("=" * 74)
    print(f"LARGE-SCALE WORKLOAD - {args.n:,} requests, concurrency={args.concurrency}")
    print("=" * 74)
    print("Transport:", "HTTP (via Apache)" if USE_HTTP_GATEWAY else "TCP (direct)")
    print(f"Run tag  : {args.tag}")
    print()

    results, total_time = run_workload_concurrent(
        number_of_requests=args.n,
        seed=args.seed,
        concurrency=args.concurrency,
        run_tag=args.tag,
    )
    m = PerformanceMonitor().calculate_metrics(results, total_time)
    m["load_balance_cv"] = load_balance_cv(m["node_distribution"])
    m["transport"] = "http" if USE_HTTP_GATEWAY else "tcp"

    print()
    print(f"  Total time : {total_time:.2f} s")
    print(f"  Throughput : {m['throughput_requests_per_second']:.2f} req/s")
    print(f"  Avg latency: {m['average_latency_ms']:.3f} ms")
    print(f"  Jitter     : {m['average_jitter_ms']:.3f} ms")
    print(f"  Loss       : {m['application_request_loss_percent']:.2f} %")
    print(f"  Balance CV : {m['load_balance_cv']:.4f}")
    print(f"  Node dist  : {m['node_distribution']}")

    path = os.path.join(RESULTS_DIR, "large_scale_results.csv")
    fresh = not os.path.exists(path)
    row = {
        "run_tag": args.tag,
        "transport": m["transport"],
        "requested": args.n,
        "concurrency": args.concurrency,
        "total_requests": m["total_requests"],
        "successful": m["successful_requests"],
        "failed": m["failed_requests"],
        "total_time_s": round(total_time, 3),
        "throughput": round(m["throughput_requests_per_second"], 3),
        "avg_latency_ms": round(m["average_latency_ms"], 4),
        "avg_jitter_ms": round(m["average_jitter_ms"], 4),
        "request_loss_pct": round(m["application_request_loss_percent"], 3),
        "load_balance_cv": round(m["load_balance_cv"], 4),
    }
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if fresh:
            w.writeheader()
        w.writerow(row)
    print(f"\nSaved summary to {path}")

    # The billing API writes transactions.csv/node_stats.csv incrementally
    # and only rewrites node_stats.csv every 200 transactions for
    # performance - force a final flush so Power BI sees everything.
    try:
        if USE_HTTP_GATEWAY:
            get_json(f"http://{APACHE_HOST}:{APACHE_PORT}/api/flush", timeout=5.0)
            print("Flushed transactions.csv / node_stats.csv via billing API.")
        else:
            print("TCP mode: MetricsLogger only runs inside billing_api.py, "
                  "so nothing to flush over HTTP. Hit /flush on the API directly "
                  "if it's running, or check its stdout for the atexit flush.")
    except Exception as e:
        print(f"Could not confirm flush ({e}); data will still land on the next periodic flush.")


if __name__ == "__main__":
    main()
