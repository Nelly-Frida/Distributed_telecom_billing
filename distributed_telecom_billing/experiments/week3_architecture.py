import csv, os, statistics, sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from nodes.node1_customer import run_workload
from monitoring.performance import PerformanceMonitor
from common.config import RESULTS_DIR, USE_HTTP_GATEWAY

os.makedirs(RESULTS_DIR, exist_ok=True)


def load_balance_cv(dist):
    vals = list(dist.values())
    if len(vals) < 2:
        return 0.0
    avg = statistics.mean(vals)
    return statistics.pstdev(vals) / avg if avg else 0.0


def run_once(n=1000, seed=42):
    results, total = run_workload(n, seed)
    m = PerformanceMonitor().calculate_metrics(results, total)
    m["load_balance_cv"] = load_balance_cv(m["node_distribution"])
    m["transport"] = "http" if USE_HTTP_GATEWAY else "tcp"
    return m


def main():
    print("=" * 74)
    print("WEEK 3 — MULTI-TIER ARCHITECTURE")
    print("=" * 74)
    print("Transport:", "HTTP (via Apache)" if USE_HTTP_GATEWAY else "TCP (direct)")
    m = run_once()
    print(f"  Throughput : {m['throughput_requests_per_second']:.2f} req/s")
    print(f"  Avg latency: {m['average_latency_ms']:.3f} ms")
    print(f"  Jitter     : {m['average_jitter_ms']:.3f} ms")
    print(f"  Loss       : {m['application_request_loss_percent']:.2f} %")
    print(f"  Balance CV : {m['load_balance_cv']:.4f}")

    path = os.path.join(RESULTS_DIR, "week3_results.csv")
    fresh = not os.path.exists(path)
    row = {
        "transport": m["transport"],
        "total_requests": m["total_requests"],
        "successful": m["successful_requests"],
        "failed": m["failed_requests"],
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
    print(f"Saved to {path}")


if __name__ == "__main__":
    main()
