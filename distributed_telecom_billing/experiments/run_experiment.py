import csv
import os
import statistics
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from nodes.node1_customer import run_workload
from monitoring.performance import PerformanceMonitor

RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

def calculate_load_balance(distribution):
    values = list(distribution.values())
    if len(values) < 2:
        return 0

    average = statistics.mean(values)
    if average == 0:
        return 0

    return statistics.pstdev(values) / average

def run_experiment(number_of_requests=1000, seed=42):
    print()
    print("=" * 60)
    print("DISTRIBUTED TELECOM BILLING EXPERIMENT")
    print("=" * 60)
    print(f"Requests: {number_of_requests}")
    print(f"Random seed: {seed}")

    results, total_time = run_workload(number_of_requests, seed)

    monitor = PerformanceMonitor()
    metrics = monitor.calculate_metrics(results, total_time)

    distribution = metrics["node_distribution"]
    metrics["load_balance_cv"] = calculate_load_balance(distribution)

    print("\nRESULTS")
    print("-" * 60)
    print(f"Requests: {metrics['total_requests']}")
    print(f"Successful: {metrics['successful_requests']}")
    print(f"Failed: {metrics['failed_requests']}")
    print(f"Throughput: {metrics['throughput_requests_per_second']:.2f} requests/s")
    print(f"Average latency: {metrics['average_latency_ms']:.3f} ms")
    print(f"Average jitter: {metrics['average_jitter_ms']:.3f} ms")
    print(f"Request loss: {metrics['application_request_loss_percent']:.2f}%")
    print(f"CPU: {metrics['cpu_percent']:.2f}%")
    print(f"Memory: {metrics['memory_mb']:.2f} MB")

    print("\nLOAD DISTRIBUTION")
    print("-" * 60)

    for node, count in distribution.items():
        percentage = count / metrics["successful_requests"] * 100
        print(f"{node}: {count} requests ({percentage:.2f}%)")

    print(f"\nLoad balance coefficient of variation: {metrics['load_balance_cv']:.4f}")
    return metrics, results

def save_results(metrics):
    filename = os.path.join(RESULTS_DIR, "experiment_results.csv")

    row = {
        "total_requests": metrics["total_requests"],
        "successful_requests": metrics["successful_requests"],
        "failed_requests": metrics["failed_requests"],
        "throughput": metrics["throughput_requests_per_second"],
        "average_latency_ms": metrics["average_latency_ms"],
        "average_jitter_ms": metrics["average_jitter_ms"],
        "request_loss_percent": metrics["application_request_loss_percent"],
        "cpu_percent": metrics["cpu_percent"],
        "memory_mb": metrics["memory_mb"],
        "load_balance_cv": metrics["load_balance_cv"],
        "node_distribution": str(metrics["node_distribution"])
    }

    file_exists = os.path.exists(filename)

    with open(filename, "a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    print(f"\nResults saved to {filename}")

if __name__ == "__main__":
    metrics, results = run_experiment(number_of_requests=1000, seed=42)
    save_results(metrics)
