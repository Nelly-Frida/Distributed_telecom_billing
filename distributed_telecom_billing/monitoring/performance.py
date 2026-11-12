import statistics
import psutil

class PerformanceMonitor:
    def __init__(self):
        self.process = psutil.Process()

    def calculate_metrics(self, results, total_time):
        successful = [r for r in results if r.get("success") is True]
        failed = [r for r in results if r.get("success") is not True]

        latencies_ms = [
            r["end_to_end_latency"] * 1000
            for r in successful
        ]

        if latencies_ms:
            average_latency = statistics.mean(latencies_ms)
            minimum_latency = min(latencies_ms)
            maximum_latency = max(latencies_ms)
        else:
            average_latency = minimum_latency = maximum_latency = 0

        jitter_values = [
            abs(latencies_ms[i] - latencies_ms[i - 1])
            for i in range(1, len(latencies_ms))
        ]

        average_jitter = statistics.mean(jitter_values) if jitter_values else 0
        throughput = len(successful) / total_time if total_time > 0 else 0

        total_requests = len(results)
        request_loss = (len(failed) / total_requests * 100) if total_requests else 0

        cpu_percent = self.process.cpu_percent(interval=0.1)
        memory_mb = self.process.memory_info().rss / (1024 * 1024)

        node_distribution = {}
        for result in successful:
            node = result.get("selected_node", "UNKNOWN")
            node_distribution[node] = node_distribution.get(node, 0) + 1

        return {
            "total_requests": total_requests,
            "successful_requests": len(successful),
            "failed_requests": len(failed),
            "total_time_seconds": total_time,
            "throughput_requests_per_second": throughput,
            "average_latency_ms": average_latency,
            "minimum_latency_ms": minimum_latency,
            "maximum_latency_ms": maximum_latency,
            "average_jitter_ms": average_jitter,
            "application_request_loss_percent": request_loss,
            "cpu_percent": cpu_percent,
            "memory_mb": memory_mb,
            "node_distribution": node_distribution
        }
