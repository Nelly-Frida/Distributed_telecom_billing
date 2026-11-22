<<<<<<< HEAD
import statistics
import psutil
=======
import statistics, psutil

>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a

class PerformanceMonitor:
    def __init__(self):
        self.process = psutil.Process()

    def calculate_metrics(self, results, total_time):
        successful = [r for r in results if r.get("success") is True]
<<<<<<< HEAD
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
=======
        failed     = [r for r in results if r.get("success") is not True]
        latencies_ms = [r["end_to_end_latency"] * 1000 for r in successful]

        if latencies_ms:
            avg = statistics.mean(latencies_ms)
            mn  = min(latencies_ms)
            mx  = max(latencies_ms)
        else:
            avg = mn = mx = 0

        jitter_vals = [abs(latencies_ms[i] - latencies_ms[i-1])
                       for i in range(1, len(latencies_ms))]
        jitter = statistics.mean(jitter_vals) if jitter_vals else 0
        throughput = len(successful)/total_time if total_time else 0
        total = len(results)
        loss = (len(failed)/total*100) if total else 0
        cpu = self.process.cpu_percent(interval=0.1)
        mem = self.process.memory_info().rss / (1024*1024)

        dist = {}
        for r in successful:
            n = r.get("selected_node", "UNKNOWN")
            dist[n] = dist.get(n, 0) + 1

        return {
            "total_requests": total,
>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a
            "successful_requests": len(successful),
            "failed_requests": len(failed),
            "total_time_seconds": total_time,
            "throughput_requests_per_second": throughput,
<<<<<<< HEAD
            "average_latency_ms": average_latency,
            "minimum_latency_ms": minimum_latency,
            "maximum_latency_ms": maximum_latency,
            "average_jitter_ms": average_jitter,
            "application_request_loss_percent": request_loss,
            "cpu_percent": cpu_percent,
            "memory_mb": memory_mb,
            "node_distribution": node_distribution
=======
            "average_latency_ms": avg,
            "minimum_latency_ms": mn,
            "maximum_latency_ms": mx,
            "average_jitter_ms": jitter,
            "application_request_loss_percent": loss,
            "cpu_percent": cpu,
            "memory_mb": mem,
            "node_distribution": dist,
>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a
        }
