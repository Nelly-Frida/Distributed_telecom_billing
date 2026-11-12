import statistics, psutil


class PerformanceMonitor:
    def __init__(self):
        self.process = psutil.Process()

    def calculate_metrics(self, results, total_time):
        successful = [r for r in results if r.get("success") is True]
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
            "successful_requests": len(successful),
            "failed_requests": len(failed),
            "total_time_seconds": total_time,
            "throughput_requests_per_second": throughput,
            "average_latency_ms": avg,
            "minimum_latency_ms": mn,
            "maximum_latency_ms": mx,
            "average_jitter_ms": jitter,
            "application_request_loss_percent": loss,
            "cpu_percent": cpu,
            "memory_mb": mem,
            "node_distribution": dist,
        }
