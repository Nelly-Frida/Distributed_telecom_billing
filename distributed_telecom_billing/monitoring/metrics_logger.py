import csv, os, threading, time
from common.config import DASHBOARD_DIR


class MetricsLogger:
    def __init__(self, out_dir=DASHBOARD_DIR):
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)
        self._lock = threading.Lock()
        self.tx_path   = os.path.join(out_dir, "transactions.csv")
        self.node_path = os.path.join(out_dir, "node_stats.csv")
        self._ensure_headers()
        self.node_counters = {}

    def _ensure_headers(self):
        if not os.path.exists(self.tx_path):
            with open(self.tx_path, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "request_id", "customer_id", "service",
                    "usage", "charge", "node", "latency_ms",
                ])
        if not os.path.exists(self.node_path):
            with open(self.node_path, "w", newline="") as f:
                csv.writer(f).writerow(["node", "requests", "failures", "last_ts"])

    def record_transaction(self, request, response):
        with self._lock:
            node = response.get("selected_node", "UNKNOWN")
            with open(self.tx_path, "a", newline="") as f:
                csv.writer(f).writerow([
                    round(time.time(), 3),
                    request.get("request_id"),
                    request.get("customer_id"),
                    request.get("service"),
                    round(float(request.get("usage", 0.0)), 3),
                    round(float(response.get("charge", 0.0)), 4),
                    node,
                    round(float(response.get("api_latency_ms", 0.0)), 3),
                ])
            c = self.node_counters.setdefault(node, {"requests": 0, "failures": 0})
            if "error" in response:
                c["failures"] += 1
            else:
                c["requests"] += 1
            with open(self.node_path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["node", "requests", "failures", "last_ts"])
                for n, c in self.node_counters.items():
                    w.writerow([n, c["requests"], c["failures"],
                                round(time.time(), 3)])

    def snapshot(self):
        return {"nodes": self.node_counters, "transactions_file": self.tx_path}

    def export_all(self):
        return {"transactions": self.tx_path, "node_stats": self.node_path}
