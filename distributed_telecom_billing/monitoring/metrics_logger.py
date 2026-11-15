import atexit
import csv
import os
import threading
import time
from common.config import DASHBOARD_DIR

TX_HEADER = ["timestamp", "request_id", "customer_id", "service",
             "usage", "charge", "node", "latency_ms", "run_tag"]
NODE_HEADER = ["node", "requests", "failures", "last_ts"]


class MetricsLogger:
    def __init__(self, out_dir=DASHBOARD_DIR, node_stats_flush_every=200):
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)
        self._lock = threading.Lock()
        self.tx_path = os.path.join(out_dir, "transactions.csv")
        self.node_path = os.path.join(out_dir, "node_stats.csv")
        self._ensure_headers()

        # Kept open for the life of the process instead of opening and
        # closing the file on every single transaction. At 100,000+
        # transactions that per-row open/close was itself a bottleneck.
        self._tx_file = open(self.tx_path, "a", newline="", encoding="utf-8")
        self._tx_writer = csv.writer(self._tx_file)

        self.node_counters = {}
        self._node_stats_flush_every = node_stats_flush_every
        self._since_flush = 0

        atexit.register(self.flush)

    def _ensure_headers(self):
        # node_stats.csv is small (one row per node) so it's safe to
        # rewrite in full; transactions.csv is the one that scales with
        # workload size, so it's append-only.
        if os.path.exists(self.tx_path):
            with open(self.tx_path, "r", newline="", encoding="utf-8") as f:
                first_line = f.readline().strip()
            if first_line != ",".join(TX_HEADER):
                # Older run predates the run_tag column - back it up
                # rather than silently mixing row shapes in one file.
                backup = self.tx_path + ".legacy_backup.csv"
                os.replace(self.tx_path, backup)
        if not os.path.exists(self.tx_path):
            with open(self.tx_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(TX_HEADER)
        if not os.path.exists(self.node_path):
            with open(self.node_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(NODE_HEADER)

    def record_transaction(self, request, response):
        with self._lock:
            node = response.get("selected_node", "UNKNOWN")
            self._tx_writer.writerow([
                round(time.time(), 3),
                request.get("request_id"),
                request.get("customer_id"),
                request.get("service"),
                round(float(request.get("usage", 0.0)), 3),
                round(float(response.get("charge", 0.0)), 4),
                node,
                round(float(response.get("api_latency_ms", 0.0)), 3),
                request.get("run_tag", ""),
            ])

            c = self.node_counters.setdefault(node, {"requests": 0, "failures": 0})
            if "error" in response:
                c["failures"] += 1
            else:
                c["requests"] += 1

            # node_stats.csv used to be rewritten from scratch on every
            # single transaction - fine at n=1000, needlessly expensive
            # at n=100000. Batching this to every N transactions (plus a
            # final flush) keeps Power BI's node_stats view fresh enough
            # without doing 100,000 full-file rewrites.
            self._since_flush += 1
            if self._since_flush >= self._node_stats_flush_every:
                self._flush_locked()
                self._since_flush = 0

    def _flush_locked(self):
        with open(self.node_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(NODE_HEADER)
            for n, c in self.node_counters.items():
                w.writerow([n, c["requests"], c["failures"], round(time.time(), 3)])
        self._tx_file.flush()

    def flush(self):
        with self._lock:
            self._flush_locked()

    def snapshot(self):
        return {"nodes": self.node_counters, "transactions_file": self.tx_path}

    def export_all(self):
        self.flush()
        return {"transactions": self.tx_path, "node_stats": self.node_path}
