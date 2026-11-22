"""Heartbeat failure detector for NODE-2 (Week 7).

Every `interval` seconds each target receives a PROBE. States:

    UP        answering probes
    SUSPECT   one missed probe, or a request to it just failed (passive report)
    DOWN      `miss_threshold` consecutive missed probes (failure declared)

A single successful probe returns a node to UP, so a wrongly suspected node is
restored within one probe interval. All state changes are written with an
epoch timestamp so detection time and recovery time can be computed against
the moment a fault was injected.

Detection time  ~ interval * miss_threshold + probe timeout (default ~2 s).
Lower thresholds detect sooner but raise the false-positive rate.
"""
import csv
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from common.communication import send_json

UP, SUSPECT, DOWN = "UP", "SUSPECT", "DOWN"


class FailureDetector(threading.Thread):
    def __init__(self, targets, interval=0.5, timeout=0.5, miss_threshold=3,
                 csv_path=None, on_recovered=None):
        super().__init__(daemon=True)
        self.targets = dict(targets)
        self.interval = interval
        self.timeout = timeout
        self.miss_threshold = max(1, miss_threshold)
        self.on_recovered = on_recovered
        self.csv_path = csv_path
        self.running = True
        self._lock = threading.Lock()
        now = time.time()
        self.state = {n: {"state": UP, "misses": 0, "last_ok": now, "since": now}
                      for n in self.targets}
        self.events = []
        if self.csv_path:
            os.makedirs(os.path.dirname(os.path.abspath(self.csv_path)), exist_ok=True)
            if not os.path.exists(self.csv_path):
                with open(self.csv_path, "w", newline="") as f:
                    csv.writer(f).writerow(["timestamp", "node", "event", "detail"])

    # -- queries ------------------------------------------------------------
    def status(self, name):
        with self._lock:
            return self.state[name]["state"] if name in self.state else UP

    def up_nodes(self, names):
        """Names ordered UP first, then SUSPECT, then DOWN (last resort)."""
        with self._lock:
            rank = {UP: 0, SUSPECT: 1, DOWN: 2}
            return sorted(names, key=lambda n: rank.get(self.state.get(n, {"state": UP})["state"], 0))

    def snapshot(self, last_events=200):
        with self._lock:
            return {
                "interval_s": self.interval, "probe_timeout_s": self.timeout,
                "miss_threshold": self.miss_threshold,
                "nodes": {n: dict(v) for n, v in self.state.items()},
                "events": list(self.events[-last_events:]),
                "server_time": time.time(),
            }

    # -- state transitions --------------------------------------------------
    def _event(self, node, event, detail=""):
        row = {"timestamp": round(time.time(), 4), "node": node, "event": event, "detail": detail}
        self.events.append(row)
        if len(self.events) > 2000:
            del self.events[:500]
        if self.csv_path:
            try:
                with open(self.csv_path, "a", newline="") as f:
                    csv.writer(f).writerow([row["timestamp"], node, event, detail])
            except OSError:
                pass

    def report_failure(self, name, detail="request failed"):
        """Passive detection: a real request to `name` just failed."""
        with self._lock:
            entry = self.state.get(name)
            if entry and entry["state"] == UP:
                entry["state"], entry["since"] = SUSPECT, time.time()
                self._event(name, "SUSPECTED_PASSIVE", detail)

    def report_success(self, name):
        with self._lock:
            entry = self.state.get(name)
            if entry:
                entry["last_ok"] = time.time()

    def _apply_probe(self, name, ok, detail=""):
        recovered = False
        with self._lock:
            entry = self.state[name]
            if ok:
                if entry["state"] != UP:
                    self._event(name, "RECOVERED", f"was {entry['state']} for "
                                f"{round(time.time() - entry['since'], 3)}s")
                    recovered = True
                entry.update(state=UP, misses=0, last_ok=time.time(), since=entry["since"] if entry["state"] == UP else time.time())
            else:
                entry["misses"] += 1
                if entry["misses"] >= self.miss_threshold and entry["state"] != DOWN:
                    entry["state"], entry["since"] = DOWN, time.time()
                    self._event(name, "DOWN", f"{entry['misses']} missed probes: {detail}")
                elif entry["state"] == UP:
                    entry["state"], entry["since"] = SUSPECT, time.time()
                    self._event(name, "SUSPECTED", detail)
        if recovered and self.on_recovered:
            threading.Thread(target=self.on_recovered, args=(name,), daemon=True).start()

    def _probe(self, name):
        host, port = self.targets[name]
        try:
            reply = send_json(host, port, {"type": "PROBE", "from_id": "NODE-2-DETECTOR"},
                              timeout=self.timeout)
            if reply.get("error"):
                raise RuntimeError(reply["error"])
            self._apply_probe(name, True)
        except Exception as exc:
            self._apply_probe(name, False, f"{type(exc).__name__}: {exc}")

    def run(self):
        if not self.targets:
            return
        with ThreadPoolExecutor(max_workers=len(self.targets)) as pool:
            while self.running:
                started = time.time()
                list(pool.map(self._probe, list(self.targets)))
                time.sleep(max(0.0, self.interval - (time.time() - started)))

    def stop(self):
        self.running = False
