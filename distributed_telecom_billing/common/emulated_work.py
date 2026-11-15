"""Emulated service demand with a fixed number of service slots per node.

Rating a call record is almost free on a modern CPU, so every system model
would look identical. To compare models fairly and reproducibly, a request may
carry `work_ms`: the service time a real rating/mediation step would need. A
node serves at most `slots` requests at the same time (like a node with that
many cores or workers); the rest queue. Busy time is accounted per node so the
experiments can report real queueing delay and utilisation instead of guesses.

work_mode "sleep" (default) models I/O-bound or remote-database work and does
not depend on how many cores the test machine has. work_mode "cpu" busy-waits
and therefore depends on the hardware.
"""
import os
import threading
import time


class ServiceCapacity:
    def __init__(self, slots=None):
        self.slots = max(1, int(slots or os.getenv("NODE_SERVICE_SLOTS", "1")))
        self._sem = threading.BoundedSemaphore(self.slots)
        self._lock = threading.Lock()
        self._reset_locked()

    def _reset_locked(self):
        self.started = time.time()
        self.busy_s = 0.0
        self.completed = 0
        self.waiting = 0
        self.total_wait_ms = 0.0
        self.max_wait_ms = 0.0

    def execute(self, work_ms, mode="sleep"):
        """Run the emulated work; return the queueing delay in milliseconds."""
        if not work_ms or work_ms <= 0:
            return 0.0
        requested = time.perf_counter()
        with self._lock:
            self.waiting += 1
        self._sem.acquire()
        wait_ms = (time.perf_counter() - requested) * 1000.0
        with self._lock:
            self.waiting -= 1
        began = time.perf_counter()
        try:
            if mode == "cpu":
                end = began + work_ms / 1000.0
                while time.perf_counter() < end:
                    pass
            else:
                time.sleep(work_ms / 1000.0)
        finally:
            elapsed = time.perf_counter() - began
            self._sem.release()
            with self._lock:
                self.busy_s += elapsed
                self.completed += 1
                self.total_wait_ms += wait_ms
                self.max_wait_ms = max(self.max_wait_ms, wait_ms)
        return wait_ms

    def snapshot(self, reset=False):
        with self._lock:
            wall = max(1e-9, time.time() - self.started)
            data = {
                "slots": self.slots,
                "window_s": round(wall, 4),
                "busy_s": round(self.busy_s, 4),
                "utilization": round(min(1.0, self.busy_s / (self.slots * wall)), 4),
                "completed": self.completed,
                "waiting_now": self.waiting,
                "mean_wait_ms": round(self.total_wait_ms / self.completed, 3) if self.completed else 0.0,
                "max_wait_ms": round(self.max_wait_ms, 3),
            }
            if reset:
                self._reset_locked()
        return data
