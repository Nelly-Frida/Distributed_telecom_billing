"""Process runtime hosted by every rating node (Week 10).

A managed process is a long-running billing task hosted inside the node: it rates a deterministic
batch of call records, one record at a time, and consumes the node's emulated service capacity, so
tasks and ordinary billing requests compete for the same node just as they would on a real machine.

    PROC_START  proc_id, spec          start (or resume from a checkpoint)
    PROC_STATUS proc_id                state, progress and checkpoint (offset + partial totals)
    PROC_STOP   proc_id                cooperative termination, returns the final checkpoint
    PROC_LIST / PROC_REAP

States: RUNNING -> DONE | TERMINATED | FAILED
"""
import random
import threading
import time

from rating.rating_engine import RatingEngine

SERVICES = ("voice", "sms", "data")
_ENGINE = RatingEngine()


def record_at(seed, index):
    """Deterministic call record number `index` of a batch: the same on every node, every time."""
    rng = random.Random(f"{seed}:{index}")
    service = rng.choice(SERVICES)
    return service, rng.randint(1, 50)


def expected_batch(seed, start, count):
    """Ground truth used to verify exactly-once results after failures and migrations."""
    charge = usage = 0.0
    for i in range(start, start + count):
        service, units = record_at(seed, i)
        charge += _ENGINE.rates[service] * units
        usage += units
    return {"charge": round(charge, 6), "usage": round(usage, 6)}


class ProcessRuntime:
    def __init__(self, node_id, capacity=None):
        self.node_id = node_id
        self.capacity = capacity
        self._lock = threading.Lock()
        self.jobs = {}

    def has(self, proc_id):
        with self._lock:
            return proc_id in self.jobs

    def _run(self, proc_id):
        job = self.jobs[proc_id]
        spec = job["spec"]
        try:
            while job["offset"] < spec["count"]:
                if job["stop"].is_set():
                    job["state"] = "TERMINATED"
                    return
                index = spec["start"] + job["offset"]
                service, units = record_at(spec["seed"], index)
                if self.capacity is not None:
                    self.capacity.execute(spec.get("work_ms", 1.0))
                else:
                    time.sleep(spec.get("work_ms", 1.0) / 1000.0)
                charge = _ENGINE.rates[service] * units
                with self._lock:
                    job["charge"] += charge
                    job["usage"] += units
                    job["offset"] += 1
            job["state"] = "DONE"
        except Exception as exc:
            job["state"] = "FAILED"
            job["error"] = str(exc)
        finally:
            job["finished"] = time.time()

    def start(self, proc_id, spec):
        with self._lock:
            existing = self.jobs.get(proc_id)
            if existing and existing["state"] == "RUNNING":
                return {"started": False, "error": "already running", "proc_id": proc_id}
            job = {"spec": dict(spec), "state": "RUNNING", "stop": threading.Event(),
                   "offset": int(spec.get("resume_offset", 0)), "charge": float(spec.get("resume_charge", 0.0)),
                   "usage": float(spec.get("resume_usage", 0.0)), "started": time.time(), "finished": None, "error": None}
            self.jobs[proc_id] = job
        threading.Thread(target=self._run, args=(proc_id,), daemon=True).start()
        return {"started": True, "proc_id": proc_id, "resumed_from": job["offset"]}

    def status(self, proc_id):
        with self._lock:
            job = self.jobs.get(proc_id)
            if not job:
                return {"state": "UNKNOWN", "proc_id": proc_id}
            return {"proc_id": proc_id, "state": job["state"], "offset": job["offset"], "count": job["spec"]["count"],
                    "charge": round(job["charge"], 6), "usage": round(job["usage"], 6), "error": job["error"],
                    "node": self.node_id, "runtime_s": round((job["finished"] or time.time()) - job["started"], 3)}

    def stop(self, proc_id):
        with self._lock:
            job = self.jobs.get(proc_id)
        if not job:
            return {"state": "UNKNOWN", "proc_id": proc_id}
        job["stop"].set()
        deadline = time.time() + 5
        while job["state"] == "RUNNING" and time.time() < deadline:
            time.sleep(0.01)
        return self.status(proc_id)

    def list(self):
        with self._lock:
            return {"processes": {p: j["state"] for p, j in self.jobs.items()}}

    def reap(self, proc_id):
        with self._lock:
            job = self.jobs.get(proc_id)
            if job and job["state"] != "RUNNING":
                del self.jobs[proc_id]
                return {"reaped": True}
        return {"reaped": False}
