"""Failure-aware dispatcher for NODE-2 (Week 7 proposed system, Week 8 pool).

Compared with processing.dispatcher.Dispatcher (the baseline):

  * it asks the failure detector which nodes are UP and prefers them;
  * a failed attempt is retried at once on another node (failover) instead
    of being returned to the client as an error;
  * a per-attempt timeout (default 1.5 s) bounds how long an omission failure
    can stall a request, instead of the global REQUEST_TIMEOUT;
  * process_verified() sends the same request to several nodes and accepts the
    majority answer, which is the only defence here against a Byzantine node
    that returns plausible but wrong charges;
  * two placement policies: round_robin (baseline pool) and least_loaded
    (fewest requests in flight), used by the Week 8 processor-pool model.
"""
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from common.communication import send_json
from common.config import (ACTIVE_RATING_NODES, FT_ATTEMPT_TIMEOUT, FT_MAX_ATTEMPTS,
                           FT_POLICY, RATING_NODES)


def majority_charge(charges, tolerance=1e-6):
    """Return (winning_charge, indices_that_agree) or (None, []) without a majority."""
    if not charges:
        return None, []
    buckets = Counter(round(c / tolerance) for c in charges)
    key, votes = buckets.most_common(1)[0]
    if votes * 2 <= len(charges):
        return None, []
    agree = [i for i, c in enumerate(charges) if round(c / tolerance) == key]
    return charges[agree[0]], agree


class ResilientDispatcher:
    def __init__(self, detector, attempt_timeout=None, max_attempts=None, policy=None):
        self.detector = detector
        self.names = list(ACTIVE_RATING_NODES)
        self.attempt_timeout = attempt_timeout or FT_ATTEMPT_TIMEOUT
        self.max_attempts = max_attempts or FT_MAX_ATTEMPTS
        self.policy = policy or FT_POLICY
        self._lock = threading.Lock()
        self._rr = 0
        self.inflight = {n: 0 for n in self.names}
        self.stats = {n: {"requests": 0, "failures": 0, "failovers_in": 0} for n in self.names}

    # -- node ordering ------------------------------------------------------
    def order(self, policy=None):
        policy = policy or self.policy
        ranked = self.detector.up_nodes(self.names)
        healthy = [n for n in ranked if self.detector.status(n) == "UP"]
        rest = [n for n in ranked if n not in healthy]
        with self._lock:
            if policy == "least_loaded":
                healthy.sort(key=lambda n: (self.inflight[n], n))
            else:
                if healthy:
                    shift = self._rr % len(healthy)
                    healthy = healthy[shift:] + healthy[:shift]
                self._rr += 1
        return healthy + rest

    def first_up(self, exclude=()):
        """Deterministic primary-with-failover choice (used for 2PC rating node)."""
        for name in self.detector.up_nodes([n for n in self.names if n not in exclude]):
            return name
        return None

    # -- single-node call ---------------------------------------------------
    def _call(self, name, request, timeout):
        host, port = RATING_NODES[name]
        with self._lock:
            self.inflight[name] += 1
        try:
            reply = send_json(host, port, request, timeout=timeout)
        finally:
            with self._lock:
                self.inflight[name] -= 1
        if reply.get("error"):
            raise RuntimeError(reply["error"])
        return reply

    # -- normal request with failover --------------------------------------
    def process_request(self, request, policy=None):
        started = time.perf_counter()
        errors = []
        for attempt, name in enumerate(self.order(policy)[: self.max_attempts], start=1):
            try:
                reply = self._call(name, request, self.attempt_timeout + float(request.get("work_ms", 0)) / 1000.0 * 8)
                self.detector.report_success(name)
                with self._lock:
                    self.stats[name]["requests"] += 1
                    if attempt > 1:
                        self.stats[name]["failovers_in"] += 1
                reply["selected_node"] = name
                reply["failover_attempts"] = attempt - 1
                reply["dispatcher_latency"] = time.perf_counter() - started
                return reply
            except Exception as exc:
                self.detector.report_failure(name, f"{type(exc).__name__}: {exc}")
                with self._lock:
                    self.stats[name]["failures"] += 1
                errors.append(f"{name}: {exc}")
        return {"request_id": request.get("request_id"), "error": "all attempts failed: " + "; ".join(errors),
                "failover_attempts": len(errors), "selected_node": errors[-1].split(":")[0] if errors else None}

    # -- Byzantine defence: majority vote -----------------------------------
    def process_verified(self, request, replicas=3):
        started = time.perf_counter()
        chosen = [n for n in self.order()][:replicas]
        timeout = self.attempt_timeout + float(request.get("work_ms", 0)) / 1000.0 * 8

        def ask(name):
            try:
                return name, self._call(name, request, timeout), None
            except Exception as exc:
                self.detector.report_failure(name, f"{type(exc).__name__}: {exc}")
                return name, None, str(exc)

        with ThreadPoolExecutor(max_workers=len(chosen)) as pool:
            answers = list(pool.map(ask, chosen))
        good = [(n, r) for n, r, e in answers if r is not None]
        if len(good) < 2:
            return {"request_id": request.get("request_id"),
                    "error": f"verification needs 2+ answers, got {len(good)}", "replicas_queried": len(chosen)}
        winner, agree = majority_charge([r["charge"] for _, r in good])
        if winner is None:
            return {"request_id": request.get("request_id"), "error": "no majority among replicas",
                    "replicas_queried": len(chosen),
                    "charges": {n: r["charge"] for n, r in good}}
        suspects = [good[i][0] for i in range(len(good)) if i not in agree]
        for name in suspects:
            self.detector.report_failure(name, "byzantine deviation: charge differs from majority")
        reply = dict(good[agree[0]][1])
        reply.update(verified=True, replicas_queried=len(chosen), byzantine_suspects=suspects,
                     dispatcher_latency=time.perf_counter() - started)
        return reply

    def snapshot(self):
        with self._lock:
            return {"policy": self.policy, "attempt_timeout_s": self.attempt_timeout,
                    "max_attempts": self.max_attempts, "inflight": dict(self.inflight),
                    "stats": {n: dict(v) for n, v in self.stats.items()}}
