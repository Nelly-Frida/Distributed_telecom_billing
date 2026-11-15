"""Controllable fault injection for every node process (Week 7).

A node owns one FaultState. The experiment framework switches faults on and
off by sending control messages over the node's normal TCP port, so the same
mechanism works for local processes and for Docker containers (no access to
the Docker socket or to process IDs is needed).

Supported fault modes and the failure class each one models:

    crash      node failure      the process exits immediately (os._exit), with
                                 no clean-up, like a power loss / SIGKILL
    drop       communication     the connection is closed without a reply
    omission   omission          the request is accepted and then ignored; the
                                 caller only finds out through its timeout
    error      service failure   the node answers with an error response
    delay      performance       the node answers late (delay_ms)
    byzantine  arbitrary         the node answers with a plausible but wrong
                                 charge; nothing in the reply reveals it

Control messages (never affected by an injected fault, so a node can always
be repaired remotely):  FAULT_INJECT, FAULT_CLEAR, FAULT_STATUS.
"""
import os
import random
import threading
import time

CONTROL_TYPES = {"FAULT_INJECT", "FAULT_CLEAR", "FAULT_STATUS"}
MODES = ("crash", "drop", "omission", "error", "delay", "byzantine")


class FaultState:
    def __init__(self, node_id, seed=None):
        self.node_id = node_id
        self._lock = threading.Lock()
        self._rng = random.Random(seed)
        self.injected_total = 0
        self._reset_locked()

    # -- internal -----------------------------------------------------------
    def _reset_locked(self):
        self.mode = "none"
        self.params = {}
        self.probability = 1.0
        self.started_at = None
        self.expires_at = None

    def _expire_locked(self):
        if self.mode != "none" and self.expires_at is not None and time.time() >= self.expires_at:
            self._reset_locked()

    @staticmethod
    def _die():
        # os._exit skips atexit handlers, flushes and finally blocks: this is
        # the closest in-process equivalent of a hard crash.
        os._exit(137)

    # -- control plane ------------------------------------------------------
    def is_control(self, request):
        return request.get("type") in CONTROL_TYPES

    def control(self, request):
        kind = request.get("type")
        if kind == "FAULT_INJECT":
            mode = request.get("mode")
            if mode not in MODES:
                return {"ok": False, "node": self.node_id,
                        "error": f"unknown fault mode {mode!r}; use one of {MODES}"}
            duration = request.get("duration_s")
            now = time.time()
            with self._lock:
                self.mode = mode
                self.params = {k: v for k, v in request.items()
                               if k not in ("type", "mode", "duration_s", "probability")}
                self.probability = float(request.get("probability", 1.0))
                self.started_at = now
                self.expires_at = (now + float(duration)) if duration else None
            if mode == "crash":
                # Small delay so this control reply reaches the caller first.
                threading.Timer(float(request.get("crash_delay_s", 0.2)), self._die).start()
            return {"ok": True, "node": self.node_id, "mode": mode, "injected_at": now}
        if kind == "FAULT_CLEAR":
            with self._lock:
                previous = self.mode
                self._reset_locked()
            return {"ok": True, "node": self.node_id, "cleared": previous, "cleared_at": time.time()}
        with self._lock:
            self._expire_locked()
            return {"ok": True, "node": self.node_id, "mode": self.mode,
                    "params": dict(self.params), "injected_total": self.injected_total,
                    "started_at": self.started_at, "expires_at": self.expires_at}

    # -- data plane ---------------------------------------------------------
    def intercept(self, request):
        """Called before a request is processed.

        Returns None (process normally) or an action tuple:
            ("drop",)            close the connection, send nothing
            ("omit", hold_s)     keep the connection open, send nothing
            ("error", message)   reply with an error
        """
        with self._lock:
            self._expire_locked()
            mode, params = self.mode, dict(self.params)
            if mode == "none" or mode == "crash" or mode == "byzantine":
                return None
            if self.probability < 1.0 and self._rng.random() > self.probability:
                return None
            self.injected_total += 1
        if mode == "drop":
            return ("drop",)
        if mode == "omission":
            return ("omit", float(params.get("hold_s", 10.0)))
        if mode == "error":
            return ("error", params.get("message", "injected service failure"))
        if mode == "delay":
            time.sleep(float(params.get("delay_ms", 500)) / 1000.0)
        return None

    def corrupt(self, response):
        """Byzantine mode: silently falsify the charge in a reply."""
        with self._lock:
            self._expire_locked()
            if self.mode != "byzantine":
                return response
            if self.probability < 1.0 and self._rng.random() > self.probability:
                return response
            factor = float(self.params.get("factor", 1.25))
            self.injected_total += 1
        if isinstance(response, dict) and isinstance(response.get("charge"), (int, float)):
            response["charge"] = response["charge"] * factor
        return response
