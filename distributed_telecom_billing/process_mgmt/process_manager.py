"""Distributed process management (Week 10), hosted in NODE-2.

    Create -> Locate -> Schedule -> Coordinate -> Terminate

Create       allocate a logical name  proc.job-N  and register it in the naming service
Schedule     choose a node (round_robin | least_loaded | random), only among nodes the failure detector
             calls UP, and start the process in that node's runtime
Locate       resolve the NAME (not a stored address) to find the node currently running the process
Coordinate   groups of tasks fan out over the nodes; wait() is a barrier; reduce() gathers the results
Terminate    cooperative stop on the node, cascade to children, name removed, runtime slot reaped
Recover      a monitor polls every process; if its node dies or restarts and forgets it, the process is
             rescheduled elsewhere from its last checkpoint (offset + partial totals), so results are
             exactly-once: records after the checkpoint are re-done, records before it are not counted twice
Migrate      voluntary move: stop with a checkpoint, switch the name, resume on the new node

Managed processes are tasks hosted by the node runtimes (process_mgmt/node_runtime.py), not OS processes:
the name, scheduling, location and life-cycle logic is the part that is distributed.
"""
import csv
import itertools
import json
import os
import random
import sqlite3
import threading
import time

from common.communication import send_json
from common.config import RATING_NODES, RESULTS_DIR
from naming.resolvers import NamingAdmin, make_resolver

TERMINAL = ("DONE", "FAILED", "TERMINATED")


class ProcessManager:
    def __init__(self, detector=None, db_path=None, poll_s=None, resolver_mode="dns"):
        self.detector = detector
        self.nodes = list(RATING_NODES)
        self.admin = NamingAdmin()
        self.resolver = make_resolver(resolver_mode, ttl=0.5) if resolver_mode == "dns" else make_resolver(resolver_mode)
        self.poll_s = poll_s or float(os.getenv("PM_POLL_S", "0.25"))
        self.cond = threading.Condition()
        self.procs = {}
        self.events = []
        self._rr = itertools.count()
        self._rng = random.Random(7)
        self.db_path = db_path or os.getenv("PM_DB_PATH", "data/process_table.db")
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        self.csv_path = os.path.join(RESULTS_DIR, "week10_process_events.csv")
        os.makedirs(RESULTS_DIR, exist_ok=True)
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                csv.writer(f).writerow(["timestamp", "proc_id", "event", "detail"])
        self._init_db()
        self.running = True
        threading.Thread(target=self._monitor, daemon=True).start()

    # ------------------------------------------------------------------ persistence and events
    def _db(self):
        db = sqlite3.connect(self.db_path, timeout=15)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        return db

    def _init_db(self):
        with self._db() as db:
            db.execute("CREATE TABLE IF NOT EXISTS processes(proc_id TEXT PRIMARY KEY, data TEXT)")
            for pid, data in db.execute("SELECT proc_id, data FROM processes"):
                self.procs[pid] = json.loads(data)   # re-attach after a NODE-2 restart: the monitor resumes watching

    def _persist(self, p):
        with self._db() as db:
            db.execute("INSERT OR REPLACE INTO processes VALUES(?,?)", (p["proc_id"], json.dumps(p)))

    def _event(self, proc_id, event, detail=""):
        row = {"timestamp": round(time.time(), 4), "proc_id": proc_id, "event": event, "detail": detail}
        self.events.append(row)
        if len(self.events) > 5000:
            del self.events[:1000]
        try:
            with open(self.csv_path, "a", newline="") as f:
                csv.writer(f).writerow([row["timestamp"], proc_id, event, detail])
        except OSError:
            pass

    def _next_id(self):
        n = max([int(p.split("-")[-1]) for p in self.procs] + [0]) + 1
        return f"proc.job-{n}"

    # ------------------------------------------------------------------ node calls
    def _call(self, node, msg, timeout=1.5):
        host, port = RATING_NODES[node]
        return send_json(host, port, msg, timeout=timeout)

    # ------------------------------------------------------------------ SCHEDULE
    def _load(self, node):
        """Estimated remaining work (ms) of the running processes assigned to a node."""
        total = 0.0
        for p in self.procs.values():
            if p["state"] == "RUNNING" and p["node"] == node:
                total += max(0, p["spec"]["count"] - p["offset"]) * p["spec"].get("work_ms", 1.0)
        return total

    def pick_node(self, policy="least_loaded", exclude=()):
        candidates = [n for n in self.nodes if n not in exclude]
        up = [n for n in candidates if not self.detector or self.detector.status(n) == "UP"] or candidates
        if policy == "round_robin":
            return up[next(self._rr) % len(up)]
        if policy == "random":
            return self._rng.choice(up)
        return min(up, key=lambda n: (self._load(n), n))

    # ------------------------------------------------------------------ CREATE (+ schedule)
    def create(self, spec, parent=None, group=None, policy="least_loaded", node=None):
        t0 = time.perf_counter()
        spec = {"seed": 1, "start": 0, "count": 100, "work_ms": 2.0, **spec}
        proc_id = self._next_id()
        p = {"proc_id": proc_id, "kind": "rate_batch", "spec": spec, "node": None, "state": "CREATED", "parent": parent,
             "group": group, "created": time.time(), "started": None, "finished": None, "offset": 0, "charge": 0.0,
             "usage": 0.0, "restarts": 0, "misses": 0, "error": None, "policy": policy}
        self.procs[proc_id] = p
        tried = []
        while True:
            target = node if (node and node not in tried) else self.pick_node(policy, exclude=tried)
            try:
                reply = self._call(target, {"type": "PROC_START", "proc_id": proc_id, "spec": spec})
                if not reply.get("started"):
                    raise RuntimeError(reply.get("error", "start refused"))
            except Exception as exc:
                tried.append(target)
                self._event(proc_id, "SCHEDULE_FAILED", f"{target}: {exc}")
                if len(tried) >= len(self.nodes):
                    p["state"], p["error"] = "FAILED", "no node could start the process"
                    self._persist(p)
                    return {"proc_id": proc_id, "state": "FAILED", "error": p["error"]}
                continue
            break
        p.update(node=target, state="RUNNING", started=time.time())
        self.admin.register_one({"name": proc_id, "kind": "process", "locations": [target], "ttl": 2.0})
        self._persist(p)
        self._event(proc_id, "CREATED", f"on {target} policy={policy}")
        return {"proc_id": proc_id, "node": target, "state": "RUNNING", "policy": policy,
                "create_ms": round((time.perf_counter() - t0) * 1000, 3)}

    def create_group(self, tasks, seed=1, policy="least_loaded", work_ms=2.0):
        """tasks: list of record counts (or {"count", "work_ms"}). Contiguous slices of one deterministic batch."""
        gid = f"group-{int(time.time() * 1000) % 10**9}"
        start, ids, t0 = 0, [], time.perf_counter()
        for t in tasks:
            spec = {"seed": seed, "start": start, "count": t["count"] if isinstance(t, dict) else t,
                    "work_ms": (t.get("work_ms", work_ms) if isinstance(t, dict) else work_ms)}
            r = self.create(spec, group=gid, policy=policy)
            ids.append(r["proc_id"])
            start += spec["count"]
        return {"group": gid, "proc_ids": ids, "create_ms": round((time.perf_counter() - t0) * 1000, 3)}

    # ------------------------------------------------------------------ LOCATE
    def locate(self, proc_id):
        """Find the node by resolving the NAME; fall back to a fresh lookup if the cached one is stale."""
        t0 = time.perf_counter()
        p = self.procs.get(proc_id)
        if p and p["state"] in ("DONE", "FAILED"):   # finished: the runtime slot is gone, the table keeps the outcome
            return {"proc_id": proc_id, "node": p["node"], "state": p["state"], "final": True, "attempts": 0,
                    "locate_ms": round((time.perf_counter() - t0) * 1000, 3)}
        for attempt in (1, 2):
            rec = self.resolver.resolve(proc_id)
            if rec:
                node = rec["locations"][0]
                try:
                    st = self._call(node, {"type": "PROC_STATUS", "proc_id": proc_id}, timeout=1.0)
                    if st.get("state") != "UNKNOWN":
                        return {"proc_id": proc_id, "node": node, "state": st["state"], "attempts": attempt,
                                "locate_ms": round((time.perf_counter() - t0) * 1000, 3)}
                except Exception:
                    pass
            self.resolver.invalidate(proc_id)
        return {"proc_id": proc_id, "node": None, "error": "not found",
                "locate_ms": round((time.perf_counter() - t0) * 1000, 3)}

    def info(self, proc_id):
        p = self.procs.get(proc_id)
        if not p:
            return {"error": "unknown process"}
        return {k: p[k] for k in ("proc_id", "node", "state", "offset", "charge", "usage", "restarts", "parent", "group")} | {
            "count": p["spec"]["count"]}

    # ------------------------------------------------------------------ monitor / RECOVER
    def _reschedule(self, p, reason):
        failed = p["node"]
        detected = time.time()
        p["state"] = "RESCHEDULING"
        spec = dict(p["spec"], resume_offset=p["offset"], resume_charge=p["charge"], resume_usage=p["usage"])
        tried = [failed]
        while len(tried) <= len(self.nodes):
            target = self.pick_node(p.get("policy", "least_loaded"), exclude=tried)
            try:
                reply = self._call(target, {"type": "PROC_START", "proc_id": p["proc_id"], "spec": spec})
                if reply.get("started"):
                    p.update(node=target, state="RUNNING", misses=0)
                    p["restarts"] += 1
                    self.admin.register_one({"name": p["proc_id"], "kind": "process", "locations": [target], "ttl": 2.0})
                    self.resolver.invalidate(p["proc_id"])
                    self._persist(p)
                    self._event(p["proc_id"], "RESCHEDULED",
                                f"{failed} -> {target} ({reason}) resumed at offset {p['offset']} "
                                f"recovery_s={round(time.time() - detected, 3)}")
                    return True
            except Exception:
                pass
            tried.append(target)
        p["state"], p["error"] = "FAILED", f"could not reschedule after {reason}"
        self._persist(p)
        self._event(p["proc_id"], "RESCHEDULE_FAILED", reason)
        return False

    def _monitor(self):
        while self.running:
            time.sleep(self.poll_s)
            changed = False
            for p in list(self.procs.values()):
                if p["state"] != "RUNNING":
                    continue
                try:
                    st = self._call(p["node"], {"type": "PROC_STATUS", "proc_id": p["proc_id"]}, timeout=0.8)
                except Exception:
                    p["misses"] += 1
                    node_down = self.detector and self.detector.status(p["node"]) == "DOWN"
                    if p["misses"] >= 2 or node_down:
                        self._reschedule(p, "node unreachable")
                        changed = True
                    continue
                if st.get("state") == "UNKNOWN":      # the node restarted and lost the task
                    self._reschedule(p, "node forgot the process")
                    changed = True
                    continue
                p["misses"] = 0
                p.update(offset=st["offset"], charge=st["charge"], usage=st["usage"])
                if st["state"] in TERMINAL:
                    p.update(state=st["state"], finished=time.time(), error=st.get("error"))
                    self._persist(p)
                    self._event(p["proc_id"], st["state"], f"offset={st['offset']} charge={st['charge']}")
                    try:   # release the runtime slot: nothing is left behind on the node
                        self._call(p["node"], {"type": "PROC_REAP", "proc_id": p["proc_id"]}, timeout=0.8)
                    except Exception:
                        pass
                    changed = True
            if changed:
                with self.cond:
                    self.cond.notify_all()

    # ------------------------------------------------------------------ COORDINATE
    def wait(self, proc_ids, timeout=120.0):
        """Barrier: return when every process is in a terminal state."""
        t0 = time.time()
        with self.cond:
            while True:
                states = {i: self.procs[i]["state"] for i in proc_ids if i in self.procs}
                if all(s in TERMINAL for s in states.values()):
                    break
                left = timeout - (time.time() - t0)
                if left <= 0:
                    return {"timeout": True, "states": states}
                self.cond.wait(min(left, 0.5))
        return {"timeout": False, "states": states, "wait_s": round(time.time() - t0, 3),
                "finished_at": max((self.procs[i]["finished"] or 0) for i in proc_ids)}

    def reduce(self, proc_ids):
        ps = [self.procs[i] for i in proc_ids]
        return {"charge": round(sum(p["charge"] for p in ps), 6), "usage": round(sum(p["usage"] for p in ps), 6),
                "records": sum(p["offset"] for p in ps), "restarts": sum(p["restarts"] for p in ps),
                "states": sorted({p["state"] for p in ps})}

    # ------------------------------------------------------------------ TERMINATE
    def terminate(self, proc_id, cascade=True):
        p = self.procs.get(proc_id)
        if not p:
            return {"error": "unknown process"}
        stopped = [proc_id]
        if cascade:
            for c in [q for q in self.procs.values() if q["parent"] == proc_id and q["state"] not in TERMINAL]:
                stopped += self.terminate(c["proc_id"], cascade=True).get("terminated", [])
        t0 = time.perf_counter()
        loc = self.locate(proc_id)
        if loc.get("node") and p["state"] not in TERMINAL:
            try:
                st = self._call(loc["node"], {"type": "PROC_STOP", "proc_id": proc_id}, timeout=6)
                p.update(offset=st.get("offset", p["offset"]), charge=st.get("charge", p["charge"]),
                         usage=st.get("usage", p["usage"]))
                self._call(loc["node"], {"type": "PROC_REAP", "proc_id": proc_id})
            except Exception as exc:
                self._event(proc_id, "TERMINATE_WARNING", str(exc))
        if p["state"] not in TERMINAL or p["state"] == "RUNNING":
            p["state"] = "TERMINATED"
        p["finished"] = time.time()
        try:
            self.admin.unregister(proc_id)
        except Exception:
            pass
        self.resolver.invalidate(proc_id)
        self._persist(p)
        self._event(proc_id, "TERMINATED", "name removed, runtime slot released")
        with self.cond:
            self.cond.notify_all()
        return {"terminated": stopped, "terminate_ms": round((time.perf_counter() - t0) * 1000, 3)}

    # ------------------------------------------------------------------ MIGRATE
    def migrate(self, proc_id, to_node):
        p = self.procs.get(proc_id)
        if not p or p["state"] != "RUNNING" or p["node"] == to_node:
            return {"error": "cannot migrate", "state": (p or {}).get("state")}
        t0 = time.perf_counter()
        src = p["node"]
        st = self._call(src, {"type": "PROC_STOP", "proc_id": proc_id}, timeout=6)   # checkpoint
        if st["state"] == "DONE":
            return {"error": "already finished", "state": "DONE"}
        t_stop = time.perf_counter()
        spec = dict(p["spec"], resume_offset=st["offset"], resume_charge=st["charge"], resume_usage=st["usage"])
        p["state"] = "MIGRATING"
        reply = self._call(to_node, {"type": "PROC_START", "proc_id": proc_id, "spec": spec})
        if not reply.get("started"):
            p["state"] = "RUNNING"
            self._call(src, {"type": "PROC_START", "proc_id": proc_id, "spec": spec})
            return {"error": "target refused", "detail": reply}
        p.update(node=to_node, offset=st["offset"], charge=st["charge"], usage=st["usage"], state="RUNNING")
        self.admin.register_one({"name": proc_id, "kind": "process", "locations": [to_node], "ttl": 2.0})
        self.resolver.invalidate(proc_id)
        try:
            self._call(src, {"type": "PROC_REAP", "proc_id": proc_id})
        except Exception:
            pass
        self._persist(p)
        self._event(proc_id, "MIGRATED", f"{src} -> {to_node} at offset {st['offset']}")
        return {"proc_id": proc_id, "from": src, "to": to_node, "checkpoint_offset": st["offset"],
                "stopped_ms": round((t_stop - t0) * 1000, 3), "total_ms": round((time.perf_counter() - t0) * 1000, 3)}

    # ------------------------------------------------------------------ housekeeping
    def table(self):
        return {"processes": [self.info(i) for i in sorted(self.procs)]}

    def reset(self):
        """Experiment clean-up: stop everything, forget everything."""
        for pid in list(self.procs):
            if self.procs[pid]["state"] not in TERMINAL:
                self.terminate(pid, cascade=False)
        for pid in list(self.procs):
            try:
                self.admin.unregister(pid)
            except Exception:
                pass
        self.procs.clear()
        with self._db() as db:
            db.execute("DELETE FROM processes")
        for n in self.nodes:
            try:
                for pid in self._call(n, {"type": "PROC_LIST"}).get("processes", {}):
                    self._call(n, {"type": "PROC_STOP", "proc_id": pid}, timeout=6)
                    self._call(n, {"type": "PROC_REAP", "proc_id": pid})
            except Exception:
                pass
        return {"ok": True}
