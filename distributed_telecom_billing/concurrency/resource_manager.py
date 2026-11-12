"""Thread-safe resource manager with wait-for graph and deadlock recovery."""
import threading
import time
from collections import defaultdict


class ResourceManager:
    def __init__(self):
        self.locks = defaultdict(threading.Lock)
        self.waiting = defaultdict(set)       # transaction -> resources requested
        self.owner = {}                       # resource -> transaction
        self.held = defaultdict(set)           # transaction -> resources held
        self.cancelled = set()
        self.meta_lock = threading.RLock()

    def acquire(self, txid, resource, timeout=2.0, poll_interval=0.005):
        start=time.perf_counter(); deadline=start+timeout
        with self.meta_lock:
            if txid in self.cancelled: return False
        while time.perf_counter() < deadline:
            with self.meta_lock:
                if txid in self.cancelled:
                    self.waiting[txid].discard(resource)
                    return False
            if self.locks[resource].acquire(blocking=False):
                with self.meta_lock:
                    if txid in self.cancelled:
                        self.locks[resource].release()
                        self.waiting[txid].discard(resource)
                        return False
                    self.waiting[txid].discard(resource)
                    self.owner[resource]=txid
                    self.held[txid].add(resource)
                return True
            with self.meta_lock:
                self.waiting[txid].add(resource)
            time.sleep(poll_interval)
        with self.meta_lock:
            self.waiting[txid].discard(resource)
        return False

    def acquire_ordered(self, txid, resources, timeout=2.0):
        """Deadlock prevention: acquire all resources in one global order."""
        acquired=[]; start=time.perf_counter()
        for resource in sorted(set(resources)):
            remaining=max(0.001,timeout-(time.perf_counter()-start))
            if not self.acquire(txid,resource,timeout=remaining):
                self.release_all(txid)
                return False
            acquired.append(resource)
        return True

    def release(self, txid, resource):
        should_release=False
        with self.meta_lock:
            if self.owner.get(resource)==txid:
                self.owner.pop(resource,None)
                self.held[txid].discard(resource)
                should_release=True
            self.waiting[txid].discard(resource)
        if should_release:
            self.locks[resource].release()

    def release_all(self, txid):
        with self.meta_lock:
            resources=list(self.held.get(txid,set()))
            self.waiting.pop(txid,None)
        for resource in resources:
            self.release(txid,resource)

    def abort(self, txid):
        with self.meta_lock:
            self.cancelled.add(txid)
        self.release_all(txid)

    def finish(self, txid):
        self.release_all(txid)
        with self.meta_lock:
            self.cancelled.discard(txid)
            self.waiting.pop(txid,None)

    def wait_for_graph(self):
        graph={}
        with self.meta_lock:
            for tx,resources in self.waiting.items():
                for resource in resources:
                    holder=self.owner.get(resource)
                    if holder and holder!=tx:
                        graph.setdefault(tx,set()).add(holder)
        return graph

    @staticmethod
    def has_cycle(graph):
        visiting=set(); visited=set()
        def dfs(node):
            if node in visiting:return True
            if node in visited:return False
            visiting.add(node)
            for nxt in graph.get(node,()):
                if dfs(nxt):return True
            visiting.remove(node); visited.add(node); return False
        return any(dfs(node) for node in graph)

    @staticmethod
    def select_victim(graph):
        """Deterministic policy: abort lexicographically greatest cycle member."""
        members=set(graph)
        for children in graph.values(): members.update(children)
        return sorted(members)[-1] if members else None

    def resource_allocation_graph(self):
        """Return both allocated and requested edges for the RAG report."""
        with self.meta_lock:
            allocated={resource:tx for resource,tx in self.owner.items()}
            requested={tx:sorted(resources) for tx,resources in self.waiting.items() if resources}
        return {"allocated":allocated,"requested":requested}

    def stats(self):
        with self.meta_lock:
            return {"locked_resources":len(self.owner),"waiting_transactions":len(self.waiting),
                    "owners":dict(self.owner),"wait_for_graph":{k:sorted(v) for k,v in self.wait_for_graph().items()}}
