"""Name-resolution strategies (Week 10): Name -> Name Resolution -> Resource Location.

    static     local hard-coded table (no messages; goes stale when a resource moves)
    broadcast  ask every node "who has this name?" (N messages per lookup, no central state)
    central    one name server holds every record (1 message; single point of failure)
    dns        hierarchical: root -> zone server referral, with TTL caching (2 / 1 / 0 messages)
    prefix     the prefix (partition) table is cached locally; longest-prefix match (0 messages
               on a hit); refreshed when a node answers NOT_OWNER

Every resolver returns a record {"name", "locations", "epoch", "kind"} or None and counts the
messages it sent, so strategies can be compared on latency, message cost and staleness.
"""
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from common.communication import send_json
from common.config import (ACCOUNT_PREFIX_DIGITS, NAME_SERVERS, NAMING_TTL_S, RATING_NODES, STATIC_MAP_PATH)

ZONE_SERVER = {"acct": "NS-ACCT", "svc": "NS-SVC", "proc": "NS-SVC"}


def partition_of(account_name, digits=None):
    """acct.C00042 -> acct.C000 (prefix length = 'acct.C' + digits)."""
    digits = ACCOUNT_PREFIX_DIGITS if digits is None else digits
    return account_name[: len("acct.C") + digits]


class Resolver:
    kind = "base"

    def __init__(self, timeout=1.0):
        self.timeout = timeout
        self._lock = threading.Lock()
        self.stats = {"lookups": 0, "messages": 0, "cache_hits": 0, "errors": 0}

    def _rpc(self, addr, msg, timeout=None):
        with self._lock:
            self.stats["messages"] += 1
        return send_json(addr[0], addr[1], msg, timeout=timeout or self.timeout)

    def _count(self, key, n=1):
        with self._lock:
            self.stats[key] += n

    def resolve(self, name):
        self._count("lookups")
        try:
            return self._resolve(name)
        except Exception:
            self._count("errors")
            return None

    def _resolve(self, name):
        raise NotImplementedError

    def invalidate(self, name=None):
        """Called when a node says the cached location is wrong."""

    def reset_stats(self):
        with self._lock:
            self.stats = {"lookups": 0, "messages": 0, "cache_hits": 0, "errors": 0}


class PrefixIndex:
    """Longest-prefix match in O(number of distinct prefix lengths), not O(number of records)."""

    def __init__(self, table):
        self.table = table
        self.by_len = {}
        for key, rec in table.items():
            self.by_len.setdefault(len(key), {})[key] = rec
        self.lengths = sorted(self.by_len, reverse=True)

    def match(self, name):
        for length in self.lengths:
            if length <= len(name):
                rec = self.by_len[length].get(name[:length])
                if rec:
                    return rec
        return None


class StaticResolver(Resolver):
    kind = "static"

    def __init__(self, table=None, path=None, **kw):
        super().__init__(**kw)
        if table is None:
            with open(path or STATIC_MAP_PATH) as f:
                table = json.load(f)
        self.table = table
        self.index = PrefixIndex(table)

    def _resolve(self, name):
        rec = self.index.match(name)
        return dict(rec) if rec else None


class CentralResolver(Resolver):
    kind = "central"

    def _resolve(self, name):
        rec = self._rpc(NAME_SERVERS["NS-CENTRAL"], {"type": "NS_RESOLVE", "name": name})
        return None if rec.get("error") else rec


class DnsResolver(Resolver):
    kind = "dns"

    def __init__(self, ttl=None, **kw):
        super().__init__(**kw)
        self.ttl = NAMING_TTL_S if ttl is None else ttl
        self.records = {}       # record name -> (record, expires)
        self.delegations = {}   # zone -> (server name, expires)

    def invalidate(self, name=None):
        self.records.clear()

    def _cached(self, name):
        now = time.time()
        best = None
        for key, (rec, exp) in list(self.records.items()):
            if exp < now:
                self.records.pop(key, None)
            elif name == key or (rec.get("is_prefix") and name.startswith(key)):
                if best is None or len(key) > len(best["name"]):
                    best = rec
        return best

    def _resolve(self, name):
        hit = self._cached(name)
        if hit:
            self._count("cache_hits")
            return dict(hit)
        zone = name.split(".")[0]
        now = time.time()
        deleg = self.delegations.get(zone)
        if not deleg or deleg[1] < now:
            ref = self._rpc(NAME_SERVERS["NS-ROOT"], {"type": "NS_RESOLVE", "name": name})
            if ref.get("error"):
                return None
            deleg = (ref["referral"], now + max(self.ttl * 6, 30))
            self.delegations[zone] = deleg
        rec = self._rpc(NAME_SERVERS[deleg[0]], {"type": "NS_RESOLVE", "name": name})
        if rec.get("error"):
            return None
        self.records[rec["name"]] = (rec, time.time() + min(self.ttl, rec.get("ttl", self.ttl)))
        return rec


class PrefixResolver(Resolver):
    kind = "prefix"

    def __init__(self, **kw):
        super().__init__(**kw)
        self.table = None

    def invalidate(self, name=None):
        self.table = None

    def _load(self):
        reply = self._rpc(NAME_SERVERS["NS-CENTRAL"], {"type": "NS_PREFIX_TABLE"}, timeout=max(self.timeout, 5))
        self.table = {r["name"]: r for r in reply["records"]}
        self.index = PrefixIndex(self.table)

    def _resolve(self, name):
        if self.table is None:
            self._load()
        else:
            self._count("cache_hits")
        rec = self.index.match(name)
        return dict(rec) if rec else None


class BroadcastResolver(Resolver):
    kind = "broadcast"

    def __init__(self, nodes=None, **kw):
        super().__init__(**kw)
        self.nodes = dict(nodes if nodes is not None else RATING_NODES)

    def _resolve(self, name):
        def ask(item):
            node, addr = item
            try:
                return node, self._rpc(addr, {"type": "WHO_HAS", "name": name}, timeout=self.timeout)
            except Exception:
                return node, None

        with ThreadPoolExecutor(max_workers=max(1, min(64, len(self.nodes)))) as pool:
            answers = list(pool.map(ask, self.nodes.items()))
        owners = sorted(n for n, r in answers if r and r.get("has"))
        if not owners:
            return None
        epoch = max((r.get("epoch", 0) for n, r in answers if r and r.get("has")), default=0)
        return {"name": name, "kind": "partition", "locations": owners, "epoch": epoch}


def make_resolver(mode=None, **kw):
    mode = mode or "prefix"
    cls = {"static": StaticResolver, "broadcast": BroadcastResolver, "central": CentralResolver,
           "dns": DnsResolver, "prefix": PrefixResolver}[mode]
    return cls(**kw)


class NamingAdmin:
    """Writes the same records into every deployment (the comparison harness) and keeps the static map."""

    def __init__(self, timeout=10.0):
        self.timeout = timeout

    def _send(self, server, msg):
        return send_json(NAME_SERVERS[server][0], NAME_SERVERS[server][1], msg, timeout=self.timeout)

    def publish(self, records, static_path=None):
        """records: [{"name","kind","locations","ttl","is_prefix"}]"""
        by_zone = {}
        for r in records:
            by_zone.setdefault(r["name"].split(".")[0], []).append(r)
        self._send("NS-CENTRAL", {"type": "NS_REGISTER_MANY", "records": records})
        self._send("NS-ROOT", {"type": "NS_REGISTER_MANY", "records": [
            {"name": z, "kind": "delegation", "locations": [ZONE_SERVER[z]], "ttl": 60.0} for z in ZONE_SERVER]})
        for server in ("NS-ACCT", "NS-SVC"):
            mine = [r for z, rs in by_zone.items() if ZONE_SERVER[z] == server for r in rs]
            if mine:
                self._send(server, {"type": "NS_REGISTER_MANY", "records": mine})
        if static_path:
            self.snapshot_static(records, static_path)

    @staticmethod
    def snapshot_static(records, path):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        table = {r["name"]: {"name": r["name"], "kind": r["kind"], "locations": r["locations"], "epoch": 0}
                 for r in records}
        with open(path, "w") as f:
            json.dump(table, f)

    def register_one(self, record):
        """Register a single record (central + its zone server), e.g. a newly created process."""
        self._send("NS-CENTRAL", {"type": "NS_REGISTER", **record})
        return self._send(ZONE_SERVER[record["name"].split(".")[0]], {"type": "NS_REGISTER", **record})

    def unregister(self, name):
        self._send("NS-CENTRAL", {"type": "NS_UNREGISTER", "name": name})
        return self._send(ZONE_SERVER[name.split(".")[0]], {"type": "NS_UNREGISTER", "name": name})

    def move(self, name, locations):
        """Change a record's locations everywhere (epoch bumps in each store)."""
        out = self._send("NS-CENTRAL", {"type": "NS_UPDATE", "name": name, "locations": locations})
        zone_server = ZONE_SERVER[name.split(".")[0]]
        self._send(zone_server, {"type": "NS_UPDATE", "name": name, "locations": locations})
        return out

    def clear(self):
        for server in NAME_SERVERS:
            try:
                self._send(server, {"type": "NS_CLEAR"})
            except Exception:
                pass
