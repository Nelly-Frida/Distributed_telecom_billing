"""Durable name records with exact and longest-prefix resolution (Week 10).

A record maps a logical name to the physical nodes that hold the resource:

    name        acct.C000          (a prefix record covers acct.C00042, acct.C00999, ...)
    kind        service | partition | process
    locations   ["NODE-3", "NODE-5"]   ordered replica list
    epoch       bumped on every change, so a stale cached copy can be recognised
    is_prefix   1 if the record covers every name that starts with `name`
"""
import json
import os
import sqlite3
import threading
import time


class NamingStore:
    def __init__(self, path):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._lock = threading.Lock()
        with self._connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS records(
                name TEXT PRIMARY KEY, kind TEXT, locations TEXT, epoch INTEGER,
                ttl REAL, is_prefix INTEGER, updated REAL)""")
        self._cache = None   # in-memory copy: name -> record, rebuilt after a write
        self._prefixes = {}  # length -> {prefix: name}

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        return db

    @staticmethod
    def _row(r):
        return {"name": r[0], "kind": r[1], "locations": json.loads(r[2]), "epoch": r[3],
                "ttl": r[4], "is_prefix": bool(r[5]), "updated": r[6]}

    def _load(self):
        if self._cache is not None:
            return
        with self._connect() as db:
            rows = [self._row(r) for r in db.execute("SELECT * FROM records")]
        self._cache = {r["name"]: r for r in rows}
        self._prefixes = {}
        for r in rows:
            if r["is_prefix"]:
                self._prefixes.setdefault(len(r["name"]), {})[r["name"]] = r["name"]

    def register(self, name, kind, locations, ttl=5.0, is_prefix=False):
        with self._lock:
            self._load()
            old = self._cache.get(name)
            epoch = (old["epoch"] + 1) if old else 1
            with self._connect() as db:
                db.execute("INSERT OR REPLACE INTO records VALUES(?,?,?,?,?,?,?)",
                           (name, kind, json.dumps(locations), epoch, ttl, int(is_prefix), time.time()))
            self._cache = None
            return {"name": name, "epoch": epoch}

    def register_many(self, records):
        with self._lock:
            self._load()
            now = time.time()
            rows = []
            for r in records:
                old = self._cache.get(r["name"])
                rows.append((r["name"], r["kind"], json.dumps(r["locations"]),
                             (old["epoch"] + 1) if old else 1, r.get("ttl", 5.0), int(r.get("is_prefix", False)), now))
            with self._connect() as db:
                db.executemany("INSERT OR REPLACE INTO records VALUES(?,?,?,?,?,?,?)", rows)
            self._cache = None
            return {"registered": len(rows)}

    def unregister(self, name):
        with self._lock:
            with self._connect() as db:
                n = db.execute("DELETE FROM records WHERE name=?", (name,)).rowcount
            self._cache = None
            return {"removed": n}

    def resolve(self, name):
        """Exact match first, then the longest registered prefix."""
        with self._lock:
            self._load()
            rec = self._cache.get(name)
            if rec:
                return rec
            for length in sorted(self._prefixes, reverse=True):
                if length <= len(name):
                    hit = self._prefixes[length].get(name[:length])
                    if hit:
                        return self._cache[hit]
            return None

    def list(self, prefix=""):
        with self._lock:
            self._load()
            return [r for n, r in sorted(self._cache.items()) if n.startswith(prefix)]

    def clear(self):
        with self._lock:
            with self._connect() as db:
                n = db.execute("DELETE FROM records").rowcount
            self._cache = None
            return {"removed": n}

    def count(self):
        with self._lock:
            self._load()
            return len(self._cache)
