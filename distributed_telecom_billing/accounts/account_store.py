"""Subscriber account partitions hosted by a rating node (Week 9).

An account is a logical resource  acct.C00042  whose state is the sum of an append-only set of
charge operations. Every operation carries an id, so applying it twice changes nothing
(idempotent) and two replicas converge once they hold the same set of operations (a grow-only
set, which is why repair is a simple union). A node only serves partitions it owns; otherwise it
answers NOT_OWNER so a caller with a stale location knows to look the name up again.
"""
import os
import sqlite3
import threading


class AccountStore:
    def __init__(self, path):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._lock = threading.Lock()
        with self._connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS owned(prefix TEXT PRIMARY KEY, epoch INTEGER)")
            db.execute("""CREATE TABLE IF NOT EXISTS ops(
                op_id TEXT PRIMARY KEY, prefix TEXT, customer TEXT, charge REAL, usage REAL)""")
            db.execute("CREATE INDEX IF NOT EXISTS ops_customer ON ops(customer)")
            db.execute("CREATE INDEX IF NOT EXISTS ops_prefix ON ops(prefix)")

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        return db

    # -- ownership ----------------------------------------------------------
    def own_add(self, prefix, epoch=1):
        with self._lock, self._connect() as db:
            db.execute("INSERT OR REPLACE INTO owned VALUES(?,?)", (prefix, epoch))
        return {"owned": prefix, "epoch": epoch}

    def own_drop(self, prefix):
        with self._lock, self._connect() as db:
            db.execute("DELETE FROM owned WHERE prefix=?", (prefix,))
            n = db.execute("DELETE FROM ops WHERE prefix=?", (prefix,)).rowcount
        return {"dropped": prefix, "ops_removed": n}

    def owned(self):
        with self._connect() as db:
            return {p: e for p, e in db.execute("SELECT prefix, epoch FROM owned")}

    def owns(self, prefix):
        with self._connect() as db:
            return db.execute("SELECT epoch FROM owned WHERE prefix=?", (prefix,)).fetchone() is not None

    def who_has(self, name):
        """Does this node hold the partition that covers `name`?"""
        for prefix, epoch in self.owned().items():
            if name.startswith(prefix) or prefix.startswith(name):
                return {"has": True, "epoch": epoch, "prefix": prefix}
        return {"has": False}

    # -- operations ---------------------------------------------------------
    def apply(self, prefix, customer, op_id, charge, usage):
        with self._lock, self._connect() as db:
            if db.execute("SELECT 1 FROM owned WHERE prefix=?", (prefix,)).fetchone() is None:
                return {"error": "NOT_OWNER", "prefix": prefix}
            cur = db.execute("INSERT OR IGNORE INTO ops VALUES(?,?,?,?,?)", (op_id, prefix, customer, charge, usage))
            total, count = db.execute("SELECT COALESCE(SUM(charge),0), COUNT(*) FROM ops WHERE customer=?",
                                      (customer,)).fetchone()
        return {"applied": cur.rowcount == 1, "total_charges": round(total, 6), "op_count": count}

    def get(self, prefix, customer):
        with self._connect() as db:
            if db.execute("SELECT 1 FROM owned WHERE prefix=?", (prefix,)).fetchone() is None:
                return {"error": "NOT_OWNER", "prefix": prefix}
            total, count, usage = db.execute(
                "SELECT COALESCE(SUM(charge),0), COUNT(*), COALESCE(SUM(usage),0) FROM ops WHERE customer=?",
                (customer,)).fetchone()
        return {"customer": customer, "total_charges": round(total, 6), "op_count": count, "total_usage": round(usage, 6)}

    # -- replica repair and migration --------------------------------------
    def op_ids(self, prefix):
        with self._connect() as db:
            return [r[0] for r in db.execute("SELECT op_id FROM ops WHERE prefix=?", (prefix,))]

    def ops_fetch(self, op_ids):
        out = []
        with self._connect() as db:
            for i in range(0, len(op_ids), 500):
                chunk = op_ids[i:i + 500]
                q = ",".join("?" * len(chunk))
                out += [list(r) for r in db.execute(
                    f"SELECT op_id,prefix,customer,charge,usage FROM ops WHERE op_id IN ({q})", chunk)]
        return out

    def ops_put(self, rows):
        with self._lock, self._connect() as db:
            before = db.total_changes
            db.executemany("INSERT OR IGNORE INTO ops VALUES(?,?,?,?,?)", rows)
            return {"inserted": db.total_changes - before}
