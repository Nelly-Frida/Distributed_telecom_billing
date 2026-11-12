"""Small durable SQLite state store used by 2PC participants."""
import json
import os
import sqlite3
from contextlib import contextmanager


class TransactionStateStore:
    def __init__(self, path):
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        with self._connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT PRIMARY KEY,
                state TEXT NOT NULL CHECK(state IN ('PREPARED','COMMITTED')),
                payload TEXT NOT NULL,
                updated_at REAL NOT NULL DEFAULT (strftime('%s','now'))
            )""")

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA busy_timeout=15000")
        try:
            yield db
        finally:
            db.close()

    def prepare(self, txid, payload):
        encoded = json.dumps(payload, sort_keys=True)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT state,payload FROM transactions WHERE transaction_id=?", (txid,)).fetchone()
            if row:
                existing = json.loads(row[1])
                if existing != payload:
                    db.execute("ROLLBACK")
                    raise ValueError(f"Transaction {txid} was reused with different data")
                db.execute("COMMIT")
                return {"prepared": True, "already_committed": row[0] == "COMMITTED", "payload": existing}
            db.execute("INSERT INTO transactions(transaction_id,state,payload) VALUES(?,?,?)", (txid, "PREPARED", encoded))
            db.execute("COMMIT")
        return {"prepared": True, "payload": payload}

    def commit(self, txid):
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT state FROM transactions WHERE transaction_id=?", (txid,)).fetchone()
            if not row:
                db.execute("ROLLBACK")
                return {"committed": False, "error": "transaction not prepared"}
            if row[0] == "PREPARED":
                db.execute("UPDATE transactions SET state='COMMITTED', updated_at=strftime('%s','now') WHERE transaction_id=?", (txid,))
            db.execute("COMMIT")
        return {"committed": True, "already_committed": row[0] == "COMMITTED", "transaction_id": txid}

    def abort(self, txid):
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT state FROM transactions WHERE transaction_id=?", (txid,)).fetchone()
            if row and row[0] == "COMMITTED":
                db.execute("ROLLBACK")
                return {"aborted": False, "error": "commit already durable", "transaction_id": txid}
            db.execute("DELETE FROM transactions WHERE transaction_id=? AND state='PREPARED'", (txid,))
            db.execute("COMMIT")
        return {"aborted": True, "transaction_id": txid}

    def status(self, txid):
        with self._connect() as db:
            row = db.execute("SELECT state,payload FROM transactions WHERE transaction_id=?", (txid,)).fetchone()
        if not row:
            return {"state": "UNKNOWN", "transaction_id": txid}
        return {"state": row[0], "payload": json.loads(row[1]), "transaction_id": txid}

    def stats(self):
        with self._connect() as db:
            rows = db.execute("SELECT state,COUNT(*) FROM transactions GROUP BY state").fetchall()
        counts = {"prepared": 0, "committed": 0}
        for state, count in rows:
            counts[state.lower()] = count
        return counts
