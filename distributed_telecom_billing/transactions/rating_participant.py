"""Durable rating-node participant for the Week 5 two-phase commit."""
import os
import threading
from transactions.state_store import TransactionStateStore


class RatingTransactionParticipant:
    def __init__(self, engine, db_path=None):
        self.engine = engine
        self.store = TransactionStateStore(db_path or os.getenv("RATING_DB_PATH", "data/rating_transactions.db"))
        self._lock = threading.RLock()

    def prepare(self, request):
        txid = request["transaction_id"]
        charge = self.engine.calculate_charge(request["service"], request["usage"])
        payload = {"charge": charge, "request": dict(request)}
        with self._lock:
            result = self.store.prepare(txid, payload)
        return {"prepared": True, "transaction_id": txid, "charge": result["payload"]["charge"],
                "already_committed": result.get("already_committed", False)}

    def commit(self, txid):
        with self._lock:
            return self.store.commit(txid)

    def abort(self, txid):
        with self._lock:
            return self.store.abort(txid)

    def status(self, txid):
        return self.store.status(txid)
