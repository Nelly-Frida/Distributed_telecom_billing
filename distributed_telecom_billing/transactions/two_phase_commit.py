"""Durable 2PC coordinator with presumed-abort and commit-decision recovery."""
import csv, json, os, sqlite3, time, uuid, threading
from common.communication import send_json
from common.config import LEDGER_HOST, LEDGER_PORT, REQUEST_TIMEOUT, RATING_NODES, ACTIVE_RATING_NODES


class CoordinatorJournal:
    def __init__(self, path):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with sqlite3.connect(self.path, timeout=15) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS coordinator_transactions (
                transaction_id TEXT PRIMARY KEY, payload TEXT NOT NULL, rating_node TEXT NOT NULL,
                state TEXT NOT NULL, decision TEXT, prepare_ms REAL, commit_ms REAL,
                total_ms REAL, failure TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL
            )""")

    def create(self, txid, payload, rating_node):
        now=time.time()
        with sqlite3.connect(self.path, timeout=15) as db:
            db.execute("INSERT OR IGNORE INTO coordinator_transactions(transaction_id,payload,rating_node,state,decision,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                       (txid,json.dumps(payload,sort_keys=True),rating_node,"PREPARING",None,now,now))

    def update(self, txid, **fields):
        allowed={"state","decision","prepare_ms","commit_ms","total_ms","failure"}
        fields={k:v for k,v in fields.items() if k in allowed}
        if not fields:return
        fields["updated_at"]=time.time()
        sql=", ".join(f"{k}=?" for k in fields)
        with sqlite3.connect(self.path, timeout=15) as db:
            db.execute(f"UPDATE coordinator_transactions SET {sql} WHERE transaction_id=?", (*fields.values(),txid))

    def get(self, txid):
        with sqlite3.connect(self.path, timeout=15) as db:
            db.row_factory=sqlite3.Row
            row=db.execute("SELECT * FROM coordinator_transactions WHERE transaction_id=?",(txid,)).fetchone()
        return dict(row) if row else None

    def pending(self):
        with sqlite3.connect(self.path, timeout=15) as db:
            db.row_factory=sqlite3.Row
            rows=db.execute("SELECT * FROM coordinator_transactions WHERE state IN ('PREPARING','PREPARED','COMMIT_DECIDED','ABORT_DECIDED','IN_DOUBT') ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]


class TwoPhaseCommitCoordinator:
    def __init__(self, csv_path=None, journal_path=None):
        self.csv_path=csv_path or os.getenv("TX_CSV_PATH","results/week5_transactions.csv")
        self.journal=CoordinatorJournal(journal_path or os.getenv("TX_JOURNAL_PATH","data/coordinator.db"))
        os.makedirs(os.path.dirname(os.path.abspath(self.csv_path)),exist_ok=True)
        self._csv_lock=threading.Lock()
        if not os.path.exists(self.csv_path):
            with open(self.csv_path,"w",newline="") as f:
                csv.writer(f).writerow(["timestamp","transaction_id","customer_id","rating_node","prepare_ms","commit_ms","total_ms","status","failure"])

    def _log(self,row):
        with self._csv_lock:
            with open(self.csv_path,"a",newline="") as f: csv.writer(f).writerow(row)

    def _participants(self,rating_node):
        if rating_node not in RATING_NODES: raise ValueError(f"Unknown rating node: {rating_node}")
        return [(rating_node,RATING_NODES[rating_node]),("LEDGER",(LEDGER_HOST,LEDGER_PORT))]

    def _send(self, addr, typ, txid, **extra):
        return send_json(addr[0],addr[1],{"type":typ,"transaction_id":txid,**extra},timeout=REQUEST_TIMEOUT)

    def execute(self, request, rating_node=None, inject_failure=False):
        required=("customer_id","service","usage")
        missing=[k for k in required if k not in request]
        if missing: raise ValueError(f"Missing transaction fields: {', '.join(missing)}")
        if request["usage"] < 0: raise ValueError("Usage cannot be negative")
        txid=request.get("transaction_id") or f"TX-{uuid.uuid4().hex[:12].upper()}"
        rating_node=rating_node or ACTIVE_RATING_NODES[0]
        participants=self._participants(rating_node)
        payload={k:request[k] for k in ("request_id","customer_id","service","usage") if k in request}
        payload["transaction_id"]=txid
        existing=self.journal.get(txid)
        if existing:
            if json.loads(existing["payload"])!=payload or existing["rating_node"]!=rating_node:
                raise ValueError(f"Transaction ID {txid} already exists with different data")
            if existing["state"] in ("PREPARING","PREPARED","COMMIT_DECIDED","ABORT_DECIDED","IN_DOUBT"):
                self.recover_pending()
                existing=self.journal.get(txid)
            charge=None
            if existing["state"]=="COMMIT":
                try:
                    status=self._send(RATING_NODES[rating_node],"TX_STATUS",txid)
                    charge=(status.get("payload") or {}).get("charge")
                except Exception:pass
            return {"transaction_id":txid,"status":existing["state"],"charge":charge,"idempotent_replay":True}
        self.journal.create(txid,payload,rating_node)
        t0=time.perf_counter(); prepared=[]; charge=None
        try:
            for name,addr in participants:
                extra=payload if name==rating_node else {k:payload[k] for k in ("customer_id","service","usage")}
                if name=="LEDGER": extra={**extra,"charge":charge}
                response=self._send(addr,"TX_PREPARE",txid,**extra)
                if not response.get("prepared"): raise RuntimeError(f"{name} rejected PREPARE: {response}")
                if name==rating_node: charge=response["charge"]
                prepared.append((name,addr))
        except Exception as exc:
            failure=str(exc)
            # Persist the global ABORT decision before sending participant aborts.
            self.journal.update(txid,state="ABORT_DECIDED",decision="ABORT",failure=failure)
            total=(time.perf_counter()-t0)*1000
            result=self._finish_abort(txid,payload["customer_id"],rating_node,failure,participants)
            self.journal.update(txid,total_ms=total)
            self._log([time.time(),txid,payload["customer_id"],rating_node,"","",round(total,3),result["status"],failure])
            return {"transaction_id":txid,"status":result["status"],"error":failure,"charge":None}

        prepare_ms=(time.perf_counter()-t0)*1000
        self.journal.update(txid,state="PREPARED",prepare_ms=prepare_ms)
        # Boolean True preserves the original experiment's controlled abort.
        if inject_failure is True or inject_failure=="after_prepare":
            failure="Injected coordinator failure after PREPARE and before global COMMIT decision"
            self.journal.update(txid,state="ABORT_DECIDED",decision="ABORT",prepare_ms=prepare_ms,failure=failure)
            result=self._finish_abort(txid,payload["customer_id"],rating_node,failure,participants)
            total=(time.perf_counter()-t0)*1000
            self.journal.update(txid,total_ms=total)
            self._log([time.time(),txid,payload["customer_id"],rating_node,round(prepare_ms,3),"",round(total,3),result["status"],failure])
            return {"transaction_id":txid,"status":result["status"],"error":failure,"charge":charge}

        # Persist the global decision BEFORE sending any COMMIT. This is the
        # critical write-ahead step that makes coordinator restart recoverable.
        self.journal.update(txid,state="COMMIT_DECIDED",decision="COMMIT")
        if inject_failure=="after_commit_decision":
            failure="Injected coordinator crash after durable COMMIT decision"
            total=(time.perf_counter()-t0)*1000
            self.journal.update(txid,state="IN_DOUBT",prepare_ms=prepare_ms,total_ms=total,failure=failure)
            self._log([time.time(),txid,payload["customer_id"],rating_node,round(prepare_ms,3),"",round(total,3),"IN_DOUBT",failure])
            return {"transaction_id":txid,"status":"IN_DOUBT","error":failure,"charge":charge,"decision":"COMMIT"}

        return self._finish_commit(txid,payload["customer_id"],rating_node,charge,t0,prepare_ms,participants)

    def _finish_commit(self,txid,customer_id,rating_node,charge,t0=None,prepare_ms=None,participants=None):
        row=self.journal.get(txid)
        participants=participants or self._participants(rating_node)
        t_commit=time.perf_counter(); errors=[]
        for name,addr in participants:
            try:
                response=self._send(addr,"TX_COMMIT",txid)
                if not response.get("committed"): errors.append(f"{name}: {response}")
            except Exception as exc: errors.append(f"{name}: {exc}")
        commit_ms=(time.perf_counter()-t_commit)*1000
        if errors:
            total_ms=((time.perf_counter()-t0)*1000) if t0 else (row.get("total_ms") or 0)+commit_ms
            failure="; ".join(errors)
            self.journal.update(txid,state="IN_DOUBT",decision="COMMIT",commit_ms=commit_ms,total_ms=total_ms,failure=failure)
            self._log([time.time(),txid,customer_id,rating_node,prepare_ms or row.get("prepare_ms") or "",round(commit_ms,3),round(total_ms,3),"IN_DOUBT",failure])
            return {"transaction_id":txid,"status":"IN_DOUBT","decision":"COMMIT","error":failure,"charge":charge}
        total_ms=((time.perf_counter()-t0)*1000) if t0 else (row.get("total_ms") or 0)+commit_ms
        self.journal.update(txid,state="COMMIT",decision="COMMIT",commit_ms=commit_ms,total_ms=total_ms,failure="")
        self._log([time.time(),txid,customer_id,rating_node,round(prepare_ms or row.get("prepare_ms") or 0,3),round(commit_ms,3),round(total_ms,3),"COMMIT",""])
        return {"transaction_id":txid,"status":"COMMIT","charge":charge,"rating_node":rating_node,
                "prepare_ms":round(prepare_ms or row.get("prepare_ms") or 0,3),"commit_ms":round(commit_ms,3),"total_ms":round(total_ms,3)}

    def _finish_abort(self,txid,customer_id,rating_node,failure="",participants=None):
        participants=participants or self._participants(rating_node)
        errors=[]
        for name,addr in participants:
            try:
                response=self._send(addr,"TX_ABORT",txid)
                if response.get("aborted") is False: errors.append(f"{name}: {response}")
            except Exception as exc: errors.append(f"{name}: {exc}")
        if errors:
            message="; ".join(errors)
            self.journal.update(txid,state="IN_DOUBT",decision="ABORT",failure=(failure+"; "+message).strip("; "))
            return {"transaction_id":txid,"status":"IN_DOUBT","decision":"ABORT","error":message}
        self.journal.update(txid,state="ABORT",decision="ABORT",failure=failure)
        return {"transaction_id":txid,"status":"ABORT"}

    def recover_pending(self):
        recovered=[]
        for row in self.journal.pending():
            txid=row["transaction_id"]; rating_node=row["rating_node"]; participants=self._participants(rating_node)
            if row.get("decision")!="COMMIT":
                # Presumed-abort recovery for transactions without a durable decision.
                self.journal.update(txid,state="ABORT_DECIDED",decision="ABORT")
                result=self._finish_abort(txid,json.loads(row["payload"]).get("customer_id"),rating_node,
                                          "Recovery applied presumed-abort policy",participants)
                recovered.append(result)
                continue
            payload=json.loads(row["payload"]); charge=None
            try:
                rating_status=self._send(participants[0][1],"TX_STATUS",txid)
                if rating_status.get("payload"): charge=rating_status["payload"].get("charge")
            except Exception:pass
            result=self._finish_commit(txid,payload.get("customer_id"),rating_node,charge,participants=participants)
            recovered.append(result)
        return {"recovered":recovered,"count":len(recovered)}

    def status(self,txid):
        row=self.journal.get(txid)
        if not row:return {"transaction_id":txid,"status":"UNKNOWN"}
        return {"transaction_id":txid,"status":row["state"],"decision":row["decision"],"failure":row["failure"]}
