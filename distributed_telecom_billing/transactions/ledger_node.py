"""Durable distributed ledger participant for the Week 5 2PC protocol."""
import json, os, socket, sys, threading
PROJECT_ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path: sys.path.insert(0, PROJECT_ROOT)
from common.config import BIND_HOST, LEDGER_PORT
from transactions.state_store import TransactionStateStore

ledger = TransactionStateStore(os.getenv("LEDGER_DB_PATH", "data/ledger_transactions.db"))


def handle(c):
    try:
        b=b""
        while b"\n" not in b:
            x=c.recv(4096)
            if not x: break
            b+=x
        if not b: return
        r=json.loads(b.split(b"\n",1)[0].decode()); typ=r.get("type"); tx=r.get("transaction_id")
        if typ=="TX_PREPARE":
            payload={k:r[k] for k in ("customer_id","charge","service","usage") if k in r}
            out=ledger.prepare(tx,payload)
        elif typ=="TX_COMMIT": out=ledger.commit(tx)
        elif typ=="TX_ABORT": out=ledger.abort(tx)
        elif typ=="TX_STATUS": out=ledger.status(tx)
        elif typ=="LEDGER_STATS": out=ledger.stats()
        else: out={"error":"unknown type"}
        c.sendall((json.dumps(out)+"\n").encode())
    except Exception as e:
        try:c.sendall((json.dumps({"error":str(e)})+"\n").encode())
        except Exception:pass
    finally:c.close()


def main():
    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    s.bind((BIND_HOST,LEDGER_PORT)); s.listen(100)
    print(f"LEDGER NODE running on {BIND_HOST}:{LEDGER_PORT} PID={os.getpid()}")
    while True: threading.Thread(target=handle,args=(s.accept()[0],),daemon=True).start()

if __name__=="__main__": main()
