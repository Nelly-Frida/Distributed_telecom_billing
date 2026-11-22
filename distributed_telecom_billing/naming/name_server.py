"""Name server process (Week 10). One code base, four deployments (NS_NAME):

    NS-CENTRAL  every record (the central-name-server strategy and the prefix table)
    NS-ROOT     delegations only: zone label -> zone server (DNS-style hierarchy)
    NS-ACCT     authoritative for names under  acct.
    NS-SVC      authoritative for names under  svc.  and  proc.

Messages (newline-delimited JSON, like every other node): NS_RESOLVE, NS_REGISTER,
NS_REGISTER_MANY, NS_UNREGISTER, NS_UPDATE, NS_LIST, NS_PREFIX_TABLE, PROBE, FAULT_*.
"""
import json
import os
import socket
import sys
import threading
import time
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.config import BIND_HOST, NAME_SERVERS
from common.fault_state import FaultState
from naming.naming_store import NamingStore

NS_NAME = os.getenv("NS_NAME", "NS-CENTRAL")
PORT = NAME_SERVERS[NS_NAME][1]
store = NamingStore(os.getenv("NS_DB_PATH", f"data/naming_{NS_NAME.lower()}.db"))
faults = FaultState(NS_NAME)
requests_served = 0


def zone_of(name):
    return name.split(".")[0]


def resolve(name):
    """Root answers with a referral, every other server with a record."""
    if NS_NAME == "NS-ROOT":
        deleg = store.resolve(zone_of(name))
        if not deleg:
            return {"error": "NXDOMAIN", "name": name}
        return {"referral": deleg["locations"][0], "zone": zone_of(name), "ttl": deleg["ttl"], "epoch": deleg["epoch"]}
    rec = store.resolve(name)
    return rec if rec else {"error": "NXDOMAIN", "name": name}


def handle(connection):
    global requests_served
    connection.settimeout(30)
    try:
        buffer = b""
        while b"\n" not in buffer:
            chunk = connection.recv(65536)
            if not chunk:
                break
            buffer += chunk
        if not buffer:
            return
        r = json.loads(buffer.split(b"\n", 1)[0].decode())
        if faults.is_control(r):
            connection.sendall((json.dumps(faults.control(r)) + "\n").encode())
            return
        action = faults.intercept(r)
        if action:
            if action[0] == "drop":
                return
            if action[0] == "omit":
                time.sleep(action[1])
                return
            if action[0] == "error":
                connection.sendall((json.dumps({"error": action[1], "node": NS_NAME}) + "\n").encode())
                return
        kind = r.get("type")
        requests_served += 1
        if kind == "NS_RESOLVE":
            out = resolve(r["name"])
        elif kind == "NS_REGISTER":
            out = store.register(r["name"], r.get("kind", "partition"), r["locations"], r.get("ttl", 5.0), r.get("is_prefix", False))
        elif kind == "NS_REGISTER_MANY":
            out = store.register_many(r["records"])
        elif kind == "NS_UPDATE":
            old = store.resolve(r["name"])
            if not old or old["name"] != r["name"]:
                out = {"error": "NXDOMAIN", "name": r["name"]}
            else:
                out = store.register(old["name"], old["kind"], r["locations"], old["ttl"], old["is_prefix"])
        elif kind == "NS_UNREGISTER":
            out = store.unregister(r["name"])
        elif kind == "NS_CLEAR":
            out = store.clear()
        elif kind == "NS_LIST":
            out = {"records": store.list(r.get("prefix", ""))}
        elif kind == "NS_PREFIX_TABLE":
            out = {"records": [x for x in store.list(r.get("prefix", "")) if x["is_prefix"]]}
        elif kind == "PROBE":
            out = {"ok": True, "node": NS_NAME, "records": store.count(), "served": requests_served}
        else:
            out = {"error": f"unknown message type {kind!r}"}
        out["server"] = NS_NAME
        connection.sendall((json.dumps(out) + "\n").encode())
    except Exception as exc:
        try:
            connection.sendall((json.dumps({"error": str(exc), "server": NS_NAME}) + "\n").encode())
        except Exception:
            pass
    finally:
        connection.close()


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((BIND_HOST, PORT))
    srv.listen(512)
    print(f"{NS_NAME} name server on {BIND_HOST}:{PORT} PID={os.getpid()} records={store.count()}", flush=True)
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
