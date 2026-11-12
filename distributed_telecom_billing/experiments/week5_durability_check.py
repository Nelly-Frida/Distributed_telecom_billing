"""Verify committed participant state survives service process/container restart."""
import json, os, sys
PROJECT_ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:sys.path.insert(0,PROJECT_ROOT)
from common.communication import send_json
from common.config import NODE_HOST,NODE2_PORT,LEDGER_HOST,LEDGER_PORT


def main():
    path="results/week5_experiment_summary.json"
    if not os.path.exists(path):raise SystemExit("Run week5_transactions.py first")
    with open(path) as f:summary=json.load(f)
    items=summary.get("transactions",[])
    committed=next((x for x in items if x.get("status")=="COMMIT" and x.get("transaction_id")),None)
    interrupted=next((x for x in items if x.get("status")=="IN_DOUBT" and x.get("transaction_id")),None)
    chosen=interrupted or committed
    if not chosen:raise SystemExit("No committed transaction found in the Week 5 summary")
    txid=chosen["transaction_id"]
    rating=send_json("127.0.0.1",5003,{"type":"TX_STATUS","transaction_id":txid})
    ledger=send_json(LEDGER_HOST,LEDGER_PORT,{"type":"TX_STATUS","transaction_id":txid})
    coordinator=send_json(NODE_HOST,NODE2_PORT,{"type":"TX_STATUS","transaction_id":txid})
    rp=(rating.get("payload") or {}).get("charge"); lp=(ledger.get("payload") or {}).get("charge")
    passed=rating.get("state")=="COMMITTED" and ledger.get("state")=="COMMITTED" and rp==lp and coordinator.get("status")=="COMMIT"
    print(json.dumps({"transaction_id":txid,"rating_state":rating.get("state"),"ledger_state":ledger.get("state"),
                      "coordinator_state":coordinator.get("status"),"rating_charge":rp,"ledger_charge":lp,
                      "durability_check":"PASS" if passed else "FAIL"},indent=2))
    if not passed:raise SystemExit(1)

if __name__=="__main__":main()
