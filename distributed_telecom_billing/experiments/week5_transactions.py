"""Week 5: durable 2PC, controlled abort, commit-decision recovery and metrics."""
import csv, json, os, sys, time
PROJECT_ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path: sys.path.insert(0,PROJECT_ROOT)
from common.communication import send_json
from common.config import NODE_HOST,NODE2_PORT,LEDGER_HOST,LEDGER_PORT


RUN_ID=str(time.time_ns())

def submit(i,inject_failure=False):
    txid=f"A3-W5-{RUN_ID}-{i:04d}"
    req={"type":"TX_BILL","transaction_id":txid,"request_id":i,"customer_id":f"C{1000+i}",
         "service":"data","usage":10+i%20,"inject_failure":inject_failure}
    t=time.perf_counter(); result=send_json(NODE_HOST,NODE2_PORT,req,timeout=20)
    result["client_ms"]=round((time.perf_counter()-t)*1000,3)
    return result


def main():
    print("="*72); print("ASSIGNMENT 3 — WEEK 5: DURABLE DISTRIBUTED TRANSACTIONS (2PC)"); print("="*72)
    results=[]
    normal_start=time.perf_counter()
    for i in range(1,11):
        r=submit(i); results.append(r); print(json.dumps(r,indent=2))
    normal_elapsed=time.perf_counter()-normal_start
    normal_commits=sum(x.get("status")=="COMMIT" for x in results)
    normal_metrics={"submitted":10,"committed":normal_commits,"elapsed_s":round(normal_elapsed,4),
                    "throughput_tx_s":round(normal_commits/normal_elapsed,3) if normal_elapsed else 0,
                    "mean_client_latency_ms":round(sum(x.get("client_ms",0) for x in results)/len(results),3)}
    print("\nNormal transaction workload metrics:",json.dumps(normal_metrics,indent=2))
    print("\n[CONTROLLED ABORT] failure after PREPARE, before global COMMIT decision")
    abort=submit(90,True); results.append(abort); print(json.dumps(abort,indent=2))
    print("\n[COORDINATOR RECOVERY] simulate crash after durable COMMIT decision")
    interrupted=submit(91,"after_commit_decision"); results.append(interrupted); print(json.dumps(interrupted,indent=2))
    recovered=send_json(NODE_HOST,NODE2_PORT,{"type":"TX_RECOVER"},timeout=20)
    print("Recovery result:",json.dumps(recovered,indent=2))
    ledger=send_json(LEDGER_HOST,LEDGER_PORT,{"type":"LEDGER_STATS"})
    print("Ledger durable state:",ledger)
    consistency=[]
    for item in results:
        if item.get("status")=="COMMIT" or item.get("transaction_id")==interrupted.get("transaction_id"):
            txid=item["transaction_id"]
            rating=send_json("127.0.0.1",5003,{"type":"TX_STATUS","transaction_id":txid})
            ledger_state=send_json(LEDGER_HOST,LEDGER_PORT,{"type":"TX_STATUS","transaction_id":txid})
            rating_charge=(rating.get("payload") or {}).get("charge")
            ledger_charge=(ledger_state.get("payload") or {}).get("charge")
            ok=rating.get("state")=="COMMITTED" and ledger_state.get("state")=="COMMITTED" and rating_charge==ledger_charge
            consistency.append({"transaction_id":txid,"rating_state":rating.get("state"),"ledger_state":ledger_state.get("state"),
                                "rating_charge":rating_charge,"ledger_charge":ledger_charge,"consistent":ok})
    print("\nParticipant consistency:",json.dumps(consistency,indent=2))
    os.makedirs("results",exist_ok=True)
    with open("results/week5_experiment_summary.json","w") as f:
        json.dump({"normal_workload_metrics":normal_metrics,"transactions":results,"recovery":recovered,"ledger":ledger,"consistency":consistency},f,indent=2)
    committed=sum(x.get("status")=="COMMIT" for x in results)
    aborted=sum(x.get("status")=="ABORT" for x in results)
    indoubt=sum(x.get("status")=="IN_DOUBT" for x in results)
    print(f"\nSummary: initial COMMIT={committed}, ABORT={aborted}, IN_DOUBT before recovery={indoubt}")
    print("See results/week5_transactions.csv and results/week5_experiment_summary.json")

if __name__=="__main__":main()
