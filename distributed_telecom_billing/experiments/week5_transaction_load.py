"""Concurrent 2PC workload to measure transaction throughput and latency."""
import argparse,csv,json,os,sys,time
from concurrent.futures import ThreadPoolExecutor,as_completed
PROJECT_ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:sys.path.insert(0,PROJECT_ROOT)
from common.communication import send_json
from common.config import NODE_HOST,NODE2_PORT


def percentile(values,p):
    if not values:return 0.0
    ordered=sorted(values); index=min(len(ordered)-1,max(0,int(round((p/100)*(len(ordered)-1)))))
    return ordered[index]


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--transactions",type=int,default=50)
    parser.add_argument("--workers",type=int,default=8)
    args=parser.parse_args()
    if args.transactions<1 or args.workers<1:raise SystemExit("transactions and workers must be positive")
    run_id=str(time.time_ns())
    def one(i):
        txid=f"A3-LOAD-{run_id}-{i:06d}"
        payload={"type":"TX_BILL","transaction_id":txid,"request_id":i,
                 "customer_id":f"LOAD-{i%1000:04d}","service":["data","voice","sms"][i%3],
                 "usage":1+(i%50)}
        start=time.perf_counter()
        try:
            result=send_json(NODE_HOST,NODE2_PORT,payload,timeout=30)
            return {"transaction_id":txid,"status":result.get("status","ERROR"),
                    "latency_ms":round((time.perf_counter()-start)*1000,3),"error":result.get("error","")}
        except Exception as exc:
            return {"transaction_id":txid,"status":"ERROR","latency_ms":round((time.perf_counter()-start)*1000,3),"error":str(exc)}
    started=time.perf_counter(); rows=[]
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures=[pool.submit(one,i) for i in range(1,args.transactions+1)]
        for future in as_completed(futures):rows.append(future.result())
    elapsed=time.perf_counter()-started
    latencies=[r["latency_ms"] for r in rows]
    committed=sum(r["status"]=="COMMIT" for r in rows)
    aborted=sum(r["status"]=="ABORT" for r in rows)
    errors=sum(r["status"] not in ("COMMIT","ABORT") for r in rows)
    metrics={"submitted":args.transactions,"workers":args.workers,"elapsed_s":round(elapsed,4),
             "committed":committed,"aborted":aborted,"errors_or_in_doubt":errors,
             "completed_throughput_tx_s":round(len(rows)/elapsed,3) if elapsed else 0,
             "successful_commit_throughput_tx_s":round(committed/elapsed,3) if elapsed else 0,
             "mean_latency_ms":round(sum(latencies)/len(latencies),3) if latencies else 0,
             "p95_latency_ms":percentile(latencies,95),"max_latency_ms":max(latencies) if latencies else 0}
    os.makedirs("results",exist_ok=True)
    with open("results/week5_load_test.csv","w",newline="") as f:
        writer=csv.DictWriter(f,fieldnames=["transaction_id","status","latency_ms","error"]);writer.writeheader();writer.writerows(rows)
    with open("results/week5_load_test_summary.json","w") as f:json.dump({"metrics":metrics,"results":rows},f,indent=2)
    print(json.dumps(metrics,indent=2))
    print("Saved results/week5_load_test.csv and results/week5_load_test_summary.json")

if __name__=="__main__":main()
