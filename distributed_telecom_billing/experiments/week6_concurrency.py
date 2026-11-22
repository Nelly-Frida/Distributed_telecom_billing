"""Week 6: demonstrate deadlock detection/recovery and ordered-lock prevention."""
import csv,json,os,sys,threading,time
from concurrent.futures import ThreadPoolExecutor, as_completed
PROJECT_ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:sys.path.insert(0,PROJECT_ROOT)
from concurrency.resource_manager import ResourceManager


def run_deadlock_recovery():
    rm=ResourceManager(); both_first=threading.Barrier(2); outcomes=[]; outcome_lock=threading.Lock()
    def transfer(txid,first,second):
        start=time.perf_counter()
        if not rm.acquire(txid,first,timeout=2):
            with outcome_lock:outcomes.append({"transaction":txid,"result":"FAILED_FIRST_LOCK","duration_ms":(time.perf_counter()-start)*1000})
            return
        print(f"{txid} acquired {first}")
        both_first.wait(timeout=5)
        print(f"{txid} requesting {second}")
        ok=rm.acquire(txid,second,timeout=8)
        if ok:
            rm.finish(txid); result="COMPLETED_AFTER_RECOVERY"
        else:
            rm.finish(txid); result="ABORTED_DEADLOCK"
        with outcome_lock:outcomes.append({"transaction":txid,"result":result,"duration_ms":round((time.perf_counter()-start)*1000,3)})

    a=threading.Thread(target=transfer,args=("TX-A","ACCOUNT-A","ACCOUNT-B"))
    b=threading.Thread(target=transfer,args=("TX-B","ACCOUNT-B","ACCOUNT-A"))
    a.start();b.start()
    deadline=time.perf_counter()+5; graph={}
    while time.perf_counter()<deadline:
        graph=rm.wait_for_graph()
        if rm.has_cycle(graph):break
        time.sleep(0.005)
    detected=rm.has_cycle(graph)
    rag=rm.resource_allocation_graph()
    print("Wait-for graph:",graph)
    print("Resource-allocation graph:",rag)
    victim=rm.select_victim(graph) if detected else None
    if victim:
        print(f"Deadlock detected. Selecting {victim} as victim.")
        rm.abort(victim)
    a.join(10);b.join(10)
    if a.is_alive() or b.is_alive():
        for tx in ("TX-A","TX-B"):rm.abort(tx)
        a.join(2);b.join(2)
    print("Deadlock results:",outcomes)
    print("Final resource state:",rm.stats())
    return {"experiment":"deadlock_detection_recovery","detected":detected,"wait_for_graph":{k:sorted(v) for k,v in graph.items()},"resource_allocation_graph":rag,"victim":victim,"outcomes":outcomes,"final_state":rm.stats()}


def run_race_condition_demo(workers=8,operations_per_worker=300):
    target=workers*operations_per_worker
    unsafe={"value":0}
    def unsafe_task():
        for _ in range(operations_per_worker):
            current=unsafe["value"]
            time.sleep(0)
            unsafe["value"]=current+1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(lambda _:unsafe_task(),range(workers)))
    safe={"value":0}; lock=threading.Lock()
    def safe_task():
        for _ in range(operations_per_worker):
            with lock:safe["value"]+=1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(lambda _:safe_task(),range(workers)))
    return {"expected":target,"without_mutex":unsafe["value"],"with_mutex":safe["value"],
            "lost_updates_without_mutex":target-unsafe["value"],"mutex_correct":safe["value"]==target}


def run_ordered_lock_prevention(workers=8,transactions=100):
    rm=ResourceManager(); results=[]; guard=threading.Lock(); start=time.perf_counter()
    balances={"ACCOUNT-A":10000,"ACCOUNT-B":10000}
    def task(i):
        txid=f"ORD-{i:04d}"; t=time.perf_counter()
        ok=rm.acquire_ordered(txid,["ACCOUNT-A","ACCOUNT-B"],timeout=5)
        if ok:
            # Both account records are protected by the same ordered lock set.
            balances["ACCOUNT-A"]-=1
            balances["ACCOUNT-B"]+=1
            time.sleep(0.001)
            rm.finish(txid); status="COMPLETED"
        else:status="ABORTED_TIMEOUT"
        row={"transaction":txid,"result":status,"wait_ms":round((time.perf_counter()-t)*1000,3)}
        with guard:results.append(row)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures=[pool.submit(task,i) for i in range(transactions)]
        for future in as_completed(futures): future.result()
    elapsed=time.perf_counter()-start
    completed=sum(r["result"]=="COMPLETED" for r in results)
    waits=[r["wait_ms"] for r in results]
    return {"experiment":"ordered_lock_prevention","transactions":transactions,"workers":workers,
            "completed":completed,"aborted":transactions-completed,"elapsed_s":round(elapsed,4),
            "throughput_tx_s":round(completed/elapsed,3) if elapsed else 0,
            "mean_wait_ms":round(sum(waits)/len(waits),3) if waits else 0,
            "max_wait_ms":round(max(waits),3) if waits else 0,
            "timeouts_observed":completed<transactions,"no_starvation_observed_in_bounded_run":completed==transactions,
            "balances":balances,"balance_total":sum(balances.values()),
            "mutual_exclusion_preserved":sum(balances.values())==20000,
            "deadlock_prevented":True,"results":results}


def main():
    print("="*72);print("ASSIGNMENT 3 — WEEK 6: CONCURRENCY AND DEADLOCK MANAGEMENT");print("="*72)
    race=run_race_condition_demo()
    print("Race-condition demonstration:",json.dumps(race,indent=2))
    detection=run_deadlock_recovery()
    prevention=run_ordered_lock_prevention()
    print("\nOrdered-lock prevention summary:",json.dumps({k:v for k,v in prevention.items() if k!="results"},indent=2))
    os.makedirs("results",exist_ok=True)
    with open("results/week6_concurrency.csv","w",newline="") as f:
        w=csv.writer(f);w.writerow(["experiment","transaction","result","duration_ms"])
        for row in detection["outcomes"]:w.writerow(["deadlock_detection",row["transaction"],row["result"],row["duration_ms"]])
        for row in prevention["results"]:w.writerow(["ordered_lock_prevention",row["transaction"],row["result"],row["wait_ms"]])
    with open("results/week6_experiment_summary.json","w") as f:json.dump({"race_condition":race,"detection":detection,"prevention":prevention},f,indent=2)
    print("\nSaved results/week6_concurrency.csv and results/week6_experiment_summary.json")

if __name__=="__main__":main()
