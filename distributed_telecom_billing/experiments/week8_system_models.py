"""Week 8: which distributed-system model fits a telecom billing platform?

The same workload (voice 50 %, SMS 30 %, data 20 %; service demand 20 / 5 / 40 ms)
is executed under five models, using the real services of this project:

  host_based      every client sends its work to ONE central host (NODE-3)
  server_based    dedicated servers per function: voice->NODE-3, sms->NODE-4,
                  data->NODE-5 (static partition, NODE-6 not provisioned)
  processor_pool  clients submit to NODE-2, which allocates each request to any
                  free node of a pool of four (NODE-3..6). Two policies are run:
                  pool_round_robin (baseline pool) and pool_least_loaded
                  (the optimisation tested in this milestone)
  workstation     every client processes its own work locally; no sharing
  integrated      hybrid: light requests (<= 10 ms of work) run on the client's own
                  workstation, heavy ones go to the pool (least_loaded)

Hypotheses
  H8-1  Throughput of host_based is capped by one node and of server_based by its
        busiest server; pooled models scale until the pool is saturated.
  H8-2  least_loaded improves tail latency over round_robin when service demands
        differ, at no extra provisioned cost.
  H8-3  workstation scales with the number of clients but wastes idle capacity and
        costs most; it has no shared capacity to absorb a spike.
  H8-4  Under one component failure only models with failover keep serving fully.

Design
  Independent variables : model, number of clients (1,2,4,8,16)
  Dependent variables   : throughput, mean/p95/p99 latency, queueing delay,
                          utilisation, scalability, reliability under a fault, cost
  Controlled variables  : request mix and sequence (per-client seed), service demand,
                          node capacity (NODE_SERVICE_SLOTS slots per node, emulated
                          so results do not depend on the test machine's core count),
                          run length and warm-up

Utilisation is busy time / available time of the provisioned units (nodes and, for
workstation and integrated, the client workstations). Cost uses ILLUSTRATIVE unit
prices (see --cost-config); replace them with your own cloud or hardware prices.

    python experiments/week8_system_models.py
    python experiments/week8_system_models.py --quick
"""
import argparse, csv, json, os, platform, random, statistics, sys, threading, time
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.communication import send_json
from common.config import NODE2_PORT, NODE_HOST, RATING_NODES, RESULTS_DIR
from experiments.ft_metrics import availability, bucketize, percentile, summarize_rows
from rating.rating_engine import RatingEngine

try:
    import psutil
except ImportError:  # optional
    psutil = None

ENGINE = RatingEngine()
WORK_MS = {"voice": 20.0, "sms": 5.0, "data": 40.0}
MIX = (("voice", 0.5), ("sms", 0.3), ("data", 0.2))
SERVER_MAP = {"voice": "NODE-3", "sms": "NODE-4", "data": "NODE-5"}
POOL = ["NODE-3", "NODE-4", "NODE-5", "NODE-6"]

MODELS = {
    "host_based":          {"label": "Host-based (one central host)",          "nodes": ["NODE-3"],           "workstations": False, "dispatcher": False},
    "server_based":        {"label": "Server-based (dedicated servers)",       "nodes": ["NODE-3", "NODE-4", "NODE-5"], "workstations": False, "dispatcher": False},
    "pool_round_robin":    {"label": "Processor pool (round robin)",           "nodes": POOL,                 "workstations": False, "dispatcher": True},
    "pool_least_loaded":   {"label": "Processor pool (least loaded)",          "nodes": POOL,                 "workstations": False, "dispatcher": True},
    "workstation":         {"label": "Workstation (local processing)",         "nodes": [],                   "workstations": True,  "dispatcher": False},
    "integrated":          {"label": "Integrated (local light + pool heavy)",  "nodes": POOL,                 "workstations": True,  "dispatcher": True},
}

DEFAULT_COSTS = {  # illustrative USD per hour; replace with real prices
    "node_per_hour": 0.10, "dispatcher_per_hour": 0.10,
    "workstation_per_hour": 0.05, "terminal_per_hour": 0.01,
}

# Qualitative gate (a design judgement, NOT a measurement): billing needs one authoritative
# ledger and the Week 5 transactions. The emulated rating step is stateless, so the numeric
# comparison cannot see this requirement; it is therefore applied as a pass/fail gate.
GATES = {
    "workstation": ("fail", "no shared central state: every workstation would need its own copy of rating plans and "
                            "balances, and the ledger/2PC of Week 5 requires one authoritative store"),
}

PROFILES = {  # decision weights (judgement calls, not measurements)
    "balanced":             {"latency": 0.20, "throughput": 0.20, "utilization": 0.10, "scalability": 0.20, "reliability": 0.20, "cost": 0.10},
    "telecom_availability": {"latency": 0.15, "throughput": 0.15, "utilization": 0.05, "scalability": 0.15, "reliability": 0.35, "cost": 0.15},
}


# ----------------------------------------------------------------------------
def remote_call(addr, msg, timeout=30.0):
    return send_json(addr[0], addr[1], msg, timeout=timeout)


def execute(model, cid, req, local_threshold_ms):
    """Run one request under `model`. Returns (ok, charge, served_by, local_busy_s)."""
    def to_node(name):
        reply = remote_call(RATING_NODES[name], req)
        return ("error" not in reply), reply.get("charge"), reply.get("node", name), 0.0

    def to_pool(policy):
        reply = remote_call((NODE_HOST, NODE2_PORT), {**req, "ft_mode": "resilient", "policy": policy})
        return ("error" not in reply), reply.get("charge"), reply.get("selected_node"), 0.0

    def local():
        began = time.perf_counter()
        time.sleep(req["work_ms"] / 1000.0)
        charge = ENGINE.calculate_charge(req["service"], req["usage"])
        return True, charge, f"WS-{cid}", time.perf_counter() - began

    if model == "host_based":
        return to_node("NODE-3")
    if model == "server_based":
        return to_node(SERVER_MAP[req["service"]])
    if model == "pool_round_robin":
        return to_pool("round_robin")
    if model == "pool_least_loaded":
        return to_pool("least_loaded")
    if model == "workstation":
        return local()
    if model == "integrated":
        return local() if req["work_ms"] <= local_threshold_ms else to_pool("least_loaded")
    raise ValueError(model)


def next_request(rng, cid, n):
    service = rng.choices([s for s, _ in MIX], weights=[w for _, w in MIX])[0]
    usage = rng.randint(1, 50)
    return {"request_id": n, "customer_id": f"W8-{cid}", "service": service, "usage": usage,
            "work_ms": WORK_MS[service]}, ENGINE.rates[service] * usage


def reset_nodes(names):
    for n in names:
        try:
            remote_call(RATING_NODES[n], {"type": "NODE_STATS", "reset": True}, timeout=3)
        except Exception:
            pass


def read_nodes(names):
    out = {}
    for n in names:
        try:
            out[n] = remote_call(RATING_NODES[n], {"type": "NODE_STATS"}, timeout=3)
        except Exception:
            out[n] = None
    return out


def wait_healthy(max_wait=45.0):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            for n in RATING_NODES:
                remote_call(RATING_NODES[n], {"type": "FAULT_CLEAR"}, timeout=1.5)
            st = remote_call((NODE_HOST, NODE2_PORT), {"type": "FT_STATUS"}, timeout=3)
            if all(v["state"] == "UP" for k, v in st["detector"]["nodes"].items() if k.startswith("NODE")):
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def run_load(model, clients, duration, warmup, seed, local_threshold_ms, paced_rate=None, fault=None):
    """Closed loop (paced_rate=None) or paced load. fault = dict(start_s, end_s, node, failed_clients)."""
    if not wait_healthy():
        raise SystemExit("System not healthy; start it with `python launcher.py` or docker compose.")
    spec = MODELS[model]
    time.sleep(0.5)
    rows, lock, stop = [], threading.Lock(), threading.Event()
    t0 = time.time()

    def client(cid):
        rng = random.Random(seed * 1000 + cid)
        n, period = 0, (1.0 / paced_rate if paced_rate else 0.0)
        next_at = time.time()
        while not stop.is_set():
            n += 1
            req, expected = next_request(rng, cid, n)
            started = time.time()
            elapsed_s = started - t0
            in_fault = bool(fault and fault["start_s"] <= elapsed_s < fault["end_s"])
            try:
                if in_fault and cid in fault.get("failed_clients", ()):
                    raise ConnectionError("workstation failed")
                ok, charge, served, busy = execute(model, cid, req, local_threshold_ms)
                correct = bool(ok and charge is not None and abs(charge - expected) < 1e-6)
            except Exception as exc:
                ok, correct, served, busy = False, False, f"ERR:{type(exc).__name__}", 0.0
            ended = time.time()
            with lock:
                rows.append({"model": model, "clients": clients, "client": cid, "service": req["service"],
                             "load_type": "fixed_rate" if paced_rate else "saturation",
                             "warmup_s": warmup, "duration_s": duration,
                             "t_start_s": round(started - t0, 4), "t_end_s": round(ended - t0, 4),
                             "ok": ok, "correct": correct, "latency_ms": round((ended - started) * 1000, 3),
                             "served_by": served, "local_busy_s": round(busy, 5)})
            if paced_rate:
                next_at += period
                time.sleep(max(0.0, next_at - time.time()))

    cpu_samples = []

    def sampler():
        while not stop.is_set():
            if psutil and time.time() - t0 >= warmup:
                cpu_samples.append((psutil.cpu_percent(interval=None), psutil.virtual_memory().percent))
            time.sleep(0.5)

    threads = [threading.Thread(target=client, args=(c,), daemon=True) for c in range(clients)]
    for t in threads:
        t.start()
    if psutil:
        psutil.cpu_percent(interval=None)
        threading.Thread(target=sampler, daemon=True).start()

    fault_thread = None
    if fault and fault.get("node"):
        def inject():
            time.sleep(max(0.0, fault["start_s"] - (time.time() - t0)))
            try:
                remote_call(RATING_NODES[fault["node"]], {"type": "FAULT_INJECT", "mode": "drop",
                            "duration_s": fault["end_s"] - fault["start_s"]}, timeout=3)
            except Exception:
                pass
        fault_thread = threading.Thread(target=inject, daemon=True)
        fault_thread.start()

    time.sleep(warmup)
    reset_nodes(spec["nodes"])
    window_start = time.time()
    time.sleep(max(0.0, duration - warmup))
    snap = read_nodes(spec["nodes"])
    window = time.time() - window_start
    stop.set()
    for t in threads:
        t.join(timeout=35)
    for n in RATING_NODES:  # leave nodes clean for the next run
        try:
            remote_call(RATING_NODES[n], {"type": "FAULT_CLEAR"}, timeout=2)
        except Exception:
            pass
    return rows, snap, window, cpu_samples, time.time() - t0


# ----------------------------------------------------------------------------
def provisioned_cost(model, clients, costs):
    spec = MODELS[model]
    total = len(spec["nodes"]) * costs["node_per_hour"]
    total += costs["dispatcher_per_hour"] if spec["dispatcher"] else 0.0
    total += clients * (costs["workstation_per_hour"] if spec["workstations"] else costs["terminal_per_hour"])
    return round(total, 4)


def measure(model, clients, args, costs, paced_rate=None):
    rows, snap, window, cpu, _ = run_load(model, clients, args.duration, args.warmup, args.seed,
                                          args.local_threshold_ms, paced_rate=paced_rate)
    scored = [r for r in rows if r["t_start_s"] >= args.warmup and r["t_end_s"] <= args.duration + 0.5]
    summary = summarize_rows(scored)
    good = [r for r in scored if r["ok"] and r["correct"]]
    lat = [r["latency_ms"] for r in good] or [0.0]
    spec = MODELS[model]
    remote_busy = sum((s or {}).get("busy_s", 0.0) for s in snap.values())
    remote_units = len(spec["nodes"])
    local_busy = sum(r["local_busy_s"] for r in scored)
    local_units = clients if spec["workstations"] else 0
    # remote busy counts only work done in the window; local work is counted from request rows in the window
    util_den = (remote_units + local_units) * window
    utilization = 100.0 * (remote_busy + local_busy) / util_den if util_den else 0.0
    node_utils = [(s or {}).get("utilization", 0.0) for s in snap.values()]
    waits = [(s or {}).get("mean_wait_ms", 0.0) for s in snap.values()]
    per_node = {}
    for r in good:
        per_node[r["served_by"]] = per_node.get(r["served_by"], 0) + 1
    counts = [per_node.get(n, 0) for n in spec["nodes"]] if spec["nodes"] else []
    cv = (statistics.pstdev(counts) / statistics.mean(counts)) if counts and statistics.mean(counts) else 0.0
    throughput = len(good) / window if window else 0.0
    hourly = provisioned_cost(model, clients, costs)
    return {
        "model": model, "clients": clients, "load_type": "fixed_rate" if paced_rate else "saturation",
        "offered_rps": round(clients * paced_rate, 1) if paced_rate else None,
        "window_s": round(window, 2), "requests": summary["requests"],
        "success_pct": summary["success_pct"], "throughput_rps": round(throughput, 2),
        "mean_latency_ms": round(sum(lat) / len(lat), 2), "p50_latency_ms": round(percentile(lat, 50), 2),
        "p95_latency_ms": round(percentile(lat, 95), 2), "p99_latency_ms": round(percentile(lat, 99), 2),
        "node_queue_wait_ms": round(sum(waits) / len(waits), 2) if waits else 0.0,
        "utilization_pct": round(utilization, 2),
        "node_util_mean_pct": round(100 * sum(node_utils) / len(node_utils), 2) if node_utils else 0.0,
        "node_util_max_pct": round(100 * max(node_utils), 2) if node_utils else 0.0,
        "load_imbalance_cv": round(cv, 3),
        "host_cpu_pct": round(sum(c for c, _ in cpu) / len(cpu), 1) if cpu else None,
        "host_mem_pct": round(sum(m for _, m in cpu) / len(cpu), 1) if cpu else None,
        "provisioned_nodes": remote_units, "provisioned_workstations": local_units,
        "cost_per_hour_usd": hourly,
        "cost_per_1000_requests_usd": round(hourly / (throughput * 3600) * 1000, 5) if throughput else None,
    }, rows


def measure_reliability(model, args):
    n = args.rel_clients
    total = args.rel_pre + args.rel_fault + args.rel_post
    fault = {"start_s": args.rel_pre, "end_s": args.rel_pre + args.rel_fault, "node": "NODE-3",
             "failed_clients": set()}
    if model == "workstation":      # the failing component is one user's own machine
        fault["node"] = None
        fault["failed_clients"] = {0}
    rows, _, _, _, elapsed = run_load(model, n, total, 0.5, args.seed, args.local_threshold_ms,
                                      paced_rate=args.rel_rate, fault=fault)
    for r in rows:
        r["phase"] = "pre" if r["t_start_s"] < fault["start_s"] else ("fault" if r["t_start_s"] < fault["end_s"] else "post")
    during = [r for r in rows if r["phase"] == "fault"]
    buckets = bucketize(rows, total, width=0.5)   # planned run length, not shutdown time
    return {
        "model": model, "clients": n, "failed_component": "workstation 0" if model == "workstation" else "NODE-3",
        "offered_requests_in_fault": len(during),
        "fault_success_pct": summarize_rows(during)["success_pct"],
        "availability_pct": round(100 * availability(buckets), 2),
        "availability_during_fault_pct": round(100 * (availability(buckets, fault["start_s"], fault["end_s"]) or 0), 2),
        "fault_mean_latency_ms": summarize_rows(during)["mean_latency_ms"],
    }


def util_score(util_pct, target):
    """1.0 at the capacity-planning target; falls off for waste (under) and overload (over)."""
    return max(0.0, 1.0 - abs(util_pct - target) / target)


def decision_matrix(sat_runs, fixed_runs, reliability, ref_clients, max_clients, target_util):
    sat_ref = {r["model"]: r for r in sat_runs if r["clients"] == ref_clients}
    sat_one = {r["model"]: r for r in sat_runs if r["clients"] == min(x["clients"] for x in sat_runs)}
    sat_top = {r["model"]: r for r in sat_runs if r["clients"] == max_clients}
    fixed = {r["model"]: r for r in fixed_runs}
    rel = {r["model"]: r for r in reliability}
    raw = {}
    for m in sat_ref:
        base = sat_one[m]["throughput_rps"] or 1e-9
        raw[m] = {"latency": fixed[m]["p95_latency_ms"],             # at the fixed offered load
                  "throughput": sat_ref[m]["throughput_rps"],        # capacity at saturation
                  "utilization": fixed[m]["utilization_pct"],        # at the fixed offered load
                  "scalability": round(sat_top[m]["throughput_rps"] / base, 3),
                  "reliability": rel[m]["fault_success_pct"],
                  "cost": fixed[m]["cost_per_1000_requests_usd"] or 1e9}

    def norm(metric, value, lower_better):
        vals = [raw[m][metric] for m in raw]
        lo, hi = min(vals), max(vals)
        if hi == lo:
            return 1.0
        x = (value - lo) / (hi - lo)
        return 1.0 - x if lower_better else x

    out = []
    for m in raw:
        row = {"model": m, **{f"raw_{k}": v for k, v in raw[m].items()}}
        for k in raw[m]:
            row[f"norm_{k}"] = (round(util_score(raw[m][k], target_util), 3) if k == "utilization"
                                else round(norm(k, raw[m][k], k in ("latency", "cost")), 3))
        for pname, w in PROFILES.items():
            row[f"score_{pname}"] = round(sum(w[k] * row[f"norm_{k}"] for k in w), 4)
        out.append(row)
    for r in out:
        gate, reason = GATES.get(r["model"], ("pass", "central ledger and coordinator remain authoritative"))
        r["gate_central_state"], r["gate_reason"] = gate, reason
    for pname in PROFILES:
        for i, r in enumerate(sorted(out, key=lambda r: -r[f"score_{pname}"]), 1):
            r[f"rank_{pname}"] = i
        passing = sorted([r for r in out if r["gate_central_state"] == "pass"], key=lambda r: -r[f"score_{pname}"])
        for r in out:
            r[f"rank_{pname}_after_gate"] = None
        for i, r in enumerate(passing, 1):
            r[f"rank_{pname}_after_gate"] = i
    return sorted(out, key=lambda r: r["rank_balanced"])


def write_csv(path, rows, fields=None):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields or list(rows[0].keys()), extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--models", default="all")
    p.add_argument("--clients", default="1,2,4,8,16")
    p.add_argument("--duration", type=float, default=7.0, help="seconds per run, including warm-up")
    p.add_argument("--warmup", type=float, default=1.5)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--local-threshold-ms", type=float, default=10.0)
    p.add_argument("--rel-clients", type=int, default=8)
    p.add_argument("--rel-rate", type=float, default=5.0, help="paced requests/s per client in the fault test")
    p.add_argument("--rel-pre", type=float, default=3.0)
    p.add_argument("--rel-fault", type=float, default=4.0)
    p.add_argument("--rel-post", type=float, default=3.0)
    p.add_argument("--ref-clients", type=int, default=8, help="client count used for the decision matrix")
    p.add_argument("--load-rate", type=float, default=10.0,
                   help="fixed offered load per client (req/s) for the utilisation, latency and cost comparison")
    p.add_argument("--target-util", type=float, default=70.0, help="capacity-planning utilisation target (%%)")
    p.add_argument("--cost-config", default=None, help="JSON file overriding the illustrative unit prices")
    p.add_argument("--quick", action="store_true")
    args = p.parse_args()
    if args.quick:
        args.clients, args.duration, args.warmup = "1,4,8", 4.0, 1.0
        args.rel_pre, args.rel_fault, args.rel_post = 2.0, 3.0, 2.0
    clients_list = [int(x) for x in args.clients.split(",")]
    if args.ref_clients not in clients_list:
        args.ref_clients = clients_list[-1]
    names = list(MODELS) if args.models == "all" else [m.strip() for m in args.models.split(",")]
    costs = dict(DEFAULT_COSTS)
    if args.cost_config:
        costs.update(json.load(open(args.cost_config)))

    os.makedirs(RESULTS_DIR, exist_ok=True)
    print("=" * 84)
    print("WEEK 8 - DISTRIBUTED SYSTEM MODELS: host / server / processor pool / workstation / integrated")
    print("=" * 84)
    print(f"Service demand ms: {WORK_MS}   mix: {dict(MIX)}   unit prices (illustrative): {costs}")

    runs, all_rows = [], []
    print("\n[A] Capacity sweep: closed-loop clients with no think time (saturation)")
    for m in names:
        print(f"\n>>> {MODELS[m]['label']}")
        for n in clients_list:
            result, rows = measure(m, n, args, costs)
            runs.append(result)
            all_rows.extend(rows)
            print(f"    clients={n:<3} throughput={result['throughput_rps']:>7} rps  mean={result['mean_latency_ms']:>8} ms  "
                  f"p95={result['p95_latency_ms']:>8} ms  node_util={result['node_util_mean_pct']:>5}%  success={result['success_pct']}%")

    print(f"\n[B] Fixed offered load: {args.ref_clients} clients x {args.load_rate} req/s = "
          f"{args.ref_clients * args.load_rate:.0f} req/s for every model")
    fixed_runs = []
    for m in names:
        result, rows = measure(m, args.ref_clients, args, costs, paced_rate=args.load_rate)
        fixed_runs.append(result)
        all_rows.extend(rows)
        print(f"    {m:<18} served={result['throughput_rps']:>6} rps  p95={result['p95_latency_ms']:>8} ms  "
              f"utilisation={result['utilization_pct']:>5}%  cost/1000=${result['cost_per_1000_requests_usd']}")

    print("\n[C] Reliability: one component fails mid-run (paced load)")
    reliability = []
    for m in names:
        r = measure_reliability(m, args)
        reliability.append(r)
        print(f"    {m:<18} failed={r['failed_component']:<13} success_in_fault={r['fault_success_pct']:>6}%  "
              f"availability={r['availability_pct']:>6}%  (during fault {r['availability_during_fault_pct']}%)")

    matrix = decision_matrix(runs, fixed_runs, reliability, args.ref_clients, max(clients_list), args.target_util) if len(names) > 1 else []
    run_fields = list(runs[0].keys())
    write_csv(os.path.join(RESULTS_DIR, "week8_runs.csv"), runs + fixed_runs, run_fields)
    write_csv(os.path.join(RESULTS_DIR, "week8_reliability.csv"), reliability)
    write_csv(os.path.join(RESULTS_DIR, "week8_requests.csv"), all_rows,
              ["model", "clients", "load_type", "warmup_s", "duration_s", "client", "service", "t_start_s", "t_end_s",
               "ok", "correct", "latency_ms", "served_by", "local_busy_s"])
    if matrix:
        write_csv(os.path.join(RESULTS_DIR, "week8_decision_matrix.csv"), matrix)
    env = {"platform": platform.platform(), "python": platform.python_version(), "cpu_count": os.cpu_count(),
           "in_docker": os.path.exists("/.dockerenv"), "date": time.strftime("%Y-%m-%d %H:%M:%S"),
           "args": vars(args), "costs": costs, "work_ms": WORK_MS, "mix": dict(MIX), "profiles": PROFILES}
    with open(os.path.join(RESULTS_DIR, "week8_experiment_summary.json"), "w") as f:
        json.dump({"environment": env, "runs": runs, "fixed_load_runs": fixed_runs, "reliability": reliability, "decision_matrix": matrix}, f, indent=2, default=str)

    if matrix:
        print("\n" + "=" * 84)
        print(f"DECISION MATRIX at {args.ref_clients} clients (weights are judgement calls; see week8_experiment_summary.json)")
        print(f"{'model':<20}{'p95 ms':>9}{'cap rps':>9}{'util%':>7}{'scale x':>9}{'rel%':>7}{'$/1000':>9}{'balanced':>10}{'telecom':>9}  gate")
        for r in matrix:
            print(f"{r['model']:<20}{r['raw_latency']:>9}{r['raw_throughput']:>9}{r['raw_utilization']:>7}{r['raw_scalability']:>9}"
                  f"{r['raw_reliability']:>7}{r['raw_cost']:>9}{r['score_balanced']:>8} #{r['rank_balanced']}{r['score_telecom_availability']:>8} #{r['rank_telecom_availability']}  {r['gate_central_state']}")
        for pname in PROFILES:
            best = next(r for r in matrix if r[f"rank_{pname}_after_gate"] == 1)
            print(f"Recommended ({pname}, after the central-state gate): {best['model']}")
    print("\nSaved: results/week8_runs.csv, week8_reliability.csv, week8_requests.csv, week8_decision_matrix.csv, week8_experiment_summary.json")


if __name__ == "__main__":
    main()
