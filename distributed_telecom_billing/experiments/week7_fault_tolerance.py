"""Week 7: fault injection, failure detection, failover and recovery.

Hypothesis
    H7-1  With a failure detector, immediate failover and a replicated ledger,
          a single node failure is masked from clients (availability close to
          100 %), whereas the baseline loses every request routed to the
          failed component for as long as it is down.
    H7-2  Failover needs one extra attempt for the first request that hits the
          dead node and nothing afterwards, because the node is then suspected.
    H7-3  A Byzantine node cannot be detected by crash-oriented failover; only
          majority voting catches it, at the price of 3x the work per request.
    H7-4  After a ledger replica or the coordinator restarts, recovery brings
          all durable state back to a consistent view with no stuck transaction.

Design
    Independent variable : system mode (baseline | resilient | resilient_verify)
                           and fault type (crash, omission, communication,
                           service error, Byzantine, ledger crash, coordinator crash)
    Dependent variables  : availability, MTTR, detection time, request success,
                           wrong results, latency, consistency after recovery
    Controlled variables : closed-loop client count, request mix, random seed,
                           fault timing, node set, timeouts (recorded in the JSON)

Every scenario runs in three phases (pre-fault, fault, post-fault) against the
real distributed services. Requires `python launcher.py` (supervised) or the
Docker Compose stack, because a crashed node must be restarted for recovery to
be measured.

    python experiments/week7_fault_tolerance.py
    python experiments/week7_fault_tolerance.py --quick
    python experiments/week7_fault_tolerance.py --scenarios rating_crash,ledger_crash
"""
import argparse, csv, json, os, platform, random, sys, threading, time
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.communication import send_json
from common.config import LEDGER_REPLICAS, NODE2_PORT, NODE_HOST, RATING_NODES, RESULTS_DIR
from experiments.ft_metrics import availability, bucketize, mttr, summarize_rows
from rating.rating_engine import RatingEngine

RATES = RatingEngine().rates

SCENARIOS = {
    "rating_crash":       dict(kind="rating", target="NODE-4", fault={"mode": "crash"},
                               label="Node failure (rating node crashes)", modes=["baseline", "resilient"]),
    "rating_omission":    dict(kind="rating", target="NODE-4", fault={"mode": "omission", "hold_s": 20},
                               label="Omission failure (node accepts, never answers)", modes=["baseline", "resilient"]),
    "rating_drop":        dict(kind="rating", target="NODE-4", fault={"mode": "drop"},
                               label="Communication failure (connections dropped)", modes=["baseline", "resilient"]),
    "rating_error":       dict(kind="rating", target="NODE-4", fault={"mode": "error"},
                               label="Service failure (node answers with errors)", modes=["baseline", "resilient"]),
    "rating_byzantine":   dict(kind="rating", target="NODE-4", fault={"mode": "byzantine", "factor": 1.25},
                               label="Byzantine failure (wrong charges, no error)",
                               modes=["baseline", "resilient", "resilient_verify"]),
    "tx_rating_crash":    dict(kind="tx", target="NODE-3", fault={"mode": "crash"},
                               label="Rating node crash during 2PC load (failover under a new transaction id)",
                               modes=["baseline", "resilient"]),
    "ledger_crash":       dict(kind="tx", target="LEDGER-1", fault={"mode": "crash"},
                               label="Ledger primary crash during 2PC load", modes=["baseline", "resilient"]),
    "coordinator_crash":  dict(kind="tx", target="NODE-2", fault={"mode": "crash"},
                               label="2PC coordinator (NODE-2) crash", modes=["resilient"]),
}


def address(name):
    if name == "NODE-2":
        return NODE_HOST, NODE2_PORT
    if name in RATING_NODES:
        return RATING_NODES[name]
    return dict(LEDGER_REPLICAS)[name]


def call(addr, message, timeout=5.0):
    return send_json(addr[0], addr[1], message, timeout=timeout)


def wait_healthy(max_wait=45.0):
    """All nodes answering and UP in the detector, with no fault active."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            for name in list(RATING_NODES) + [n for n, _ in LEDGER_REPLICAS] + ["NODE-2"]:
                call(address(name), {"type": "FAULT_CLEAR"}, timeout=1.5)
            status = call(address("NODE-2"), {"type": "FT_STATUS"}, timeout=3)
            if all(v["state"] == "UP" for v in status["detector"]["nodes"].values()):
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def sync_ledgers(max_wait=90.0):
    """Controlled starting state: both ledger replicas identical and no pending 2PC.

    The baseline architecture writes only to the primary ledger, so a baseline run leaves
    the replica behind; without this step that lag would leak into the next scenario."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            call(address("NODE-2"), {"type": "TX_RESYNC"}, timeout=60)
            call(address("NODE-2"), {"type": "TX_RECOVER"}, timeout=60)
            report = call(address("NODE-2"), {"type": "TX_CONSISTENCY"}, timeout=30)
            if report.get("consistent"):
                return True
        except Exception:
            pass
        time.sleep(1.0)
    return False


def run_scenario(name, mode, args, run_id, rep=1):
    sc = SCENARIOS[name]
    ft_mode = "baseline" if mode == "baseline" else "resilient"
    if not wait_healthy():
        raise SystemExit("System did not become healthy; is `python launcher.py` (or docker compose) running?")
    if sc["kind"] == "tx" and not sync_ledgers():
        raise SystemExit("Ledger replicas could not be brought into sync before the scenario.")
    time.sleep(1.0)

    rows, lock, stop = [], threading.Lock(), threading.Event()
    total_s = args.pre + args.fault + args.post
    t0 = time.time()
    target_addr = address(sc["target"])

    def client(cid):
        rng = random.Random(args.seed * 1000 + cid)
        n = 0
        while not stop.is_set():
            n += 1
            service = rng.choice(["voice", "sms", "data"])
            usage = rng.randint(1, 50)
            expected = RATES[service] * usage
            txid = f"W7-{run_id}-{name}-{mode}-r{rep}-{cid}-{n}"
            if sc["kind"] == "rating":
                msg = {"request_id": n, "customer_id": f"W7-{cid}", "service": service, "usage": usage, "ft_mode": ft_mode}
                if mode == "resilient_verify":
                    msg["ft_verify"] = True
            else:
                msg = {"type": "TX_BILL", "transaction_id": txid, "request_id": n, "customer_id": f"W7-{cid}",
                       "service": service, "usage": usage, "ft_mode": ft_mode}
            started = time.time()
            reply = {}
            try:
                reply = call((NODE_HOST, NODE2_PORT), msg, timeout=args.client_timeout)
                ok = (reply.get("status") == "COMMIT") if sc["kind"] == "tx" else ("error" not in reply)
                charge = reply.get("charge")
                correct = bool(ok and charge is not None and abs(charge - expected) < 1e-6)
            except Exception as exc:
                ok, correct, reply = False, False, {"error": f"{type(exc).__name__}: {exc}"}
            ended = time.time()
            with lock:
                rows.append({"scenario": name, "mode": mode, "client": cid, "request_no": n,
                             "transaction_id": txid if sc["kind"] == "tx" else "",
                             "t_start_s": round(started - t0, 4), "t_end_s": round(ended - t0, 4),
                             "ok": ok, "correct": correct, "latency_ms": round((ended - started) * 1000, 3),
                             "served_by": reply.get("selected_node") or reply.get("rating_node") or "",
                             "failover_attempts": reply.get("failover_attempts", reply.get("attempts", 1) - 1 if "attempts" in reply else 0),
                             "error": str(reply.get("error", ""))[:120],
                             "_msg": msg if sc["kind"] == "tx" else None})
            time.sleep(args.think)

    # monitor the target so node-level downtime is measured independently of clients
    probe = {"down_at": None, "up_at": None}

    def monitor():
        while not stop.is_set():
            now = time.time() - t0
            try:
                r = call(target_addr, {"type": "PROBE"}, timeout=0.4)
                healthy = not r.get("error")
            except Exception:
                healthy = False
            if now >= args.pre:
                if not healthy and probe["down_at"] is None:
                    probe["down_at"] = now
                if healthy and probe["down_at"] is not None and probe["up_at"] is None:
                    probe["up_at"] = now
            time.sleep(0.1)

    threads = [threading.Thread(target=client, args=(c,), daemon=True) for c in range(args.clients)]
    mon = threading.Thread(target=monitor, daemon=True)
    for t in threads:
        t.start()
    mon.start()

    time.sleep(args.pre)
    inject_epoch = time.time()
    inject_s = inject_epoch - t0
    try:
        injected = call(target_addr, {"type": "FAULT_INJECT", **sc["fault"]}, timeout=3)
    except Exception as exc:
        injected = {"ok": False, "error": str(exc)}
    time.sleep(args.fault)
    clear_epoch = time.time()
    if sc["fault"]["mode"] != "crash":
        try:
            call(target_addr, {"type": "FAULT_CLEAR"}, timeout=3)
        except Exception:
            pass
    time.sleep(args.post)
    stop.set()
    for t in threads:
        t.join(timeout=args.client_timeout + 2)
    elapsed = time.time() - t0

    # ---- evidence from the system under test ------------------------------
    for _ in range(40):  # allow a restarting coordinator to come back
        try:
            ft = call((NODE_HOST, NODE2_PORT), {"type": "FT_STATUS"}, timeout=3)
            break
        except Exception:
            time.sleep(0.5)
    else:
        ft = {"detector": {"events": [], "nodes": {}}, "config": {}}
    events = [e for e in ft["detector"]["events"] if e["timestamp"] >= inject_epoch - 0.5]
    mine = [e for e in events if e["node"] == sc["target"]]
    first = lambda names: next((round(e["timestamp"] - inject_epoch, 3) for e in mine if e["event"] in names), None)
    detection_s = first(("SUSPECTED", "SUSPECTED_PASSIVE", "DOWN"))
    down_declared_s = first(("DOWN",))
    recovered_s = first(("RECOVERED",))

    consistency = resync = stuck = None
    converge_s = auto_recovery_s = None
    if sc["kind"] == "tx":
        # The baseline architecture has one ledger; the replica only exists in the resilient system.
        names = ["LEDGER-1"] if mode == "baseline" else [n for n, _ in LEDGER_REPLICAS]

        def consistent(report):
            return all(report["replicas"].get(n, {}).get("reachable") and
                       report["replicas"][n]["missing_committed"] == 0 and
                       report["replicas"][n]["orphan_prepared"] == 0 for n in names)

        deadline = time.time() + 60
        consistency = {"error": "no reply"}
        while time.time() < deadline:   # poll until durable state has converged
            try:
                consistency = call((NODE_HOST, NODE2_PORT), {"type": "TX_CONSISTENCY"}, timeout=30)
                if consistent(consistency):
                    converge_s = round(max(0.0, time.time() - (t0 + elapsed)), 3)  # seconds after the run ended
                    break
            except Exception as exc:
                consistency = {"error": str(exc)}
            time.sleep(0.5)
        if "replicas" in consistency:
            consistency["consistent"] = consistent(consistency)
        # Automatic recovery (re-drive on node recovery + periodic re-drive) must clear every pending
        # transaction on its own; this explicit wait does NOT call recovery for it.
        auto_recovery_s, stuck = None, None
        wait_started = time.time()
        while time.time() - wait_started < 30:
            try:
                stuck = call((NODE_HOST, NODE2_PORT), {"type": "TX_PENDING"}, timeout=10).get("pending")
            except Exception:
                stuck = None
            if stuck == 0:
                auto_recovery_s = round(time.time() - wait_started, 3)
                break
            time.sleep(0.5)
        if stuck:   # leave the system clean for the next scenario even if automatic recovery failed
            try:
                call((NODE_HOST, NODE2_PORT), {"type": "TX_RECOVER"}, timeout=60)
            except Exception:
                pass
        failed = [r for r in rows if not r["ok"] and r["transaction_id"]]
        outcomes = {}
        for r in failed[:400]:
            try:
                st = call((NODE_HOST, NODE2_PORT), {"type": "TX_STATUS", "transaction_id": r["transaction_id"]}, timeout=5).get("status")
            except Exception:
                st = "UNREACHABLE"
            outcomes[st] = outcomes.get(st, 0) + 1
        # exactly-once check: a client replays failed requests with the SAME transaction id
        replayed = committed_again = 0
        for r in failed[:50]:
            replayed += 1
            try:
                again = call((NODE_HOST, NODE2_PORT), r["_msg"], timeout=10)
                committed_again += again.get("status") == "COMMIT"
            except Exception:
                pass
        resync = {"failed_request_outcomes": outcomes, "replay_attempted": replayed, "replay_committed": committed_again,
                  "recovery_events": [e["detail"] for e in events if e["event"] in ("RESYNC", "STARTUP_RECOVERY", "REDRIVE")][:5]}

    # ---- metrics ----------------------------------------------------------
    for r in rows:
        r["phase"] = "pre" if r["t_start_s"] < inject_s else ("fault" if r["t_start_s"] < inject_s + args.fault else "post")
        r["run_s"] = total_s
        r["repeat"] = rep
    # the grid ends at the planned run length: the time after the clients stop is not service time
    buckets = bucketize(rows, total_s, width=args.bucket)
    m = mttr(buckets, inject_s, width=args.bucket)
    phase = {p: summarize_rows([r for r in rows if r["phase"] == p]) for p in ("pre", "fault", "post")}
    summary = {
        "scenario": name, "repeat": rep, "label": sc["label"], "mode": mode, "target": sc["target"],
        "fault": sc["fault"]["mode"], "clients": args.clients, "duration_s": round(elapsed, 2),
        "requests": len(rows),
        "availability_pct": round(100 * availability(buckets), 2),
        "availability_during_fault_pct": round(100 * (availability(buckets, inject_s, inject_s + args.fault) or 0), 2),
        "request_success_pct": summarize_rows(rows)["success_pct"],
        "wrong_result_pct": summarize_rows(rows)["wrong_pct"],
        "fault_phase_success_pct": phase["fault"]["success_pct"],
        "fault_phase_wrong_pct": phase["fault"]["wrong_pct"],
        "pre_mean_latency_ms": phase["pre"]["mean_latency_ms"],
        "fault_mean_latency_ms": phase["fault"]["mean_latency_ms"],
        "fault_p95_latency_ms": phase["fault"]["p95_latency_ms"],
        "masked": m["masked"], "mttr_s": m["mttr_s"], "outage_s": m["outage_s"],
        "detection_s": detection_s, "down_declared_s": down_declared_s, "detector_recovered_s": recovered_s,
        "node_down_observed_s": round(probe["down_at"] - inject_s, 3) if probe["down_at"] is not None else None,
        "node_recovery_s": round(probe["up_at"] - inject_s, 3) if probe["up_at"] is not None else None,
        "failovers": sum(1 for r in rows if (r["failover_attempts"] or 0) > 0),
        "consistent_after_recovery": (consistency or {}).get("consistent") if consistency else None,
        "post_run_convergence_s": converge_s,
        "pending_after_auto_recovery": stuck, "auto_recovery_s": auto_recovery_s,
    }
    detail = {"injected": injected, "inject_epoch": inject_epoch, "clear_epoch": clear_epoch,
              "consistency": consistency, "recovery": resync, "ft_config": ft.get("config", {})}
    return summary, rows, buckets, detail


def aggregate_runs(summaries):
    """Mean / min / max over repeated runs of the same scenario and mode."""
    def stat(values):
        values = [v for v in values if v is not None]
        return (round(sum(values) / len(values), 3), round(min(values), 3), round(max(values), 3)) if values else (None, None, None)
    out = []
    keys = []
    for s in summaries:
        if (s["scenario"], s["mode"]) not in keys:
            keys.append((s["scenario"], s["mode"]))
    for scenario, mode in keys:
        runs = [s for s in summaries if s["scenario"] == scenario and s["mode"] == mode]
        row = {"scenario": scenario, "mode": mode, "runs": len(runs), "masked_runs": sum(1 for r in runs if r["masked"])}
        for label, field in (("availability", "availability_pct"), ("success", "request_success_pct"),
                             ("wrong", "wrong_result_pct"), ("mttr", "mttr_s"), ("detection", "detection_s"),
                             ("node_recovery", "node_recovery_s"), ("fault_latency", "fault_mean_latency_ms")):
            mean, lo, hi = stat([r[field] for r in runs])
            unit = "_pct" if field.endswith("_pct") else ("_ms" if field.endswith("_ms") else "_s")
            row[f"{label}_mean{unit}"], row[f"{label}_min{unit}"], row[f"{label}_max{unit}"] = mean, lo, hi
        out.append(row)
    return out


def write_csv(path, rows, fields):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scenarios", default="all")
    p.add_argument("--pre", type=float, default=5.0)
    p.add_argument("--fault", type=float, default=8.0)
    p.add_argument("--post", type=float, default=8.0)
    p.add_argument("--clients", type=int, default=8)
    p.add_argument("--think", type=float, default=0.02, help="client think time (s)")
    p.add_argument("--bucket", type=float, default=0.5)
    p.add_argument("--client-timeout", type=float, default=15.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--repeats", type=int, default=1, help="independent repetitions per scenario and mode")
    p.add_argument("--quick", action="store_true", help="shorter phases (3/4/4 s) for a smoke test")
    args = p.parse_args()
    if args.quick:
        args.pre, args.fault, args.post = 3.0, 4.0, 4.0
    names = list(SCENARIOS) if args.scenarios == "all" else [s.strip() for s in args.scenarios.split(",")]
    for n in names:
        if n not in SCENARIOS:
            raise SystemExit(f"unknown scenario {n}; choose from {list(SCENARIOS)}")

    os.makedirs(RESULTS_DIR, exist_ok=True)
    run_id = str(int(time.time()))
    print("=" * 78)
    print("WEEK 7 - FAULT TOLERANCE AND RECOVERY (baseline vs resilient)")
    print("=" * 78)
    summaries, all_rows, all_buckets, details = [], [], [], {}
    for name in names:
        for mode in SCENARIOS[name]["modes"]:
          for rep in range(1, args.repeats + 1):
            print(f"\n>>> {name} / {mode}: {SCENARIOS[name]['label']}  (run {rep}/{args.repeats})")
            summary, rows, buckets, detail = run_scenario(name, mode, args, run_id, rep)
            summaries.append(summary)
            all_rows.extend(rows)
            for b in buckets:
                all_buckets.append({"scenario": name, "mode": mode, "repeat": rep, **b})
            details[f"{name}/{mode}/r{rep}"] = detail
            print(f"    availability {summary['availability_pct']}%  (during fault {summary['availability_during_fault_pct']}%)"
                  f"  success {summary['request_success_pct']}%  wrong {summary['wrong_result_pct']}%"
                  f"  MTTR {summary['mttr_s']}s{' (masked)' if summary['masked'] else ''}"
                  f"  detection {summary['detection_s']}s  failovers {summary['failovers']}")

    write_csv(os.path.join(RESULTS_DIR, "week7_requests.csv"), all_rows,
              ["scenario", "mode", "repeat", "phase", "run_s", "client", "request_no", "transaction_id", "t_start_s", "t_end_s",
               "ok", "correct", "latency_ms", "served_by", "failover_attempts", "error"])
    write_csv(os.path.join(RESULTS_DIR, "week7_timeline.csv"), all_buckets,
              ["scenario", "mode", "repeat", "bucket_start_s", "completed", "good", "failed", "wrong", "up"])
    write_csv(os.path.join(RESULTS_DIR, "week7_summary.csv"), summaries, list(summaries[0].keys()))
    env = {"platform": platform.platform(), "python": platform.python_version(),
           "cpu_count": os.cpu_count(), "in_docker": os.path.exists("/.dockerenv"),
           "date": time.strftime("%Y-%m-%d %H:%M:%S"), "args": vars(args)}
    with open(os.path.join(RESULTS_DIR, "week7_experiment_summary.json"), "w") as f:
        json.dump({"environment": env, "summaries": summaries, "details": details}, f, indent=2, default=str)

    aggregate = aggregate_runs(summaries)
    write_csv(os.path.join(RESULTS_DIR, "week7_summary_aggregate.csv"), aggregate, list(aggregate[0].keys()))
    print("\n" + "=" * 98)
    print(f"Mean over {args.repeats} run(s) per scenario and mode  (min-max in week7_summary_aggregate.csv)")
    print(f"{'scenario':<18}{'mode':<17}{'avail%':>8}{'success%':>9}{'wrong%':>7}{'MTTR s':>8}{'masked':>8}{'detect s':>9}{'recov s':>9}")
    for a in aggregate:
        print(f"{a['scenario']:<18}{a['mode']:<17}{a['availability_mean_pct']:>8}{a['success_mean_pct']:>9}"
              f"{a['wrong_mean_pct']:>7}{str(a['mttr_mean_s']):>8}{str(a['masked_runs']) + '/' + str(a['runs']):>8}"
              f"{str(a['detection_mean_s']):>9}{str(a['node_recovery_mean_s']):>9}")
    print("\nSaved: results/week7_requests.csv, week7_timeline.csv, week7_summary.csv, week7_experiment_summary.json")


if __name__ == "__main__":
    main()
