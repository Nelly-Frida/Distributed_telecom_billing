"""Week 10: naming and distributed process management.

Part A  Naming:  Name -> Name Resolution -> Resource Location, five strategies compared:
        static map, broadcast, central name server, DNS-style hierarchy with TTL cache, prefix table.
Part B  Process management:  Create -> Locate -> Schedule -> Coordinate -> Terminate.

Hypotheses
  H10-1 latency      Cached strategies (prefix, DNS warm) need no message and are the fastest; the central
                     server needs one; DNS cold needs two; broadcast needs N and is the slowest.
  H10-2 scalability  Broadcast cost grows linearly with the number of nodes; the others do not. A central
                     server saturates under concurrent clients; caching resolvers scale. A prefix table pays
                     in transfer size, not in lookup time.
  H10-3 staleness    After a resource moves, static is wrong forever, DNS until its TTL expires, a prefix
                     table until a NOT_OWNER reply makes it refresh; central and broadcast are never wrong.
  H10-4 availability Caches keep resolving through a name-server outage; central and cold lookups fail.
  H10-5 scheduling   Least-loaded scheduling gives a shorter makespan than round robin or random on skewed work.
  H10-6 coordination Fan-out and the barrier add a small, roughly constant overhead; speed-up is limited by the
                     number of nodes.
  H10-7 recovery     A task whose node crashes is rescheduled and finishes with an exact result (exactly-once),
                     and the same holds for a voluntary migration; terminate leaves no name and no task behind.

Needs the running system. Outputs: results/week10_summary.csv, week10_lookups.csv, week10_experiment_summary.json.

    python experiments/week10_naming_processes.py
    python experiments/week10_naming_processes.py --quick
"""
import argparse, csv, json, os, platform, re, random, statistics, subprocess, sys, threading, time
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.communication import send_json
from common.config import NAME_SERVERS, NODE2_PORT, NODE_HOST, RATING_NODES, RESULTS_DIR
from experiments.ft_metrics import percentile
from naming.resolvers import (BroadcastResolver, CentralResolver, DnsResolver, NamingAdmin, PrefixResolver,
                              StaticResolver, partition_of)
from process_mgmt.node_runtime import expected_batch
from transparency import placement

NODES = list(RATING_NODES)
SUMMARY, LOOKUPS, DETAIL = [], [], {}
STATIC_PATH = "data/static_map.json"


def gw(msg, timeout=60):
    return send_json(NODE_HOST, NODE2_PORT, msg, timeout=timeout)


def node(name, msg, timeout=10):
    h, p = RATING_NODES[name]
    return send_json(h, p, msg, timeout=timeout)


def ns(server, msg, timeout=10):
    h, p = NAME_SERVERS[server]
    return send_json(h, p, msg, timeout=timeout)


def add(experiment, variant, metric, value, unit=""):
    SUMMARY.append({"experiment": experiment, "variant": variant, "metric": metric, "value": value, "unit": unit})


def lat_stats(v):
    if not v:
        return {"mean": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}
    return {"mean": round(sum(v) / len(v), 4), "p50": round(percentile(v, 50), 4), "p95": round(percentile(v, 95), 4),
            "p99": round(percentile(v, 99), 4)}


def wait_healthy(timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            for n in NODES:
                node(n, {"type": "FAULT_CLEAR"}, timeout=1.5)
            for s in NAME_SERVERS:
                ns(s, {"type": "FAULT_CLEAR"}, timeout=1.5)
            st = gw({"type": "FT_STATUS"}, timeout=3)
            if all(v["state"] == "UP" for k, v in st["detector"]["nodes"].items() if k.startswith("NODE")):
                return True
        except Exception:
            pass
        time.sleep(0.5)
    raise SystemExit("system not healthy: start `python launcher.py` or `docker compose up -d` first")


def fresh(partitions, replication=2):
    """Clean slate: nodes and name servers reset, gateway cache flushed."""
    wait_healthy()
    pl = placement.setup_partitions(placement.prefixes(partitions), replication, static_path=STATIC_PATH)
    gw({"type": "LOGICAL_INVALIDATE"})
    return pl


def zipf_names(rng, n, partitions=100, per_partition=100, s=1.1):
    weights = [1.0 / (k + 1) ** s for k in range(partitions)]
    parts = rng.choices(range(partitions), weights=weights, k=n)
    return [f"acct.C{p * per_partition + rng.randrange(per_partition):05d}" for p in parts]


def authoritative(name):
    return ns("NS-CENTRAL", {"type": "NS_RESOLVE", "name": name})["locations"]


def strategies(ttl=5.0):
    """Fresh resolver instances: name -> (resolver, per-lookup preparation)."""
    static = StaticResolver(path=STATIC_PATH)
    return {
        "static": (static, None),
        "broadcast": (BroadcastResolver(), None),
        "central": (CentralResolver(), None),
        "dns_cold": (DnsResolver(ttl=0.0), lambda r: r.delegations.clear()),
        "dns_warm": (DnsResolver(ttl=ttl), None),
        "prefix_cold": (PrefixResolver(), lambda r: r.invalidate()),
        "prefix_warm": (PrefixResolver(), None),
    }


# =========================================================================== Part A: naming
def a1_latency(args):
    print("\n[H10-1] Resolution latency and message cost per strategy")
    fresh(100, 2)
    rng = random.Random(args.seed)
    names = zipf_names(rng, args.lookups)
    truth = {}
    for name in set(names):
        truth[name] = sorted(authoritative(partition_of(name)))
    for label, (res, prep) in strategies().items():
        res.resolve(names[0])          # warm-up (also loads the table / delegation for the warm variants)
        res.reset_stats()
        lat, correct = [], 0
        for i, name in enumerate(names):
            if prep:
                prep(res)
            before = res.stats["messages"]
            t = time.perf_counter()
            rec = res.resolve(name)
            ms = (time.perf_counter() - t) * 1000
            ok = bool(rec) and sorted(rec["locations"]) == truth[name]
            correct += ok
            lat.append(ms)
            LOOKUPS.append({"experiment": "resolution", "strategy": label, "i": i, "latency_ms": round(ms, 4),
                            "messages": res.stats["messages"] - before, "correct": ok})
        s = lat_stats(lat)
        msgs = res.stats["messages"] / len(names)
        hits = res.stats["cache_hits"] / len(names)
        for k, v in (("mean_ms", s["mean"]), ("p50_ms", s["p50"]), ("p95_ms", s["p95"]), ("p99_ms", s["p99"]),
                     ("messages_per_lookup", round(msgs, 3)), ("cache_hit_ratio", round(hits, 3)),
                     ("correct_pct", round(100.0 * correct / len(names), 2))):
            add("resolution", label, k, v)
        print(f"   {label:<12} mean {s['mean']:>8} ms  p95 {s['p95']:>8} ms  msgs/lookup {msgs:>6.2f}  hits {hits:>5.2f}  correct {100 * correct / len(names):.1f}%")


def a2_staleness(args):
    print("\n[H10-3] Staleness after a resource moves")
    wait_healthy()
    fresh(20, 2)
    prefix, name = "acct.C005", "acct.C00501"
    for label in ("static", "central", "broadcast", "dns", "prefix"):
        fresh(20, 2)
        gw({"type": "LOGICAL_INVALIDATE"})
        res = {"static": StaticResolver(path=STATIC_PATH), "central": CentralResolver(), "broadcast": BroadcastResolver(),
               "dns": DnsResolver(ttl=args.ttl), "prefix": PrefixResolver()}[label]
        old = sorted(res.resolve(name)["locations"])
        spare = next(n for n in NODES if n not in old)
        t_move = time.time()
        gw({"type": "LOGICAL_MIGRATE", "prefix": prefix, "from": old[0], "to": spare})
        new = sorted(authoritative(prefix))
        first_correct, wrong = None, 0
        while time.time() - t_move < args.ttl + 3:
            rec = res.resolve(name)
            if rec and sorted(rec["locations"]) == new:
                first_correct = round(time.time() - t_move, 3)
                break
            wrong += 1
            time.sleep(0.1)
        # with the redirect protocol: use the answer, get NOT_OWNER from the old node, refresh, retry
        res2 = {"static": StaticResolver(path=STATIC_PATH), "central": CentralResolver(), "broadcast": BroadcastResolver(),
                "dns": DnsResolver(ttl=60), "prefix": PrefixResolver()}[label]
        if label in ("dns", "prefix"):      # make this resolver stale again: warm it, then move the partition
            fresh(20, 2)
            rec0 = res2.resolve(name)
            gw({"type": "LOGICAL_MIGRATE", "prefix": prefix, "from": rec0["locations"][0],
                "to": next(n for n in NODES if n not in rec0["locations"])})
        t0 = time.perf_counter()
        attempts, served = 0, False
        for attempts in (1, 2):
            rec = res2.resolve(name)
            r = node(rec["locations"][0], {"type": "ACCT_GET", "prefix": prefix, "customer": "C00501"}) if rec else {"error": "x"}
            if r.get("error") != "NOT_OWNER" and "error" not in r:
                served = True
                break
            res2.invalidate(name)
        redirect_ms = round((time.perf_counter() - t0) * 1000, 3)
        add("staleness", label, "time_to_correct_s", first_correct, "s")
        add("staleness", label, "wrong_lookups_before_correct", wrong)
        add("staleness", label, "request_served_after_move_with_redirect", served)
        add("staleness", label, "attempts_with_redirect", attempts)
        add("staleness", label, "redirect_total_ms", redirect_ms, "ms")
        print(f"   {label:<10} correct again after {first_correct}s ({wrong} wrong lookups);  op after the move with redirect: "
              f"served={served} in {attempts} attempt(s), {redirect_ms} ms")


_farm = None


def start_farm(count):
    global _farm
    _farm = subprocess.Popen([sys.executable, "-m", "naming.endpoint_farm", "--base-port", "6100", "--count", str(count)],
                             cwd=PROJECT_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.5)


def stop_farm():
    if _farm:
        _farm.terminate()


def a3_scalability(args):
    print("\n[H10-2] Scalability: nodes, concurrent clients, number of names")
    fresh(100, 2)
    # (a) broadcast cost against the number of nodes (real sockets to a farm of endpoints)
    sizes = [4, 8, 16, 32, 64] if not args.quick else [4, 16, 64]
    start_farm(max(sizes))
    try:
        for n in sizes:
            res = BroadcastResolver(nodes={f"F{k}": ("127.0.0.1", 6100 + k) for k in range(n)}, timeout=2.0)
            lat = []
            for i in range(args.lookups // 10):
                k = i % n
                t = time.perf_counter()
                rec = res.resolve(f"acct.C{k:03d}01")
                lat.append((time.perf_counter() - t) * 1000)
                assert rec and rec["locations"] == [f"F{k}"], rec
            s = lat_stats(lat)
            add("scalability_nodes", f"broadcast_N={n}", "mean_ms", s["mean"], "ms")
            add("scalability_nodes", f"broadcast_N={n}", "messages_per_lookup", res.stats["messages"] / len(lat))
            print(f"   broadcast N={n:<3} mean {s['mean']:>8} ms  messages/lookup {res.stats['messages'] / len(lat):.0f}")
    finally:
        stop_farm()
    for label in ("central", "prefix_warm", "dns_warm"):
        res = dict(strategies())[label][0]
        res.resolve("acct.C00001")
        lat = []
        for i in range(args.lookups // 10):
            t = time.perf_counter()
            res.resolve(f"acct.C{(i % 100) * 100:05d}")
            lat.append((time.perf_counter() - t) * 1000)
        add("scalability_nodes", f"{label}_independent_of_N", "mean_ms", lat_stats(lat)["mean"], "ms")
    # (b) concurrent clients: a central server saturates, caching resolvers do not
    for label in ("central", "dns_warm", "prefix_warm"):
        for threads in ([1, 4, 16, 32] if not args.quick else [1, 16]):
            res = dict(strategies())[label][0]
            res.resolve("acct.C00001")
            done, lats, stop = [0], [], threading.Event()
            lock = threading.Lock()

            def worker(seed):
                rng = random.Random(seed)
                while not stop.is_set():
                    t = time.perf_counter()
                    res.resolve(f"acct.C{rng.randrange(100) * 100 + 1:05d}")
                    ms = (time.perf_counter() - t) * 1000
                    with lock:
                        done[0] += 1
                        lats.append(ms)

            ts = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(threads)]
            t0 = time.time()
            for t in ts:
                t.start()
            time.sleep(args.window)
            stop.set()
            for t in ts:
                t.join(timeout=5)
            span = time.time() - t0
            add("scalability_clients", f"{label}_T={threads}", "throughput_lookups_s", round(done[0] / span, 1), "1/s")
            add("scalability_clients", f"{label}_T={threads}", "mean_ms", lat_stats(lats)["mean"], "ms")
            print(f"   {label:<12} clients={threads:<3} {done[0] / span:>9.0f} lookups/s  mean {lat_stats(lats)['mean']:>8} ms")
    # (c) number of registered names: lookup time and the price of downloading a prefix table
    for k in ([1000, 10000, 50000] if not args.quick else [1000, 10000]):
        recs = [{"name": f"acct.K{i:07d}", "kind": "partition", "locations": [NODES[i % 4], NODES[(i + 1) % 4]],
                 "ttl": 5.0, "is_prefix": True} for i in range(k)]
        t = time.perf_counter()
        ns("NS-CENTRAL", {"type": "NS_REGISTER_MANY", "records": recs}, timeout=120)
        reg_s = time.perf_counter() - t
        pr = PrefixResolver(timeout=10)
        t = time.perf_counter()
        pr.resolve(f"acct.K{k // 2:07d}99")
        load_ms = (time.perf_counter() - t) * 1000
        lat_p, lat_c = [], []
        cr = CentralResolver()
        for i in range(300):
            nm = f"acct.K{(i * 7919) % k:07d}55"
            t = time.perf_counter()
            pr.resolve(nm)
            lat_p.append((time.perf_counter() - t) * 1000)
            t = time.perf_counter()
            cr.resolve(nm)
            lat_c.append((time.perf_counter() - t) * 1000)
        add("scalability_names", f"names={k}", "register_s", round(reg_s, 3), "s")
        add("scalability_names", f"names={k}", "prefix_table_load_ms", round(load_ms, 2), "ms")
        add("scalability_names", f"names={k}", "prefix_lookup_ms", lat_stats(lat_p)["mean"], "ms")
        add("scalability_names", f"names={k}", "central_lookup_ms", lat_stats(lat_c)["mean"], "ms")
        print(f"   names={k:<6} register {reg_s:5.2f}s  prefix table load {load_ms:8.1f} ms  lookup: prefix {lat_stats(lat_p)['mean']} ms, central {lat_stats(lat_c)['mean']} ms")
    fresh(100, 2)   # restore the standard placement


def a4_outage(args):
    print("\n[H10-4] Name-server outage: which strategies keep resolving")
    fresh(100, 2)
    rng = random.Random(args.seed)
    names = zipf_names(rng, 200)[:50]
    resolvers = strategies(ttl=30)
    for label, (res, prep) in resolvers.items():      # warm every resolver while the name servers are healthy
        for nm in names:
            res.resolve(nm)
    for s_ in NAME_SERVERS:
        ns(s_, {"type": "FAULT_INJECT", "mode": "drop", "duration_s": args.outage})
    time.sleep(0.2)
    for label, (res, prep) in resolvers.items():
        ok = 0
        for nm in names:
            if prep:
                prep(res)            # the *_cold variants start every lookup with empty caches
            ok += bool(res.resolve(nm))
        pct = round(100.0 * ok / len(names), 1)
        add("ns_outage", label, "resolved_during_outage_pct", pct, "%")
        print(f"   {label:<12} resolved {pct:>5}% of lookups while every name server drops connections")
    time.sleep(args.outage)
    for s_ in NAME_SERVERS:
        ns(s_, {"type": "FAULT_CLEAR"})
    recovered = bool(CentralResolver().resolve(names[0]))
    add("ns_outage", "central", "resolves_again_after_recovery", recovered)
    print(f"   after the outage the central server resolves again: {recovered}")


# =========================================================================== Part B: processes
def pm(msg, timeout=120):
    return gw(msg, timeout=timeout)


def reset_pm():
    pm({"type": "PM_RESET"})


def b1_lifecycle(args):
    print("\n[H10-7a] Life cycle: create, schedule, locate, coordinate, terminate (and nothing left behind)")
    wait_healthy()
    fresh(20, 2)
    reset_pm()
    tasks = [120, 80, 200, 60, 140, 100]
    g = pm({"type": "PM_CREATE_GROUP", "tasks": tasks, "seed": 21, "work_ms": 2.0, "policy": "least_loaded"})
    ids = g["proc_ids"]
    named = sum(1 for i in ids if CentralResolver().resolve(i))
    located = sum(1 for i in ids if pm({"type": "PM_LOCATE", "proc_id": i}).get("node"))
    w = pm({"type": "PM_WAIT", "proc_ids": ids})
    red = pm({"type": "PM_REDUCE", "proc_ids": ids})
    exp = expected_batch(21, 0, sum(tasks))
    exact = abs(red["charge"] - exp["charge"]) < 1e-6 and abs(red["usage"] - exp["usage"]) < 1e-6 and red["records"] == sum(tasks)
    # parent / child termination with cascade
    parent = pm({"type": "PM_CREATE", "spec": {"seed": 1, "start": 0, "count": 4000, "work_ms": 2.0}})["proc_id"]
    kids = [pm({"type": "PM_CREATE", "spec": {"seed": 2, "start": 0, "count": 4000, "work_ms": 2.0}, "parent": parent})["proc_id"]
            for _ in range(3)]
    time.sleep(0.3)
    term = pm({"type": "PM_TERMINATE", "proc_id": parent, "cascade": True})
    time.sleep(0.5)
    names_left = sum(1 for i in [parent] + kids if CentralResolver().resolve(i))
    states = [pm({"type": "PM_INFO", "proc_id": i})["state"] for i in [parent] + kids]
    leaked = sum(len(node(n, {"type": "PROC_LIST"})["processes"]) for n in NODES)
    add("lifecycle", "group", "tasks", len(tasks))
    add("lifecycle", "group", "names_registered", named)
    add("lifecycle", "group", "located", located)
    add("lifecycle", "group", "result_exact", bool(exact))
    add("lifecycle", "group", "makespan_s", w["wait_s"], "s")
    add("lifecycle", "terminate", "terminated_processes", len(term["terminated"]))
    add("lifecycle", "terminate", "all_terminated", all(s == "TERMINATED" for s in states))
    add("lifecycle", "terminate", "names_left", names_left)
    add("lifecycle", "terminate", "tasks_left_on_nodes", leaked)
    add("lifecycle", "terminate", "terminate_ms", term["terminate_ms"], "ms")
    print(f"   {named}/{len(ids)} names registered, {located}/{len(ids)} located, result exact: {exact}; "
          f"terminate cascade stopped {len(term['terminated'])} processes in {term['terminate_ms']} ms, "
          f"names left {names_left}, tasks left on nodes {leaked}")
    DETAIL["lifecycle"] = {"reduce": red, "expected": exp, "terminate": term}


def b2_scheduling(args):
    print("\n[H10-5] Scheduling policies on skewed work (makespan, balance)")
    wait_healthy()
    fresh(20, 2)
    rng = random.Random(args.seed)
    tasks = [rng.choice([30, 30, 40, 60, 80, 400, 600]) for _ in range(24)]
    DETAIL["scheduling_tasks"] = tasks
    for policy in ("round_robin", "random", "least_loaded"):
        spans = []
        for rep in range(args.repeats):
            reset_pm()
            t0 = time.time()
            g = pm({"type": "PM_CREATE_GROUP", "tasks": tasks, "seed": 3, "work_ms": 2.0, "policy": policy})
            w = pm({"type": "PM_WAIT", "proc_ids": g["proc_ids"]})
            span = time.time() - t0
            spans.append(span)
            if rep == 0:
                per_node = {}
                for i, task in zip(g["proc_ids"], tasks):
                    n = pm({"type": "PM_INFO", "proc_id": i})["node"]
                    per_node[n] = per_node.get(n, 0) + task
                vals = [per_node.get(n, 0) for n in NODES]
                cv = statistics.pstdev(vals) / statistics.mean(vals)
                add("scheduling", policy, "assigned_work_cv", round(cv, 3))
                add("scheduling", policy, "max_node_records", max(vals))
        add("scheduling", policy, "makespan_s_mean", round(sum(spans) / len(spans), 3), "s")
        add("scheduling", policy, "makespan_s_max", round(max(spans), 3), "s")
        print(f"   {policy:<13} makespan mean {sum(spans) / len(spans):6.2f}s (max {max(spans):.2f}s)  imbalance cv {cv:.3f}  busiest node {max(vals)} records")
    reset_pm()
    ideal = sum(tasks) * 2.0 / 1000 / len(NODES)
    add("scheduling", "ideal", "makespan_s", round(ideal, 3), "s")
    print(f"   ideal (perfect balance over {len(NODES)} nodes): {ideal:.2f}s")


def b3_coordination(args):
    print("\n[H10-6] Coordination overhead: fan-out, barrier, reduce")
    wait_healthy()
    fresh(20, 2)
    total = 1600
    base = None
    for k in ([1, 2, 4, 8, 16] if not args.quick else [1, 4, 16]):
        reset_pm()
        tasks = [total // k] * k
        t0 = time.time()
        g = pm({"type": "PM_CREATE_GROUP", "tasks": tasks, "seed": 4, "work_ms": 2.0, "policy": "least_loaded"})
        w = pm({"type": "PM_WAIT", "proc_ids": g["proc_ids"]})
        t_ret = time.time()
        red = pm({"type": "PM_REDUCE", "proc_ids": g["proc_ids"]})
        span = t_ret - t0
        barrier_ms = max(0.0, (t_ret - w["finished_at"]) * 1000)
        exact = abs(red["charge"] - expected_batch(4, 0, total)["charge"]) < 1e-6
        base = base or span
        add("coordination", f"tasks={k}", "create_ms", g["create_ms"], "ms")
        add("coordination", f"tasks={k}", "makespan_s", round(span, 3), "s")
        add("coordination", f"tasks={k}", "barrier_overhead_ms", round(barrier_ms, 1), "ms")
        add("coordination", f"tasks={k}", "speedup", round(base / span, 2))
        add("coordination", f"tasks={k}", "result_exact", bool(exact))
        print(f"   tasks={k:<3} create {g['create_ms']:>7} ms  makespan {span:6.2f}s  barrier overhead {barrier_ms:6.1f} ms  speed-up {base / span:4.2f}  exact={exact}")
    reset_pm()


def b4_recovery(args):
    print("\n[H10-7b] A node crashes while it runs tasks: reschedule from checkpoint, exactly-once result")
    wait_healthy()
    fresh(20, 2)
    reset_pm()
    victim = "NODE-4"
    tasks = [300] * 8
    g = pm({"type": "PM_CREATE_GROUP", "tasks": tasks, "seed": 6, "work_ms": 3.0, "policy": "round_robin"})
    ids = g["proc_ids"]
    time.sleep(0.6)
    infos = {i: pm({"type": "PM_INFO", "proc_id": i}) for i in ids}
    on_victim = [i for i, inf in infos.items() if inf["node"] == victim and inf["state"] == "RUNNING"]
    already_done = [i for i, inf in infos.items() if inf["node"] == victim and inf["state"] != "RUNNING"]
    t_crash = time.time()
    node(victim, {"type": "FAULT_INJECT", "mode": "crash"}, timeout=3)
    w = pm({"type": "PM_WAIT", "proc_ids": ids})
    red = pm({"type": "PM_REDUCE", "proc_ids": ids})
    exp = expected_batch(6, 0, sum(tasks))
    events = [e for e in pm({"type": "PM_EVENTS", "last": 300})["events"] if e["event"] == "RESCHEDULED" and e["timestamp"] >= t_crash]
    first = round(min((e["timestamp"] for e in events), default=t_crash) - t_crash, 3)
    last = round(max((e["timestamp"] for e in events), default=t_crash) - t_crash, 3)
    exact = abs(red["charge"] - exp["charge"]) < 1e-6 and red["records"] == sum(tasks)
    add("recovery", "crash", "tasks_running_on_crashed_node", len(on_victim))
    add("recovery", "crash", "tasks_already_finished_there", len(already_done))
    add("recovery", "crash", "tasks_rescheduled", len(events))
    add("recovery", "crash", "first_reschedule_s", first, "s")
    add("recovery", "crash", "last_reschedule_s", last, "s")
    add("recovery", "crash", "result_exact", bool(exact))
    add("recovery", "crash", "records_processed", red["records"])
    add("recovery", "crash", "restarts", red["restarts"])
    print(f"   {len(on_victim)} running tasks were on {victim} ({len(already_done)} had already finished); {len(events)} rescheduled between {first}s and {last}s after the crash; "
          f"records {red['records']}/{sum(tasks)}; result exact (exactly-once): {exact}")
    DETAIL["recovery"] = {"reduce": red, "expected": exp, "events": events}
    reset_pm()
    wait_healthy()


def b5_migration(args):
    print("\n[H10-7c] Voluntary process migration with a checkpoint")
    wait_healthy()
    fresh(20, 2)
    reset_pm()
    c = pm({"type": "PM_CREATE", "spec": {"seed": 8, "start": 0, "count": 600, "work_ms": 3.0}, "node": "NODE-3"})
    pid = c["proc_id"]
    time.sleep(0.8)
    before = pm({"type": "PM_LOCATE", "proc_id": pid})["node"]
    m = pm({"type": "PM_MIGRATE", "proc_id": pid, "to": "NODE-5"})
    after = pm({"type": "PM_LOCATE", "proc_id": pid})["node"]
    pm({"type": "PM_WAIT", "proc_ids": [pid]})
    red = pm({"type": "PM_REDUCE", "proc_ids": [pid]})
    exp = expected_batch(8, 0, 600)
    exact = abs(red["charge"] - exp["charge"]) < 1e-6 and red["records"] == 600
    add("migration_process", "checkpoint", "downtime_ms", m["stopped_ms"], "ms")
    add("migration_process", "checkpoint", "total_ms", m["total_ms"], "ms")
    add("migration_process", "checkpoint", "checkpoint_offset", m["checkpoint_offset"])
    add("migration_process", "checkpoint", "located_on_new_node_by_name", before == "NODE-3" and after == "NODE-5")
    add("migration_process", "checkpoint", "result_exact", bool(exact))
    print(f"   {pid}: {before} -> {after} at record {m['checkpoint_offset']}, stop-to-switch {m['stopped_ms']} ms, total {m['total_ms']} ms, result exact: {exact}")
    reset_pm()


def numeric(value):
    """Power BI friendly: booleans become 1/0, numbers stay numbers, everything else is blank."""
    if isinstance(value, bool) or str(value) in ("True", "False"):
        return 1 if str(value) == "True" else 0
    try:
        return float(value)
    except (TypeError, ValueError):
        return ""


def series_of(variant):
    """broadcast_N=16 -> broadcast ; tasks=8 -> tasks ; central_T=4 -> central (one line per strategy in a chart)."""
    m = re.match(r"^(.*?)_?(N|T|tasks|names|R)=\d+", str(variant))
    return (m.group(1) or m.group(2)) if m else str(variant)


def param_of(variant):
    """N=16 / T=4 / tasks=8 / names=1000 / R=2#1  ->  the number, so charts can use it as a numeric axis."""
    m = re.search(r"=(\d+)", str(variant))
    return int(m.group(1)) if m else ""


def write_csv(path, rows, fields):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--parts", default="all", help="comma list of latency,staleness,scalability,outage,lifecycle,scheduling,coordination,recovery,migration")
    p.add_argument("--lookups", type=int, default=2000)
    p.add_argument("--ttl", type=float, default=2.0, help="DNS cache lifetime used in the staleness test (s)")
    p.add_argument("--window", type=float, default=2.0, help="seconds per concurrent-clients measurement")
    p.add_argument("--outage", type=float, default=4.0)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--quick", action="store_true")
    args = p.parse_args()
    if args.quick:
        args.lookups, args.window, args.repeats, args.outage = 500, 1.0, 1, 3.0
    table = {"latency": a1_latency, "staleness": a2_staleness, "scalability": a3_scalability, "outage": a4_outage,
             "lifecycle": b1_lifecycle, "scheduling": b2_scheduling, "coordination": b3_coordination,
             "recovery": b4_recovery, "migration": b5_migration}
    names = list(table) if args.parts == "all" else [x.strip() for x in args.parts.split(",")]
    os.makedirs(RESULTS_DIR, exist_ok=True)
    print("=" * 78)
    print("WEEK 10 - NAMING AND DISTRIBUTED PROCESS MANAGEMENT")
    print("=" * 78)
    for n in names:
        table[n](args)
    for row in SUMMARY:
        row["value_num"], row["param_num"] = numeric(row["value"]), param_of(row["variant"])
        row["series"] = series_of(row["variant"])
    write_csv(os.path.join(RESULTS_DIR, "week10_summary.csv"), SUMMARY,
              ["experiment", "variant", "series", "metric", "value", "value_num", "param_num", "unit"])
    write_csv(os.path.join(RESULTS_DIR, "week10_lookups.csv"), LOOKUPS, ["experiment", "strategy", "i", "latency_ms", "messages", "correct"])
    env = {"platform": platform.platform(), "python": platform.python_version(), "cpu_count": os.cpu_count(),
           "in_docker": os.path.exists("/.dockerenv"), "date": time.strftime("%Y-%m-%d %H:%M:%S"), "args": vars(args)}
    with open(os.path.join(RESULTS_DIR, "week10_experiment_summary.json"), "w") as f:
        json.dump({"environment": env, "summary": SUMMARY, "details": DETAIL}, f, indent=2, default=str)
    print("\nSaved: results/week10_summary.csv, week10_lookups.csv, week10_experiment_summary.json")


if __name__ == "__main__":
    main()
