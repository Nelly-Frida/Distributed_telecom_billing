"""Week 9: transparency and reliability.

Applications use logical names (acct.C00042) through the gateway. Each experiment compares that with
a NON-transparent baseline, an application that holds the physical placement and talks to the nodes
itself (common/transparent_client.DirectClient).

Hypotheses
  H9-1 access       The same operation gives the same result over TCP and over HTTP.
  H9-2 location     Callers need no address: the data physically sits where the naming service says,
                    whichever naming strategy the gateway uses.
  H9-3 replication  The replication factor can change from 1 to 3 and back with no client error and no
                    lost write; write latency grows with the number of replicas, while read latency is almost
                    independent of it in a read-only load (it rises in a mixed load because writes share the nodes).
  H9-4 migration    A partition can move to another node under load with no client error, a freeze of a few
                    milliseconds and no lost write; the baseline fails permanently after the move.
  H9-5 failure      A crashed replica is masked (no client error, no lost write) and repaired after it restarts;
                    the baseline fails for the whole outage.
  H9-6 overhead     Transparency costs one extra hop plus one extra write per replica; it is measurable and small
                    compared with the service time of a real rating step.

Design
  Independent variables : client kind (transparent | direct), replication factor R, naming strategy,
                          event (reconfigure, migrate, crash)
  Dependent variables   : error rate, latency (mean, p95, max), freeze time, lost writes, convergence,
                          throughput
  Controlled variables  : workload mix and seed, partitions, node set, clients, run length

Needs the running system (python launcher.py or docker compose up -d); the crash experiment needs the
supervisor / restart policy so the crashed node comes back.

    python experiments/week9_transparency.py
    python experiments/week9_transparency.py --quick
"""
import argparse, csv, json, os, platform, re, random, sys, threading, time
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.communication import send_json
from common.config import (BILLING_API_HOST, BILLING_API_PORT, NAME_SERVERS, NODE2_PORT, NODE_HOST, RATING_NODES,
                           RESULTS_DIR)
from common.http_client import post_json
from common.transparent_client import DirectClient, TransparentClient
from experiments.ft_metrics import percentile
from naming.resolvers import NamingAdmin
from transparency import placement

NODES = list(RATING_NODES)
PARTS = 12
ROWS, SUMMARY, DETAIL = [], [], {}
ROW_LOCK = threading.Lock()


def gw(msg, timeout=60):
    return send_json(NODE_HOST, NODE2_PORT, msg, timeout=timeout)


def node(name, msg, timeout=15):
    h, p = RATING_NODES[name]
    return send_json(h, p, msg, timeout=timeout)


def customers_of(part):
    return [f"C{part * 100 + k:05d}" for k in range(100)]


def wait_healthy(timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            for n in NODES:
                node(n, {"type": "FAULT_CLEAR"}, timeout=1.5)
            st = gw({"type": "FT_STATUS"}, timeout=3)
            if all(v["state"] == "UP" for k, v in st["detector"]["nodes"].items() if k.startswith("NODE")):
                return True
        except Exception:
            pass
        time.sleep(0.5)
    raise SystemExit("system not healthy: start `python launcher.py` or `docker compose up -d` first")


def fresh(replication, parts=PARTS):
    """Clean slate: wipe, publish the placement, empty the gateway caches and stale marks."""
    wait_healthy()
    pl = placement.setup_partitions(placement.prefixes(parts), replication, static_path="data/static_map.json")
    gw({"type": "LOGICAL_MODE", "mode": "prefix"})
    time.sleep(0.3)
    return pl


def stat(values):
    if not values:
        return {"n": 0, "mean": 0.0, "p95": 0.0, "max": 0.0}
    return {"n": len(values), "mean": round(sum(values) / len(values), 3), "p95": round(percentile(values, 95), 3),
            "max": round(max(values), 3)}


def add(experiment, variant, metric, value, unit=""):
    SUMMARY.append({"experiment": experiment, "variant": variant, "metric": metric, "value": value, "unit": unit})


def run_clients(experiment, variant, n, duration, parts, kind, seed, write_share=0.5, direct_placement=None,
                events=None, think=0.0):
    """Closed-loop clients. `events` = [(t_seconds, callable)] run on the main thread. Returns (rows, acked, t0)."""
    rows, acked = [], {}
    stop, t0 = threading.Event(), time.time()

    def worker(cid):
        rng = random.Random(seed * 1000 + cid)
        client = TransparentClient() if kind == "transparent" else DirectClient(direct_placement)
        k = 0
        while not stop.is_set():
            k += 1
            part = rng.choice(parts)
            cust = rng.choice(customers_of(part))
            is_write = rng.random() < write_share
            op_id = f"{experiment}-{variant}-{cid}-{k}"
            started = time.time()
            served, err, ok = "", "", False
            try:
                reply = client.apply(cust, 1.0, 1.0, op_id) if is_write else client.get(cust)
                ok = "error" not in reply
                err = str(reply.get("error", ""))[:60]
                served = reply.get("served_by", "")
                if ok and is_write:
                    with ROW_LOCK:
                        acked.setdefault(part, set()).add(op_id)
            except Exception as exc:
                err = f"{type(exc).__name__}"
            ended = time.time()
            with ROW_LOCK:
                rows.append({"experiment": experiment, "variant": variant, "client": cid, "op": "apply" if is_write else "get",
                             "partition": part, "t_start_s": round(started - t0, 4), "t_end_s": round(ended - t0, 4),
                             "ok": ok, "latency_ms": round((ended - started) * 1000, 3), "served_by": served, "error": err})
            if think:
                time.sleep(think)

    threads = [threading.Thread(target=worker, args=(c,), daemon=True) for c in range(n)]
    for t in threads:
        t.start()
    pending = sorted(events or [], key=lambda e: e[0])
    while time.time() - t0 < duration:
        if pending and time.time() - t0 >= pending[0][0]:
            pending.pop(0)[1]()
        time.sleep(0.01)
    stop.set()
    for t in threads:
        t.join(timeout=20)
    return rows, acked, t0


def lost_writes(acked, nodes_for):
    """Acknowledged writes that exist on no replica. nodes_for(part) -> replica names (from the naming service)."""
    lost = 0
    for part, ids in acked.items():
        have = set()
        for nname in nodes_for(part):
            try:
                have |= set(node(nname, {"type": "ACCT_OP_IDS", "prefix": f"acct.C{part:03d}"})["op_ids"])
            except Exception:
                pass
        lost += len(ids - have)
    return lost


def placement_of(prefix):
    host, port = NAME_SERVERS["NS-CENTRAL"]
    return send_json(host, port, {"type": "NS_RESOLVE", "name": prefix}, timeout=5)["locations"]


def place(prefix, nodes):
    """Put a partition on exactly these nodes (administrator action used to set up an experiment)."""
    for n in NODES:
        node(n, {"type": "OWN_DROP", "prefix": prefix})
    NamingAdmin().move(prefix, list(nodes))
    for n in nodes:
        node(n, {"type": "OWN_ADD", "prefix": prefix, "epoch": 5})


def phase_stats(rows, lo, hi):
    sel = [r for r in rows if lo <= r["t_start_s"] < hi]
    ok = [r for r in sel if r["ok"]]
    s = stat([r["latency_ms"] for r in ok])
    return {"requests": len(sel), "errors": len(sel) - len(ok),
            "error_pct": round(100.0 * (len(sel) - len(ok)) / len(sel), 2) if sel else 0.0, **s}


# --------------------------------------------------------------------------- experiments
def t1_access(args):
    print("\n[H9-1] Access transparency: the same logical operation over TCP and over HTTP")
    fresh(2)
    tcp, results = TransparentClient(), []
    url = f"http://{BILLING_API_HOST}:{BILLING_API_PORT}/logical"
    http_ok = True
    lat = {"tcp": [], "http": []}
    for i in range(60):
        cust = f"C{i:05d}"
        t = time.perf_counter()
        a = tcp.apply(cust, 3.0, 1.0, f"t1-tcp-{i}")
        lat["tcp"].append((time.perf_counter() - t) * 1000)
        try:
            t = time.perf_counter()
            b = post_json(url, {"name": f"acct.{cust}", "op": "apply", "args": {"charge": 4.0, "usage": 1.0, "op_id": f"t1-http-{i}"}},
                          timeout=10)
            lat["http"].append((time.perf_counter() - t) * 1000)
        except Exception as exc:
            http_ok = False
            DETAIL["t1_http_error"] = str(exc)
            break
    if http_ok:
        same = 0
        for i in range(60):
            g1 = tcp.get(f"C{i:05d}")["result"]
            g2 = post_json(url, {"name": f"acct.C{i:05d}", "op": "get"}, timeout=10)["result"]
            same += (g1 == g2 and g1["total_charges"] == 7.0 and g1["op_count"] == 2)
        add("access", "tcp_vs_http", "identical_results_pct", round(100.0 * same / 60, 1), "%")
        add("access", "tcp", "mean_latency_ms", stat(lat["tcp"])["mean"], "ms")
        add("access", "http", "mean_latency_ms", stat(lat["http"])["mean"], "ms")
        print(f"   identical results over both paths: {same}/60   mean latency tcp {stat(lat['tcp'])['mean']} ms, http {stat(lat['http'])['mean']} ms")
    else:
        print("   billing API not reachable, HTTP path skipped:", DETAIL.get("t1_http_error"))
        add("access", "tcp_vs_http", "identical_results_pct", None, "%")


def t2_location(args):
    print("\n[H9-2] Location transparency: logical names only; where the data really is")
    pl = fresh(2)
    c = TransparentClient()
    for p in range(PARTS):
        for cust in customers_of(p)[:10]:
            c.apply(cust, 1.0, 1.0, f"t2-{cust}")
    mismatch = 0
    for p in range(PARTS):
        prefix = f"acct.C{p:03d}"
        named = set(placement_of(prefix))
        holders = {n for n in NODES if prefix in node(n, {"type": "OWN_LIST"})["owned"]}
        if holders != named:
            mismatch += 1
        for n in named:
            ids = node(n, {"type": "ACCT_OP_IDS", "prefix": prefix})["op_ids"]
            if len(ids) != 10:
                mismatch += 1
    add("location", "naming_vs_physical", "partitions_where_data_differs_from_name", mismatch, "count")
    print(f"   partitions whose physical location differs from the name record: {mismatch} of {PARTS}")
    for mode in ("prefix", "dns", "central", "broadcast"):
        gw({"type": "LOGICAL_MODE", "mode": mode})
        lat, resolve, same = [], [], 0
        for i in range(60):
            cust = f"C{(i % PARTS) * 100 + 1:05d}"
            t = time.perf_counter()
            r = c.get(cust)
            lat.append((time.perf_counter() - t) * 1000)
            resolve.append(r.get("t_resolve_ms", 0.0))
            same += (r.get("result", {}).get("op_count") == 1)
        add("location", mode, "correct_results_pct", round(100 * same / 60, 1), "%")
        add("location", mode, "mean_resolve_ms", stat(resolve)["mean"], "ms")
        add("location", mode, "mean_latency_ms", stat(lat)["mean"], "ms")
        print(f"   gateway naming={mode:<9} correct {same}/60  mean resolve {stat(resolve)['mean']} ms  mean total {stat(lat)['mean']} ms")
    gw({"type": "LOGICAL_MODE", "mode": "prefix"})


def t3_replication(args):
    print("\n[H9-3] Replication transparency: replication factor 1 -> 2 -> 3 -> 2 under load")
    fresh(1)
    prefix, part = "acct.C000", 0
    steps = [(1, None), (2, NODES[:2]), (3, NODES[:3]), (2, NODES[:2])]
    phase_len = args.phase
    rows_all, acked_all = [], {}
    sequence = []

    def reconf(nodes, label):
        def run():
            r = gw({"type": "LOGICAL_REPLICAS", "prefix": prefix, "nodes": nodes})
            sequence.append((label, r))
        return run

    events = []
    for i, (r, nodes) in enumerate(steps[1:], start=1):
        events.append((phase_len * i, reconf(nodes, f"R={r}")))
    total = phase_len * len(steps)
    rows, acked, t0 = run_clients("replication", "transparent", args.clients, total, [part], "transparent", args.seed,
                                  write_share=0.5, events=events)
    ROWS.extend(rows)
    for i, (r, _) in enumerate(steps):
        ph = [x for x in rows if phase_len * i + 0.5 <= x["t_start_s"] < phase_len * (i + 1)]
        w = [x["latency_ms"] for x in ph if x["ok"] and x["op"] == "apply"]
        g = [x["latency_ms"] for x in ph if x["ok"] and x["op"] == "get"]
        errs = sum(1 for x in ph if not x["ok"])
        readers = {}
        for x in ph:
            if x["ok"] and x["op"] == "get":
                readers[x["served_by"]] = readers.get(x["served_by"], 0) + 1
        add("replication", f"R={r}#{i}", "write_mean_ms", stat(w)["mean"], "ms")
        add("replication", f"R={r}#{i}", "read_mean_ms", stat(g)["mean"], "ms")
        add("replication", f"R={r}#{i}", "errors", errs, "count")
        add("replication", f"R={r}#{i}", "replicas_serving_reads", len(readers), "count")
        print(f"   phase {i} R={r}: write {stat(w)['mean']} ms, read {stat(g)['mean']} ms, errors {errs}, readers {readers}")
    for label, r in sequence:
        add("replication", label, "freeze_ms", r.get("freeze_ms"), "ms")
        add("replication", label, "ops_copied", r.get("ops_copied"), "count")
    lost = lost_writes(acked, lambda p: placement_of(f"acct.C{p:03d}"))
    v = gw({"type": "LOGICAL_VERIFY", "prefix": prefix})
    add("replication", "all", "lost_writes", lost, "count")
    add("replication", "all", "replicas_converged", v["converged"], "bool")
    DETAIL["replication"] = {"reconfigurations": sequence, "verify": v}
    print(f"   lost acknowledged writes: {lost}   replicas converged: {v['converged']}   reconfigurations: "
          f"{[(l, r.get('freeze_ms')) for l, r in sequence]}")


def t4_migration(args):
    print("\n[H9-4] Migration transparency: move a partition under load (transparent vs direct clients)")
    pl = fresh(1)
    pt, pd = 1, 2   # pt: used by transparent clients, pd: used by direct clients (placement snapshot taken now)
    snapshot = dict(pl)
    prefixes = {pt: f"acct.C{pt:03d}", pd: f"acct.C{pd:03d}"}
    spare = lambda part: next(n for n in NODES if n not in placement_of(prefixes[part]))
    moved = {}

    def migrate_both():
        t = time.time()
        for part in (pt, pd):
            moved[part] = gw({"type": "LOGICAL_MIGRATE", "prefix": prefixes[part],
                              "from": placement_of(prefixes[part])[0], "to": spare(part)})
        moved["span"] = (t, time.time())

    total = args.phase * 3
    holder = {}   # both client groups run at the same time, each against its own partition

    def group(kind, part, dp):
        holder[kind] = run_clients("migration", kind, args.clients // 2 or 1, total, [part], kind, args.seed,
                                   write_share=0.5, direct_placement=dp,
                                   events=[(args.phase, migrate_both)] if kind == "transparent" else None)

    ts = [threading.Thread(target=group, args=("transparent", pt, None)),
          threading.Thread(target=group, args=("direct", pd, snapshot))]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    (rows_t, acked_t, t0_t), (rows_d, acked_d, t0_d) = holder["transparent"], holder["direct"]
    ROWS.extend(rows_t + rows_d)
    mig_lo = moved["span"][0] - t0_t
    mig_hi = moved["span"][1] - t0_t
    for kind, rows in (("transparent", rows_t), ("direct", rows_d)):
        for name, lo, hi in (("before", 0.3, mig_lo), ("during", mig_lo, mig_hi + 0.3), ("after", mig_hi + 0.3, total)):
            s = phase_stats(rows, lo, hi)
            for key in ("requests", "errors", "error_pct", "mean", "p95", "max"):
                add("migration", f"{kind}:{name}", key, s[key])
        a = phase_stats(rows, mig_hi + 0.3, total)
        print(f"   {kind:<11} after the move: {a['requests']} requests, {a['errors']} errors ({a['error_pct']} %)")
    mt = moved[pt]
    add("migration", "transparent", "freeze_ms", mt.get("freeze_ms"), "ms")
    add("migration", "transparent", "migration_total_ms", mt.get("total_ms"), "ms")
    add("migration", "transparent", "ops_copied", mt.get("ops_copied"), "count")
    lost = lost_writes(acked_t, lambda p: placement_of(prefixes[pt]))
    add("migration", "transparent", "lost_writes", lost, "count")
    add("migration", "transparent", "replicas_converged", gw({"type": "LOGICAL_VERIFY", "prefix": prefixes[pt]})["converged"], "bool")
    print(f"   migration: freeze {mt.get('freeze_ms')} ms, total {mt.get('total_ms')} ms, {mt.get('ops_copied')} operations copied, lost writes {lost}")
    DETAIL["migration"] = {"moves": {str(k): v for k, v in moved.items() if k != "span"}}


def t5_failure(args):
    print("\n[H9-5] Failure transparency: a replica node crashes under load (transparent vs direct clients)")
    pl = fresh(2)
    victim = "NODE-4"
    pt, pd = 3, 6
    # both partitions live on [victim, other]; the direct client always talks to the first replica
    place(f"acct.C{pt:03d}", [victim, "NODE-5"])
    place(f"acct.C{pd:03d}", [victim, "NODE-3"])
    gw({"type": "LOGICAL_INVALIDATE"})
    snapshot = {f"acct.C{pt:03d}": [victim, "NODE-5"], f"acct.C{pd:03d}": [victim, "NODE-3"]}
    state = {"crash_at": None, "up_at": None}

    def crash():
        state["crash_at"] = time.time()
        node(victim, {"type": "FAULT_INJECT", "mode": "crash"}, timeout=3)

    def monitor(stop):
        while not stop.is_set():
            try:
                ok = not node(victim, {"type": "PROBE"}, timeout=0.4).get("error")
            except Exception:
                ok = False
            if state["crash_at"] and ok and state.get("down") and not state["up_at"]:
                state["up_at"] = time.time()
            if state["crash_at"] and not ok:
                state["down"] = True
            time.sleep(0.1)

    total = args.phase * 4
    stop_mon = threading.Event()
    threading.Thread(target=monitor, args=(stop_mon,), daemon=True).start()
    holder = {}

    def group(kind, part, dp):
        holder[kind] = run_clients("failure", kind, args.clients // 2 or 1, total, [part], kind, args.seed + 1,
                                   write_share=0.5, direct_placement=dp,
                                   events=[(args.phase, crash)] if kind == "transparent" else None)

    ts = [threading.Thread(target=group, args=("transparent", pt, None)),
          threading.Thread(target=group, args=("direct", pd, snapshot))]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    stop_mon.set()
    (rows_t, acked_t, t0), (rows_d, acked_d, _) = holder["transparent"], holder["direct"]
    ROWS.extend(rows_t + rows_d)
    lo = state["crash_at"] - t0
    hi = (state["up_at"] or state["crash_at"] + 3) - t0
    outage = round(hi - lo, 2)
    add("failure", "victim", "node_outage_s", outage, "s")
    for kind, rows in (("transparent", rows_t), ("direct", rows_d)):
        for name, a, b in (("before", 0.3, lo), ("during", lo, hi), ("after", hi + 0.5, total)):
            s = phase_stats(rows, a, b)
            for key in ("requests", "errors", "error_pct", "mean", "p95", "max"):
                add("failure", f"{kind}:{name}", key, s[key])
        d = phase_stats(rows, lo, hi)
        print(f"   {kind:<11} during the {outage}s outage: {d['requests']} requests, {d['errors']} errors ({d['error_pct']} %)")
    # repair: wait until the gateway has re-synchronised the restarted replica
    t_wait = time.time()
    converged, v = False, {}
    while time.time() - t_wait < 40:
        try:
            v = gw({"type": "LOGICAL_VERIFY", "prefix": f"acct.C{pt:03d}"})
            st = gw({"type": "LOGICAL_STATUS"})
            if v["converged"] and not any(s.startswith(f"acct.C{pt:03d}@") for s in st["stale"]):
                converged = True
                break
        except Exception:
            pass
        time.sleep(0.5)
    add("failure", "transparent", "repair_after_run_s", round(time.time() - t_wait, 2) if converged else None, "s")
    add("failure", "transparent", "replicas_converged", converged, "bool")
    lost = lost_writes(acked_t, lambda p: [victim, "NODE-5"])
    add("failure", "transparent", "lost_writes", lost, "count")
    st = gw({"type": "LOGICAL_STATUS"})["stats"]
    add("failure", "transparent", "repaired_ops", st["repaired_ops"], "count")
    print(f"   repair: converged={converged} after {round(time.time() - t_wait, 2)}s, repaired ops {st['repaired_ops']}, lost writes {lost}")
    DETAIL["failure"] = {"verify": v, "gateway_stats": st}


def t6_overhead(args):
    print("\n[H9-6] Overhead of transparency: direct physical access vs gateway with R = 1, 2, 3")
    parts = list(range(PARTS))
    variants = [("direct", 1), ("gateway", 1), ("gateway", 2), ("gateway", 3)]
    for mix, share in (("read", 0.0), ("write", 1.0)):
        for kind, r in variants:
            pl = fresh(r)
            label = "direct" if kind == "direct" else f"gateway_R{r}"
            snap = {p: v for p, v in pl.items()}
            for part in parts:                      # warm-up so every account exists and caches are hot
                for cust in customers_of(part)[:5]:
                    TransparentClient().apply(cust, 1.0, 1.0, f"warm-{mix}-{label}-{cust}")
            rows, _, t0 = run_clients("overhead", f"{mix}:{label}", args.clients, args.phase, parts,
                                      "direct" if kind == "direct" else "transparent", args.seed,
                                      write_share=share, direct_placement=snap)
            ROWS.extend(rows)
            ok = [r_ for r_ in rows if r_["ok"] and r_["t_start_s"] >= 0.5]
            s = stat([r_["latency_ms"] for r_ in ok])
            tput = round(len(ok) / max(0.1, args.phase - 0.5), 1)
            add("overhead", f"{mix}:{label}", "mean_ms", s["mean"], "ms")
            add("overhead", f"{mix}:{label}", "p95_ms", s["p95"], "ms")
            add("overhead", f"{mix}:{label}", "throughput_rps", tput, "req/s")
            print(f"   {mix:<5} {label:<11} mean {s['mean']:>7} ms  p95 {s['p95']:>7} ms  throughput {tput:>7} req/s")
    # resolution cost: the first operation after a cache flush versus a warm one
    fresh(2)
    c = TransparentClient()
    cold, warm = [], []
    for i in range(30):
        gw({"type": "LOGICAL_INVALIDATE"})
        cold.append(c.get(f"C{(i % PARTS) * 100:05d}").get("t_resolve_ms", 0.0))
        warm.append(c.get(f"C{(i % PARTS) * 100:05d}").get("t_resolve_ms", 0.0))
    add("overhead", "resolution", "cold_resolve_ms", stat(cold)["mean"], "ms")
    add("overhead", "resolution", "warm_resolve_ms", stat(warm)["mean"], "ms")
    print(f"   name resolution: cold {stat(cold)['mean']} ms, warm {stat(warm)['mean']} ms")


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
    p.add_argument("--experiments", default="all", help="comma list of access,location,replication,migration,failure,overhead")
    p.add_argument("--clients", type=int, default=8)
    p.add_argument("--phase", type=float, default=4.0, help="seconds per phase")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--quick", action="store_true")
    args = p.parse_args()
    if args.quick:
        args.phase = 2.5
    table = {"access": t1_access, "location": t2_location, "replication": t3_replication,
             "migration": t4_migration, "failure": t5_failure, "overhead": t6_overhead}
    names = list(table) if args.experiments == "all" else [x.strip() for x in args.experiments.split(",")]
    os.makedirs(RESULTS_DIR, exist_ok=True)
    print("=" * 78)
    print("WEEK 9 - TRANSPARENCY AND RELIABILITY (logical resources vs physical access)")
    print("=" * 78)
    for n in names:
        table[n](args)
    for row in SUMMARY:
        row["value_num"], row["param_num"] = numeric(row["value"]), param_of(row["variant"])
        row["series"] = series_of(row["variant"])
    write_csv(os.path.join(RESULTS_DIR, "week9_summary.csv"), SUMMARY,
              ["experiment", "variant", "series", "metric", "value", "value_num", "param_num", "unit"])
    write_csv(os.path.join(RESULTS_DIR, "week9_requests.csv"), ROWS,
              ["experiment", "variant", "client", "op", "partition", "t_start_s", "t_end_s", "ok", "latency_ms", "served_by", "error"])
    env = {"platform": platform.platform(), "python": platform.python_version(), "cpu_count": os.cpu_count(),
           "in_docker": os.path.exists("/.dockerenv"), "date": time.strftime("%Y-%m-%d %H:%M:%S"), "args": vars(args),
           "partitions": PARTS}
    with open(os.path.join(RESULTS_DIR, "week9_experiment_summary.json"), "w") as f:
        json.dump({"environment": env, "summary": SUMMARY, "details": DETAIL}, f, indent=2, default=str)
    print("\nSaved: results/week9_summary.csv, week9_requests.csv, week9_experiment_summary.json")


if __name__ == "__main__":
    main()
