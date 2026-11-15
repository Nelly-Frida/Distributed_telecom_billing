"""Narrated failure demo for the oral defence (Part E):
Failure -> Detection -> Recovery -> Service Restoration.

A steady client sends billing requests while one node is deliberately broken. Every
second the script prints what the client sees, and it prints each event of the failure
detector as it happens. Run it twice to contrast the two systems:

    python experiments/week7_live_demo.py --mode baseline
    python experiments/week7_live_demo.py --mode resilient
    python experiments/week7_live_demo.py --fault omission --mode resilient
    python experiments/week7_live_demo.py --fault crash --target LEDGER-1 --tx --mode resilient

Needs the system running (python launcher.py, or docker compose up -d).
"""
import argparse, os, random, sys, threading, time
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.communication import send_json
from common.config import LEDGER_REPLICAS, NODE2_PORT, NODE_HOST, RATING_NODES
from rating.rating_engine import RatingEngine

RATES = RatingEngine().rates


def addr(name):
    if name in RATING_NODES:
        return RATING_NODES[name]
    return dict(LEDGER_REPLICAS)[name]


def call(a, msg, timeout=5.0):
    return send_json(a[0], a[1], msg, timeout=timeout)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["baseline", "resilient"], default="resilient")
    p.add_argument("--fault", choices=["crash", "omission", "drop", "error", "byzantine"], default="crash")
    p.add_argument("--target", default="NODE-4")
    p.add_argument("--tx", action="store_true", help="send two-phase-commit transactions instead of plain billing")
    p.add_argument("--verify", action="store_true", help="use majority voting (Byzantine defence)")
    p.add_argument("--pre", type=float, default=4.0)
    p.add_argument("--fault-duration", type=float, default=6.0)
    p.add_argument("--post", type=float, default=6.0)
    args = p.parse_args()
    ft_mode = args.mode
    total = args.pre + args.fault_duration + args.post

    print("=" * 74)
    print(f"LIVE FAILURE DEMO  system={args.mode}  fault={args.fault} on {args.target}  "
          f"workload={'2PC transactions' if args.tx else 'billing requests'}")
    print("=" * 74)
    for n in list(RATING_NODES) + [x for x, _ in LEDGER_REPLICAS]:
        try:
            call(addr(n), {"type": "FAULT_CLEAR"}, timeout=2)
        except Exception:
            pass

    stats = {"ok": 0, "failed": 0, "wrong": 0, "served": {}, "failovers": 0}
    lock, stop = threading.Lock(), threading.Event()
    t0 = time.time()
    totals = {"ok": 0, "failed": 0, "wrong": 0}

    def client():
        rng, n = random.Random(1), 0
        while not stop.is_set():
            n += 1
            service, usage = rng.choice(["voice", "sms", "data"]), rng.randint(1, 50)
            msg = {"request_id": n, "customer_id": "DEMO", "service": service, "usage": usage, "ft_mode": ft_mode}
            if args.tx:
                msg.update(type="TX_BILL", transaction_id=f"DEMO-{int(t0)}-{n}")
            if args.verify:
                msg["ft_verify"] = True
            try:
                r = call((NODE_HOST, NODE2_PORT), msg, timeout=12)
                good = (r.get("status") == "COMMIT") if args.tx else ("error" not in r)
                correct = good and abs((r.get("charge") or 0) - RATES[service] * usage) < 1e-6
                with lock:
                    if correct:
                        stats["ok"] += 1
                        who = r.get("selected_node") or r.get("rating_node") or "?"
                        stats["served"][who] = stats["served"].get(who, 0) + 1
                        stats["failovers"] += 1 if (r.get("failover_attempts") or 0) > 0 or r.get("attempts", 1) > 1 else 0
                    elif good:
                        stats["wrong"] += 1
                    else:
                        stats["failed"] += 1
            except Exception:
                with lock:
                    stats["failed"] += 1
            time.sleep(0.05)

    seen_events = set()

    def show_events(label_t):
        try:
            ft = call((NODE_HOST, NODE2_PORT), {"type": "FT_STATUS"}, timeout=2)
        except Exception:
            return
        for e in ft["detector"]["events"]:
            key = (e["timestamp"], e["node"], e["event"])
            if key in seen_events or e["timestamp"] < t0:
                continue
            seen_events.add(key)
            print(f"   [detector t={e['timestamp'] - t0:5.1f}s] {e['node']:<9} {e['event']:<18} {str(e['detail'])[:60]}")

    thread = threading.Thread(target=client, daemon=True)
    thread.start()
    fault_epoch, injected, cleared = None, False, False
    degraded_from, restored_at = None, None
    last_print, last = 0.0, dict(ok=0, failed=0, wrong=0)
    while time.time() - t0 < total:
        now = time.time() - t0
        if not injected and now >= args.pre:
            injected = True
            fault_epoch = time.time()
            msg = {"type": "FAULT_INJECT", "mode": args.fault}
            if args.fault == "omission":
                msg["hold_s"] = 20
            call(addr(args.target), msg, timeout=3)
            print(f"\n>>> t={now:4.1f}s  FAULT INJECTED: {args.fault} on {args.target}\n")
        if injected and not cleared and now >= args.pre + args.fault_duration and args.fault != "crash":
            cleared = True
            call(addr(args.target), {"type": "FAULT_CLEAR"}, timeout=3)
            print(f"\n>>> t={now:4.1f}s  FAULT CLEARED on {args.target} (repair)\n")
        if now - last_print >= 1.0:
            last_print = now
            with lock:
                cur = dict(ok=stats["ok"], failed=stats["failed"], wrong=stats["wrong"])
            d = {k: cur[k] - last[k] for k in cur}
            last = cur
            state = "NORMAL" if not injected else ("DEGRADED" if d["failed"] or d["wrong"] else "SERVING")
            print(f"t={now:4.1f}s  last second: ok={d['ok']:<3} failed={d['failed']:<3} wrong={d['wrong']:<3}  [{state}]")
            show_events(now)
            if injected and (d["failed"] or d["wrong"]) and degraded_from is None:
                degraded_from = time.time() - fault_epoch
            if degraded_from is not None and restored_at is None and d["failed"] == 0 and d["wrong"] == 0 and d["ok"] > 0:
                restored_at = time.time() - fault_epoch
        time.sleep(0.1)
    stop.set()
    thread.join(timeout=15)

    print("\n" + "=" * 74)
    with lock:
        total_req = stats["ok"] + stats["failed"] + stats["wrong"]
        print(f"requests: {total_req}   correct: {stats['ok']}   failed: {stats['failed']}   wrong results: {stats['wrong']}"
              f"   served after failover: {stats['failovers']}")
        print(f"served by: {dict(sorted(stats['served'].items()))}")
    if degraded_from is None:
        print("Service impact: none visible to the client; the failure was masked (failover happened within a request).")
    elif restored_at is not None:
        print(f"Service impact: first degraded {degraded_from:.1f}s after injection, restored {restored_at:.1f}s after injection "
              f"(about {restored_at - degraded_from:.1f}s of degraded service at one-second resolution).")
    else:
        print("Service impact: degraded until the end of the demo; the system did not restore service by itself in this window.")
    print("Explain: failure injected -> detector events above show detection -> failover/restart -> service restored.")


if __name__ == "__main__":
    main()
