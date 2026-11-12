import os, subprocess, sys, time
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.config import ACTIVE_RATING_NODES

PROCESS_MAP = {
    "NODE-2": "nodes/node2_billing.py",
    "NODE-3": "nodes/node3_rating.py",
    "NODE-4": "nodes/node4_rating.py",
    "NODE-5": "nodes/node5_rating.py",
    "NODE-6": "nodes/node6_rating.py",
    "BILLING-API": "api/billing_api.py",
    "LEDGER": "transactions/ledger_node.py",
}


def start(node_name):
    script = PROCESS_MAP[node_name]
    env = os.environ.copy()
    env["PYTHONPATH"] = PROJECT_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    p = subprocess.Popen([sys.executable, script], env=env)
    print(f"  -> {node_name:<12} PID={p.pid}")
    return p


def main():
    procs = []
    print("=" * 60)
    print("DISTRIBUTED TELECOM BILLING - ASSIGNMENT 3 / WEEKS 5 TO 6")
    print("=" * 60)

    print("\n[1/4] Billing node + transaction coordinator")
    procs.append(start("NODE-2")); time.sleep(0.8)
    print("\n[2/4] Distributed ledger participant")
    procs.append(start("LEDGER")); time.sleep(0.5)

    print("\n[3/4] Rating nodes (leader election enabled)")
    for n in ACTIVE_RATING_NODES:
        procs.append(start(n)); time.sleep(0.4)

    print("\n[4/4] Python billing API (HTTP :8000)")
    procs.append(start("BILLING-API"))

    print("\nSystem up.")
    print("Start Apache HTTP Server on port 80 (see apache/README.md).")
    print("Then run one of the experiments:")
    print("  python experiments/week3_architecture.py")
    print("  python experiments/week4_election.py")
    print("  python experiments/week4_failure.py")
    print("  python experiments/week5_transactions.py")
    print("  python experiments/week5_transaction_load.py --transactions 100 --workers 10")
    print("  python experiments/week5_durability_check.py (after restarting services)")
    print("  python experiments/week6_concurrency.py")
    print("For container deployment, use: docker compose up --build -d")
    print("\nPress CTRL+C to terminate.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nTerminating...")
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait()
        print("Done.")


if __name__ == "__main__":
    main()
