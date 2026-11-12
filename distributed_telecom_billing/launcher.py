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
    print("DISTRIBUTED TELECOM BILLING - WEEKS 3 & 4 LAUNCHER")
    print("=" * 60)

    print("\n[1/3] Billing node")
    procs.append(start("NODE-2")); time.sleep(0.8)

    print("\n[2/3] Rating nodes (leader election enabled)")
    for n in ACTIVE_RATING_NODES:
        procs.append(start(n)); time.sleep(0.4)

    print("\n[3/3] Python billing API (HTTP :8000)")
    procs.append(start("BILLING-API"))

    print("\nSystem up.")
    print("Start Apache HTTP Server on port 80 (see apache/README.md).")
    print("Then run one of the experiments:")
    print("  python experiments/week3_architecture.py")
    print("  python experiments/week4_election.py")
    print("  python experiments/week4_failure.py")
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
