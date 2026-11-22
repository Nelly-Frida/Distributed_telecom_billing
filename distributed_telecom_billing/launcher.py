<<<<<<< HEAD
import os
import subprocess
import sys
import time

=======
"""Start every distributed process locally, with an optional supervisor.

The supervisor is the local equivalent of Docker's `restart: unless-stopped`:
when a node crashes (for example through a Week 7 crash fault) it is started
again after RESTART_DELAY seconds, and the crash/restart is written to
results/week7_supervisor_events.csv so recovery time can be measured.

    python launcher.py                  # supervised (default)
    python launcher.py --no-supervise   # a crashed node stays down
"""
import argparse, csv, os, subprocess, sys, time
>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

<<<<<<< HEAD
from common.config import ACTIVE_RATING_NODES

PROCESS_MAP = {
    "NODE-2": "nodes/node2_billing.py",
    "NODE-3": "nodes/node3_rating.py",
    "NODE-4": "nodes/node4_rating.py",
    "NODE-5": "nodes/node5_rating.py",
    "NODE-6": "nodes/node6_rating.py"
}

def start_process(node_name):
    script = os.path.join(PROJECT_ROOT, PROCESS_MAP[node_name])
    env = os.environ.copy()
    env["PYTHONPATH"] = PROJECT_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    process = subprocess.Popen([sys.executable, script], env=env)
    print(f"Created process for {node_name} (PID={process.pid})")
    return process

def main():
    processes = []

    print("=" * 60)
    print("DISTRIBUTED TELECOM BILLING")
    print("PROCESS LAUNCHER")
    print("=" * 60)

    process = start_process("NODE-2")
    processes.append(process)
    time.sleep(1)

    for node in ACTIVE_RATING_NODES:
        process = start_process(node)
        processes.append(process)
        time.sleep(0.5)

    print("\nAll distributed processes created.")
    print("Press CTRL+C to terminate.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nTerminating distributed processes...")
        for process in processes:
            process.terminate()
        for process in processes:
            process.wait()
        print("All processes terminated.")
=======
from common.config import ACTIVE_RATING_NODES, LEDGER_REPLICATION_ENABLED, RESULTS_DIR

PROCESS_MAP = {
    "NODE-2": ("nodes/node2_billing.py", {}),
    "NODE-3": ("nodes/node3_rating.py", {}),
    "NODE-4": ("nodes/node4_rating.py", {}),
    "NODE-5": ("nodes/node5_rating.py", {}),
    "NODE-6": ("nodes/node6_rating.py", {}),
    "BILLING-API": ("api/billing_api.py", {}),
    "LEDGER-1": ("transactions/ledger_node.py", {}),
    "LEDGER-2": ("transactions/ledger_node.py",
                 {"LEDGER_PORT": "5008", "LEDGER_NODE_ID": "LEDGER-2",
                  "LEDGER_DB_PATH": "data/ledger_replica_transactions.db"}),
}


def spawn(name):
    script, extra_env = PROCESS_MAP[name]
    env = os.environ.copy()
    env["PYTHONPATH"] = PROJECT_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    env["PYTHONUNBUFFERED"] = "1"
    env.update(extra_env)
    return subprocess.Popen([sys.executable, script], env=env, cwd=PROJECT_ROOT)


def start(name):
    p = spawn(name)
    print(f"  -> {name:<12} PID={p.pid}")
    return p


def log_supervisor(event):
    os.makedirs(os.path.join(PROJECT_ROOT, RESULTS_DIR), exist_ok=True)
    path = os.path.join(PROJECT_ROOT, RESULTS_DIR, "week7_supervisor_events.csv")
    fresh = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if fresh:
            w.writerow(["timestamp", "node", "event", "exit_code", "new_pid"])
        w.writerow(event)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-supervise", action="store_true")
    parser.add_argument("--restart-delay", type=float, default=float(os.getenv("RESTART_DELAY", "1.0")))
    args = parser.parse_args()
    supervise = not args.no_supervise and os.getenv("SUPERVISE", "1") != "0"

    procs = {}
    print("=" * 60)
    print("DISTRIBUTED TELECOM BILLING - ASSIGNMENT 3 / WEEKS 5 TO 8")
    print("=" * 60)

    print("\n[1/4] Distributed ledger (primary" + (" + replica)" if LEDGER_REPLICATION_ENABLED else ")"))
    procs["LEDGER-1"] = start("LEDGER-1")
    if LEDGER_REPLICATION_ENABLED:
        procs["LEDGER-2"] = start("LEDGER-2")
    time.sleep(0.5)
    print("\n[2/4] Rating nodes (leader election enabled)")
    for n in ACTIVE_RATING_NODES:
        procs[n] = start(n); time.sleep(0.4)
    print("\n[3/4] Billing node + transaction coordinator + failure detector")
    procs["NODE-2"] = start("NODE-2"); time.sleep(0.8)
    print("\n[4/4] Python billing API (HTTP :8000)")
    procs["BILLING-API"] = start("BILLING-API")

    print("\nSystem up.  Supervisor:", "ON (crashed nodes restart after "
          f"{args.restart_delay}s)" if supervise else "OFF (crashed nodes stay down)")
    print("Start Apache HTTP Server on port 80 if you use the HTTP gateway (see apache/README.md).")
    print("Experiments:")
    print("  python experiments/week5_transactions.py | week5_transaction_load.py | week6_concurrency.py")
    print("  python experiments/week7_fault_tolerance.py")
    print("  python experiments/week8_system_models.py")
    print("For container deployment, use: docker compose up --build -d")
    print("\nPress CTRL+C to terminate.")

    restart_at = {}
    try:
        while True:
            time.sleep(0.2)
            now = time.time()
            for name, p in list(procs.items()):
                code = p.poll()
                if code is None:
                    continue
                if name not in restart_at:
                    print(f"  !! {name} exited with code {code}")
                    log_supervisor([round(now, 4), name, "EXITED", code, ""])
                    restart_at[name] = now + args.restart_delay
                if supervise and now >= restart_at[name]:
                    procs[name] = start(name)
                    log_supervisor([round(time.time(), 4), name, "RESTARTED", code, procs[name].pid])
                    del restart_at[name]
    except KeyboardInterrupt:
        print("\nTerminating...")
        for p in procs.values():
            if p.poll() is None:
                p.terminate()
        for p in procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        print("Done.")

>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a

if __name__ == "__main__":
    main()
