import os
import subprocess
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

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

if __name__ == "__main__":
    main()
