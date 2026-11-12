import os, sys, time, psutil
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.communication import send_json
from common.config import RATING_NODES


def find_pid(substr):
    for proc in psutil.process_iter(["pid", "cmdline"]):
        cmd = " ".join(proc.info["cmdline"] or [])
        if substr in cmd:
            return proc.info["pid"]
    return None


def ping_all():
    # PROBE, not PING: keeps this diagnostic script from being counted as
    # a bully-election follower by whichever node currently holds the
    # Coordinator role (see nodes/node*_rating.py handle()).
    for node in ("NODE-3", "NODE-4", "NODE-5", "NODE-6"):
        try:
            host, port = RATING_NODES[node]
            r = send_json(host, port, {"type": "PROBE", "from_id": 0}, timeout=1.0)
            print(f"  {node}: {r}")
        except Exception as e:
            print(f"  {node}: unreachable ({e})")


def main():
    print("=" * 74)
    print("WEEK 4 — LEADER FAILURE & RE-ELECTION")
    print("=" * 74)
    print("\n[1] Before failure:")
    ping_all()
    pid = find_pid("node6_rating")
    if pid:
        print(f"\n[2] Killing NODE-6 (PID={pid})")
        try:
            p = psutil.Process(pid)
            p.terminate()
            p.wait(timeout=3)
        except Exception as e:
            print(f"   kill failed: {e}")
    print("\n[3] Waiting 8 seconds for re-election ...")
    time.sleep(8)
    print("\n[4] After re-election:")
    ping_all()
    print("\nExpected: NODE-5 is the new leader.")


if __name__ == "__main__":
    main()
