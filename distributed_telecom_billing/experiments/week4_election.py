import os, sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.communication import send_json
from common.config import RATING_NODES


def ping(node_name):
    # PROBE is a read-only external health check. It is intentionally a
    # different message type from the internal "PING" the bully-election
    # heartbeat protocol uses between real peers (ids 3-6), so running
    # this script never registers a phantom follower in coordination.csv.
    host, port = RATING_NODES[node_name]
    try:
        return send_json(host, port, {"type": "PROBE", "from_id": 0}, timeout=1.0)
    except Exception as e:
        return {"error": str(e)}


def main():
    print("=" * 74)
    print("WEEK 4 — BULLY LEADER ELECTION")
    print("=" * 74)
    for node in ("NODE-3", "NODE-4", "NODE-5", "NODE-6"):
        print(f"  {node}: {ping(node)}")
    print("\nExpected leader: NODE-6 (highest id).")


if __name__ == "__main__":
    main()
