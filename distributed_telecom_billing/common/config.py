<<<<<<< HEAD
NODE_HOST = "127.0.0.1"

NODE1_PORT = 5001
NODE2_PORT = 5002

RATING_NODES = {
    "NODE-3": (NODE_HOST, 5003),
    "NODE-4": (NODE_HOST, 5004),
    "NODE-5": (NODE_HOST, 5005),
    "NODE-6": (NODE_HOST, 5006),
}

# Baseline: ["NODE-3"]
# Proposed 2-node: ["NODE-3", "NODE-4"]
# Proposed 4-node: ["NODE-3", "NODE-4", "NODE-5", "NODE-6"]
ACTIVE_RATING_NODES = ["NODE-3", "NODE-4"]

REQUEST_TIMEOUT = 5.0
WORKLOAD_SIZE = 1000
=======
"""Environment-aware network configuration for local and Docker deployments."""
import os

BIND_HOST = os.getenv("BIND_HOST", "127.0.0.1")
ADVERTISE_HOST = os.getenv("ADVERTISE_HOST", "127.0.0.1")
NODE_HOST = os.getenv("NODE_HOST", "127.0.0.1")
NODE1_PORT = int(os.getenv("NODE1_PORT", "5001"))
NODE2_PORT = int(os.getenv("NODE2_PORT", "5002"))

RATING_NODES = {
    "NODE-3": (os.getenv("NODE3_HOST", "127.0.0.1"), int(os.getenv("NODE3_PORT", "5003"))),
    "NODE-4": (os.getenv("NODE4_HOST", "127.0.0.1"), int(os.getenv("NODE4_PORT", "5004"))),
    "NODE-5": (os.getenv("NODE5_HOST", "127.0.0.1"), int(os.getenv("NODE5_PORT", "5005"))),
    "NODE-6": (os.getenv("NODE6_HOST", "127.0.0.1"), int(os.getenv("NODE6_PORT", "5006"))),
}
ACTIVE_RATING_NODES = [n.strip() for n in os.getenv("ACTIVE_RATING_NODES", "NODE-3,NODE-4,NODE-5,NODE-6").split(",") if n.strip()]

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "5.0"))
WORKLOAD_SIZE = int(os.getenv("WORKLOAD_SIZE", "1000"))
USE_HTTP_GATEWAY = True
APACHE_HOST = os.getenv("APACHE_HOST", "127.0.0.1")
APACHE_PORT = int(os.getenv("APACHE_PORT", "80"))
BILLING_API_HOST = os.getenv("BILLING_API_HOST", "127.0.0.1")
BILLING_API_PORT = int(os.getenv("BILLING_API_PORT", "8000"))
DASHBOARD_DIR = os.getenv("DASHBOARD_DIR", "powerbi/dashboard_data")
RESULTS_DIR = os.getenv("RESULTS_DIR", "results")
LEADER_ELECTION_ENABLED = True
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "2.0"))
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "5.0"))
LEDGER_HOST = os.getenv("LEDGER_HOST", "127.0.0.1")
LEDGER_PORT = int(os.getenv("LEDGER_PORT", "5007"))
TX_COORDINATOR_ENABLED = True

# ---------------------------------------------------------------------------
# Week 7 — fault tolerance (failure detection, failover, ledger replication)
# ---------------------------------------------------------------------------
LEDGER_REPLICA_HOST = os.getenv("LEDGER_REPLICA_HOST", "127.0.0.1")
LEDGER_REPLICA_PORT = int(os.getenv("LEDGER_REPLICA_PORT", "5008"))
LEDGER_REPLICATION_ENABLED = os.getenv("LEDGER_REPLICATION_ENABLED", "1") == "1"
LEDGER_REPLICAS = [("LEDGER-1", (LEDGER_HOST, LEDGER_PORT))]
if LEDGER_REPLICATION_ENABLED:
    LEDGER_REPLICAS.append(("LEDGER-2", (LEDGER_REPLICA_HOST, LEDGER_REPLICA_PORT)))

# "resilient" = failure detector + failover + replicated ledger (proposed system)
# "baseline" = original Week 3-6 behaviour (no detector, no failover, one ledger)
FT_DEFAULT_MODE = os.getenv("FT_DEFAULT_MODE", "resilient")
FT_PROBE_INTERVAL = float(os.getenv("FT_PROBE_INTERVAL", "0.5"))
FT_PROBE_TIMEOUT = float(os.getenv("FT_PROBE_TIMEOUT", "0.5"))
FT_MISS_THRESHOLD = int(os.getenv("FT_MISS_THRESHOLD", "3"))
FT_ATTEMPT_TIMEOUT = float(os.getenv("FT_ATTEMPT_TIMEOUT", "1.5"))
FT_MAX_ATTEMPTS = int(os.getenv("FT_MAX_ATTEMPTS", "3"))
FT_POLICY = os.getenv("FT_POLICY", "round_robin")  # round_robin | least_loaded

# ---------------------------------------------------------------------------
# Week 8 — emulated node capacity so system-model comparisons are reproducible
# ---------------------------------------------------------------------------
NODE_SERVICE_SLOTS = int(os.getenv("NODE_SERVICE_SLOTS", "1"))

>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a
