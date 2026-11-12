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
