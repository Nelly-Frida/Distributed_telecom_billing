"""NODE-2: billing front-end, 2PC coordinator and (Week 7) failure-aware router.

Each request may carry ft_mode:
    "baseline"   original Week 3-6 behaviour: plain round-robin dispatcher, one
                 ledger, no failure detection, no failover
    "resilient"  failure detector + failover + replicated ledger (default,
                 set with FT_DEFAULT_MODE)
"""
import socket, json, os, sys, time, threading
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.config import (BIND_HOST, NODE2_PORT, ACTIVE_RATING_NODES, RATING_NODES, LEDGER_REPLICAS,
                           RESULTS_DIR, REQUEST_TIMEOUT, FT_DEFAULT_MODE, FT_PROBE_INTERVAL,
                           FT_PROBE_TIMEOUT, FT_MISS_THRESHOLD, FT_ATTEMPT_TIMEOUT, FT_MAX_ATTEMPTS,
                           FT_POLICY)
from common.fault_state import FaultState
from fault_tolerance.failure_detector import FailureDetector
from fault_tolerance.resilient_dispatcher import ResilientDispatcher
from processing.dispatcher import Dispatcher
from transactions.two_phase_commit import TwoPhaseCommitCoordinator

HOST = BIND_HOST
PORT = NODE2_PORT
NODE_ID = "NODE-2"

dispatcher = Dispatcher()                 # baseline
tx_coordinator = TwoPhaseCommitCoordinator()
faults = FaultState(NODE_ID)


def _on_node_recovered(name):
    """A participant came back: re-drive pending 2PC and (for a ledger) replay missed commits."""
    try:
        redriven = tx_coordinator.recover_pending()
        if redriven.get("count"):
            detector._event(name, "REDRIVE", f"re-drove {redriven['count']} pending transaction(s)")
        if name.startswith("LEDGER"):
            result = tx_coordinator.resync_ledger()
            detector._event(name, "RESYNC", json.dumps(result))
    except Exception as exc:
        detector._event(name, "RECOVERY_ERROR", str(exc))


targets = {n: RATING_NODES[n] for n in ACTIVE_RATING_NODES}
targets.update({name: addr for name, addr in LEDGER_REPLICAS})
detector = FailureDetector(targets, interval=FT_PROBE_INTERVAL, timeout=FT_PROBE_TIMEOUT,
                           miss_threshold=FT_MISS_THRESHOLD,
                           csv_path=os.path.join(RESULTS_DIR, "week7_detector_events.csv"),
                           on_recovered=_on_node_recovered)
resilient = ResilientDispatcher(detector)


def startup_recovery():
    """After a coordinator restart: resolve in-flight 2PC and re-align ledgers."""
    time.sleep(3.0)
    try:
        recovered = tx_coordinator.recover_pending()
        resync = tx_coordinator.resync_ledger()
        detector._event(NODE_ID, "STARTUP_RECOVERY",
                        f"recovered={recovered.get('count', 0)} resync={json.dumps(resync)}")
        print(f"[{NODE_ID}] startup recovery: {recovered.get('count', 0)} transaction(s); resync {resync}")
    except Exception as exc:
        detector._event(NODE_ID, "STARTUP_RECOVERY_FAILED", str(exc))


def anti_entropy():
    """Background repair: while a ledger replica is lagging and reachable, resynchronise it.

    Needed because a transaction that was in flight when a replica came back can
    commit on the other replica only, after the one-shot recovery resync took its snapshot."""
    ledger_names = [n for n, _ in LEDGER_REPLICAS]
    tick = 0
    while True:
        time.sleep(1.0)
        tick += 1
        try:
            if tx_coordinator.ledger_dirty and all(detector.status(n) == "UP" for n in ledger_names):
                result = tx_coordinator.resync_ledger()
                if result.get("healed") or result.get("orphans_cleaned"):
                    detector._event("LEDGER", "ANTI_ENTROPY", json.dumps(result))
            # A transaction that was still running when a recovery pass took its snapshot is skipped by
            # that pass; if it then ends IN_DOUBT, re-drive it once every participant is healthy.
            if tick % 2 == 0 and all(detector.status(n) == "UP" for n in targets):
                redriven = tx_coordinator.recover_pending()
                if redriven.get("count"):
                    detector._event("NODE-2", "REDRIVE_PERIODIC", f"re-drove {redriven['count']} pending transaction(s)")
        except Exception as exc:
            detector._event("LEDGER", "ANTI_ENTROPY_ERROR", str(exc))


def route(request):
    kind = request.get("type")
    mode = request.get("ft_mode", FT_DEFAULT_MODE)
    if kind == "TX_BILL":
        tx_request = {k: request[k] for k in ("request_id", "transaction_id", "customer_id", "service", "usage") if k in request}
        inject = request.get("inject_failure", False)
        if mode == "resilient":
            return tx_coordinator.execute_resilient(tx_request, resilient,
                                                    rating_node=request.get("rating_node"), inject_failure=inject)
        return tx_coordinator.execute(tx_request, rating_node=request.get("rating_node"),
                                      inject_failure=inject, replicate=False)
    if kind == "TX_RECOVER":
        return tx_coordinator.recover_pending()
    if kind == "TX_STATUS":
        return tx_coordinator.status(request["transaction_id"])
    if kind == "TX_RESYNC":
        return tx_coordinator.resync_ledger()
    if kind == "TX_PENDING":
        return {"pending": tx_coordinator.pending_count()}
    if kind == "TX_CONSISTENCY":
        return tx_coordinator.consistency_report()
    if kind == "FT_STATUS":
        return {"default_mode": FT_DEFAULT_MODE, "detector": detector.snapshot(),
                "dispatcher": resilient.snapshot(),
                "config": {"request_timeout_s": REQUEST_TIMEOUT, "attempt_timeout_s": FT_ATTEMPT_TIMEOUT,
                           "max_attempts": FT_MAX_ATTEMPTS, "policy": FT_POLICY,
                           "probe_interval_s": FT_PROBE_INTERVAL, "miss_threshold": FT_MISS_THRESHOLD,
                           "ledger_replicas": [n for n, _ in LEDGER_REPLICAS]}}
    if kind == "PROBE":
        return {"ok": True, "node": NODE_ID}
    # ordinary billing request
    if mode == "resilient":
        if request.get("ft_verify"):
            return resilient.process_verified(request)
        return resilient.process_request(request, policy=request.get("policy"))
    return dispatcher.process_request(request)


def handle(connection):
    connection.settimeout(60)
    try:
        buffer = b""
        while b"\n" not in buffer:
            chunk = connection.recv(65536)
            if not chunk:
                break
            buffer += chunk
        if not buffer:
            return
        request = json.loads(buffer.split(b"\n", 1)[0].decode())
        if faults.is_control(request):
            connection.sendall((json.dumps(faults.control(request)) + "\n").encode())
            return
        action = faults.intercept(request)
        if action:
            if action[0] == "drop":
                return
            if action[0] == "omit":
                time.sleep(action[1])
                return
            if action[0] == "error":
                connection.sendall((json.dumps({"error": action[1], "node": NODE_ID}) + "\n").encode())
                return
        start = time.perf_counter()
        response = route(request)
        total_time = time.perf_counter() - start
        response["billing_node"] = NODE_ID
        response["billing_processing_time"] = total_time
        response["billing_pid"] = os.getpid()
        connection.sendall((json.dumps(response) + "\n").encode())
    except Exception as error:
        try:
            connection.sendall((json.dumps({"error": str(error), "node": NODE_ID}) + "\n").encode())
        except Exception:
            pass
    finally:
        connection.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(512)

    print(f"{NODE_ID} Billing Server running on {HOST}:{PORT}")
    print(f"PID: {os.getpid()}  default ft_mode={FT_DEFAULT_MODE}")
    print(f"Active rating nodes: {dispatcher.node_names}  ledger replicas: {[n for n, _ in LEDGER_REPLICAS]}")

    detector.start()
    threading.Thread(target=startup_recovery, daemon=True).start()
    threading.Thread(target=anti_entropy, daemon=True).start()

    # One thread per connection so a slow or stalled request never blocks others.
    while True:
        connection, _ = server.accept()
        threading.Thread(target=handle, args=(connection,), daemon=True).start()


if __name__ == "__main__":
    main()
