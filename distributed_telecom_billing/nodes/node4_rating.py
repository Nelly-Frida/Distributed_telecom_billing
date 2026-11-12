"""Rating node with Lamport clock + Bully election (Week 4)."""
import socket, json, time, os, sys, threading
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from rating.rating_engine import RatingEngine
from coordination.lamport_clock import LamportClock
from coordination.bully_election import BullyElection
from coordination.coordinator import Coordinator
from transactions.rating_participant import RatingTransactionParticipant
from common.config import BIND_HOST, ADVERTISE_HOST, RATING_NODES

HOST, PORT = BIND_HOST, 5004
NODE_ID    = "NODE-4"
MY_ID      = 4

engine  = RatingEngine()
tx_participant = RatingTransactionParticipant(engine, os.getenv("RATING_DB_PATH", "data/node4_rating_transactions.db"))
clock   = LamportClock(NODE_ID)
elector = None
coord   = None


def process_request(request):
    start  = time.perf_counter()
    charge = engine.calculate_charge(request["service"], request["usage"])
    processing_time = time.perf_counter() - start
    return {
        "request_id": request["request_id"],
        "customer_id": request["customer_id"],
        "charge": charge,
        "node": NODE_ID,
        "processing_time": processing_time,
        "pid": os.getpid(),
    }


def handle(connection):
    global coord
    try:
        buffer = b""
        while b"\n" not in buffer:
            chunk = connection.recv(4096)
            if not chunk:
                break
            buffer += chunk
        if not buffer:
            return
        request = json.loads(buffer.split(b"\n", 1)[0].decode())
        clock.update(request.get("lamport", 0))

        if request.get("type") == "TX_PREPARE":
            response = tx_participant.prepare(request)
        elif request.get("type") == "TX_COMMIT":
            response = tx_participant.commit(request["transaction_id"])
        elif request.get("type") == "TX_ABORT":
            response = tx_participant.abort(request["transaction_id"])
        elif request.get("type") == "TX_STATUS":
            response = tx_participant.status(request["transaction_id"])
        elif request.get("type") == "ELECTION" and elector:
            elector.receive_election(request["from_id"], request["from_addr"])
            response = {"ok": True, "node": NODE_ID}
        elif request.get("type") == "COORDINATOR" and elector:
            elector.receive_coordinator(request["leader_id"], request["leader_addr"])
            response = {"ok": True, "node": NODE_ID}
        elif request.get("type") == "PING":
            # Internal bully-election heartbeat channel only.
            # Real follower ids are always >= 3, so this is safe from
            # collision with the external diagnostic PROBE below.
            if coord:
                coord.note_heartbeat(request.get("from_id", "?"))
            response = {"ok": True, "node": NODE_ID, "my_id": MY_ID}
        elif request.get("type") == "PROBE":
            # External health-check (used by experiment scripts).
            # Deliberately does NOT touch the coordinator heartbeat
            # table, so manual probing never shows up in
            # coordination.csv as a phantom follower failure.
            response = {"ok": True, "node": NODE_ID, "my_id": MY_ID,
                        "leader_id": elector.leader_id if elector else None}
        else:
            response = process_request(request)

        response["lamport"] = clock.tick()
        response["origin"] = NODE_ID
        connection.sendall((json.dumps(response) + "\n").encode())
    except Exception as error:
        try:
            connection.sendall(
                (json.dumps({"error": str(error), "node": NODE_ID}) + "\n").encode()
            )
        except Exception:
            pass
    finally:
        connection.close()


def main():
    global elector, coord
    peer_ids = [3, 4, 5, 6]
    all_peers = [(i, RATING_NODES[f"NODE-{i}"]) for i in peer_ids if i != MY_ID]
    higher_peers = [(i, addr) for i, addr in all_peers if i > MY_ID]

    def _on_leader_change(leader_id, leader_addr):
        global coord
        if leader_id == MY_ID and coord is None:
            coord = Coordinator(NODE_ID)
            coord.start()

    elector = BullyElection(
        node_id=NODE_ID, my_id=MY_ID,
        higher_peers=higher_peers,
        my_addr=(ADVERTISE_HOST, PORT),
        on_leader_change=_on_leader_change,
        all_peers=all_peers,
    )
    elector.start()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(200)
    print(f"{NODE_ID} running on {HOST}:{PORT} advertised={ADVERTISE_HOST}:{PORT} PID={os.getpid()}")

    while True:
        connection, _ = srv.accept()
        threading.Thread(target=handle, args=(connection,), daemon=True).start()


if __name__ == "__main__":
    main()
