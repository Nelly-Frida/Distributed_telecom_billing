"""Rating node with Lamport clock + Bully election (Week 4)."""
import socket, json, time, os, sys, threading
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from rating.rating_engine import RatingEngine
from coordination.lamport_clock import LamportClock
from coordination.bully_election import BullyElection
from coordination.coordinator import Coordinator

HOST, PORT = "127.0.0.1", 5006
NODE_ID    = "NODE-6"
MY_ID      = 6

engine  = RatingEngine()
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

        if request.get("type") == "ELECTION" and elector:
            elector.receive_election(request["from_id"], request["from_addr"])
            response = {"ok": True, "node": NODE_ID}
        elif request.get("type") == "COORDINATOR" and elector:
            elector.receive_coordinator(request["leader_id"], request["leader_addr"])
            response = {"ok": True, "node": NODE_ID}
        elif request.get("type") == "PING":
            if coord:
                coord.note_heartbeat(request.get("from_id", "?"))
            response = {"ok": True, "node": NODE_ID, "my_id": MY_ID}
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
    higher_peers = []

    def _on_leader_change(leader_id, leader_addr):
        global coord
        if leader_id == MY_ID:
            coord = Coordinator(NODE_ID)
            coord.start()

    elector = BullyElection(
        node_id=NODE_ID, my_id=MY_ID,
        higher_peers=higher_peers, my_addr=(HOST, PORT),
        on_leader_change=_on_leader_change,
    )
    elector.start()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(50)
    print(f"{NODE_ID} running on {HOST}:{PORT} PID={os.getpid()}")

    while True:
        connection, _ = srv.accept()
        threading.Thread(target=handle, args=(connection,), daemon=True).start()


if __name__ == "__main__":
    main()
