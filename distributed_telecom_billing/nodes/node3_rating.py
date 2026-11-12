import socket
import json
import time
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from rating.rating_engine import RatingEngine

HOST = "127.0.0.1"
PORT = 5003
NODE_ID = "NODE-3"

engine = RatingEngine()

def process_request(request):
    start = time.perf_counter()
    charge = engine.calculate_charge(request["service"], request["usage"])
    processing_time = time.perf_counter() - start

    return {
        "request_id": request["request_id"],
        "customer_id": request["customer_id"],
        "charge": charge,
        "node": NODE_ID,
        "processing_time": processing_time,
        "pid": os.getpid()
    }

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((HOST, PORT))
server.listen(50)

print(f"{NODE_ID} running on {HOST}:{PORT} PID={os.getpid()}")

while True:
    connection, address = server.accept()

    try:
        buffer = b""
        while b"\n" not in buffer:
            chunk = connection.recv(4096)
            if not chunk:
                break
            buffer += chunk

        if not buffer:
            continue

        request = json.loads(buffer.split(b"\n", 1)[0].decode())
        response = process_request(request)
        connection.sendall((json.dumps(response) + "\n").encode())

    except Exception as error:
        response = {"error": str(error), "node": NODE_ID}
        connection.sendall((json.dumps(response) + "\n").encode())
    finally:
        connection.close()
