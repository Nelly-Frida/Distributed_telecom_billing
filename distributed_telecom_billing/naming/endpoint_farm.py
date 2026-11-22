"""A farm of lightweight endpoints for the broadcast scalability experiment (Week 10).

    python -m naming.endpoint_farm --base-port 6100 --count 32

Endpoint k owns the partition  acct.C<k:03d>  and answers WHO_HAS over a real TCP socket, so
broadcasting to N endpoints costs N real round trips, not N function calls.
"""
import argparse
import json
import socket
import threading
import time


def serve(port, owned_prefix):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(256)

    def handle(conn):
        try:
            buf = b""
            while b"\n" not in buf:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                buf += chunk
            req = json.loads(buf.split(b"\n", 1)[0].decode())
            has = req.get("type") == "WHO_HAS" and req.get("name", "").startswith(owned_prefix)
            conn.sendall((json.dumps({"has": has, "epoch": 1, "node": f"F{port}"}) + "\n").encode())
        finally:
            conn.close()

    while True:
        c, _ = srv.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base-port", type=int, default=6100)
    p.add_argument("--count", type=int, default=16)
    a = p.parse_args()
    for k in range(a.count):
        threading.Thread(target=serve, args=(a.base_port + k, f"acct.C{k:03d}"), daemon=True).start()
    print(f"endpoint farm: {a.count} endpoints on ports {a.base_port}-{a.base_port + a.count - 1}", flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
