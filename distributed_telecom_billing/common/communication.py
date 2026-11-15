import json
import socket


def send_json(host, port, message, timeout=5.0):
    data = (json.dumps(message) + "\n").encode("utf-8")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect((host, port))
        client.sendall(data)
        buffer = b""
        while b"\n" not in buffer:
            chunk = client.recv(4096)
            if not chunk:
                break
            buffer += chunk
    if not buffer:
        raise ConnectionError("No response received")
    return json.loads(buffer.split(b"\n", 1)[0].decode("utf-8"))
