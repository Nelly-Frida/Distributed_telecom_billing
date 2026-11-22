import json
import socket

<<<<<<< HEAD
def send_json(host, port, message, timeout=5.0):
    data = (json.dumps(message) + "\n").encode("utf-8")

=======

def send_json(host, port, message, timeout=5.0):
    data = (json.dumps(message) + "\n").encode("utf-8")
>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect((host, port))
        client.sendall(data)
<<<<<<< HEAD

=======
>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a
        buffer = b""
        while b"\n" not in buffer:
            chunk = client.recv(4096)
            if not chunk:
                break
            buffer += chunk
<<<<<<< HEAD

    if not buffer:
        raise ConnectionError("No response received")

    response = buffer.split(b"\n", 1)[0]
    return json.loads(response.decode("utf-8"))
=======
    if not buffer:
        raise ConnectionError("No response received")
    return json.loads(buffer.split(b"\n", 1)[0].decode("utf-8"))
>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a
