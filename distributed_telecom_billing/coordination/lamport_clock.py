import threading


class LamportClock:
    def __init__(self, node_id):
        self.node_id = node_id
        self.time = 0
        self._lock = threading.Lock()

    def tick(self):
        with self._lock:
            self.time += 1
            return self.time

    def update(self, remote):
        with self._lock:
            self.time = max(self.time, int(remote or 0)) + 1
            return self.time

    def stamp(self):
        return {"lamport": self.tick(), "origin": self.node_id}
