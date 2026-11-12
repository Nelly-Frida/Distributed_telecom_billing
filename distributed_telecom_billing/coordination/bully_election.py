import threading, time
from common.communication import send_json


class BullyElection(threading.Thread):
    def __init__(self, node_id, my_id, higher_peers, my_addr,
                 heartbeat_interval=2.0, heartbeat_timeout=5.0,
                 on_leader_change=None):
        super().__init__(daemon=True)
        self.node_id = node_id
        self.my_id = my_id
        self.higher_peers = higher_peers
        self.my_addr = my_addr
        self.hb_interval = heartbeat_interval
        self.hb_timeout = heartbeat_timeout
        self.leader_id = None
        self.leader_addr = None
        self.last_hb = time.time()
        self.on_leader_change = on_leader_change or (lambda *a, **k: None)
        self.running = True
        self.elections_started = 0

    def receive_election(self, from_id, from_addr):
        if self.my_id > from_id:
            self._start_election()

    def receive_coordinator(self, leader_id, leader_addr):
        self.leader_id = leader_id
        self.leader_addr = leader_addr
        self.last_hb = time.time()
        self.on_leader_change(leader_id, leader_addr)

    def run(self):
        time.sleep(2.0)
        self._start_election()
        while self.running:
            time.sleep(self.hb_interval)
            if self.leader_addr is None:
                continue
            try:
                send_json(self.leader_addr[0], self.leader_addr[1],
                          {"type": "PING", "from_id": self.my_id}, timeout=1.0)
                self.last_hb = time.time()
            except Exception:
                if time.time() - self.last_hb > self.hb_timeout:
                    self.leader_id = None
                    self.leader_addr = None
                    self._start_election()

    def _start_election(self):
        self.elections_started += 1
        answered = False
        for peer_id, peer_addr in self.higher_peers:
            try:
                send_json(peer_addr[0], peer_addr[1], {
                    "type": "ELECTION",
                    "from_id": self.my_id,
                    "from_addr": self.my_addr,
                }, timeout=1.0)
                answered = True
            except Exception:
                continue
        if not answered:
            self.leader_id = self.my_id
            self.leader_addr = self.my_addr
            self.on_leader_change(self.my_id, self.my_addr)
            self._broadcast_coordinator()

    def _broadcast_coordinator(self):
        for peer_id, peer_addr in self.higher_peers:
            try:
                send_json(peer_addr[0], peer_addr[1], {
                    "type": "COORDINATOR",
                    "leader_id": self.my_id,
                    "leader_addr": self.my_addr,
                }, timeout=1.0)
            except Exception:
                pass

    def stop(self):
        self.running = False
