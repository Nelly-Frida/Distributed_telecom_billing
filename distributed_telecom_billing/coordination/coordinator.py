import threading, time, os, csv
from common.config import HEARTBEAT_TIMEOUT, DASHBOARD_DIR


class Coordinator(threading.Thread):
    def __init__(self, node_id, csv_path=None):
        super().__init__(daemon=True)
        self.node_id = node_id
        self.followers = {}
        self.running = True
        self.csv_path = csv_path or os.path.join(DASHBOARD_DIR, "coordination.csv")
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                csv.writer(f).writerow(["timestamp", "event", "node", "details"])

    def note_heartbeat(self, follower_id):
        self.followers[follower_id] = time.time()

    def _log(self, event, node, details=""):
        with open(self.csv_path, "a", newline="") as f:
            csv.writer(f).writerow([round(time.time(), 3), event, node, details])

    def run(self):
        while self.running:
            time.sleep(1.0)
            now = time.time()
            for fid, last in list(self.followers.items()):
                if now - last > HEARTBEAT_TIMEOUT:
                    self._log("failure_detected", fid,
                              f"missed for {round(now-last,1)}s")
                    self.followers.pop(fid, None)

    def stop(self):
        self.running = False
