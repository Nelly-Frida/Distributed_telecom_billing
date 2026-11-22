import csv, os, threading, time, psutil
from common.config import DASHBOARD_DIR


class SystemMonitor(threading.Thread):
    def __init__(self, interval=5.0):
        super().__init__(daemon=True)
        self.interval = interval
        self.running = True
        self.process = psutil.Process()
        self.path = os.path.join(DASHBOARD_DIR, "system.csv")
        os.makedirs(DASHBOARD_DIR, exist_ok=True)
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="") as f:
                csv.writer(f).writerow(["timestamp", "cpu_percent", "mem_mb"])

    def run(self):
        while self.running:
            time.sleep(self.interval)
            with open(self.path, "a", newline="") as f:
                csv.writer(f).writerow([
                    round(time.time(), 3),
                    self.process.cpu_percent(interval=0.1),
                    round(self.process.memory_info().rss / (1024*1024), 2),
                ])

    def stop(self):
        self.running = False
