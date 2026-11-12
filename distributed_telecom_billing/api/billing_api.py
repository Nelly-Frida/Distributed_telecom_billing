"""HTTP API — reverse-proxy target for Apache HTTP Server."""
import json, os, sys, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.communication import send_json
from common.config import NODE_HOST, NODE2_PORT, REQUEST_TIMEOUT, BILLING_API_PORT
from monitoring.metrics_logger import MetricsLogger

logger = MetricsLogger()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw.decode("utf-8"))

    def do_POST(self):
        if self.path.rstrip("/") == "/bill":
            try:
                req = self._read_json()
                t0 = time.perf_counter()
                response = send_json(NODE_HOST, NODE2_PORT, req,
                                     timeout=REQUEST_TIMEOUT)
                latency_ms = (time.perf_counter() - t0) * 1000
                response["api_latency_ms"] = round(latency_ms, 3)
                logger.record_transaction(req, response)
                self._send(200, response)
            except Exception as e:
                self._send(500, {"error": str(e)})
        else:
            self._send(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"ok": True, "service": "billing-api"})
        elif self.path == "/stats":
            self._send(200, logger.snapshot())
        elif self.path == "/export":
            self._send(200, {"ok": True, "files": logger.export_all()})
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, *args):
        pass


def main():
    server = ThreadingHTTPServer((NODE_HOST, BILLING_API_PORT), Handler)
    print(f"[billing-api] http://{NODE_HOST}:{BILLING_API_PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
