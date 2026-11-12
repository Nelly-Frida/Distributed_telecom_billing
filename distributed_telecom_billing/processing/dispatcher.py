import time

from common.communication import send_json
from common.config import RATING_NODES, ACTIVE_RATING_NODES, REQUEST_TIMEOUT

class Dispatcher:
    def __init__(self):
        self.nodes = [RATING_NODES[node] for node in ACTIVE_RATING_NODES]
        self.node_names = ACTIVE_RATING_NODES.copy()
        self.next_node = 0

        self.node_statistics = {
            node: {
                "requests": 0,
                "failures": 0,
                "total_processing_time": 0.0,
                "total_latency": 0.0
            }
            for node in self.node_names
        }

    def select_node(self):
        if not self.nodes:
            raise RuntimeError("No rating nodes configured.")

        index = self.next_node % len(self.nodes)
        self.next_node += 1
        return self.node_names[index], self.nodes[index]

    def process_request(self, request):
        node_name, node_address = self.select_node()
        host, port = node_address
        start = time.perf_counter()

        try:
            response = send_json(host, port, request, timeout=REQUEST_TIMEOUT)
            latency = time.perf_counter() - start

            self.node_statistics[node_name]["requests"] += 1
            self.node_statistics[node_name]["total_processing_time"] += response.get("processing_time", 0)
            self.node_statistics[node_name]["total_latency"] += latency

            response["dispatcher_latency"] = latency
            response["selected_node"] = node_name
            return response

        except Exception as error:
            self.node_statistics[node_name]["failures"] += 1
            return {
                "request_id": request["request_id"],
                "error": str(error),
                "selected_node": node_name
            }

    def get_statistics(self):
        return self.node_statistics
