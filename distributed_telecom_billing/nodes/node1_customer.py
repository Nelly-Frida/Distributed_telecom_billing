import time
import random
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.communication import send_json
from common.config import NODE_HOST, NODE2_PORT, WORKLOAD_SIZE, REQUEST_TIMEOUT

SERVICES = ["voice", "sms", "data"]

def create_request(request_id):
    service = random.choice(SERVICES)
    if service == "voice":
        usage = random.uniform(10, 300)
    elif service == "sms":
        usage = random.uniform(1, 50)
    else:
        usage = random.uniform(10, 1000)

    return {
        "request_id": request_id,
        "customer_id": f"C{request_id:05d}",
        "service": service,
        "usage": usage,
        "timestamp": time.time()
    }

def run_workload(number_of_requests=WORKLOAD_SIZE, seed=42):
    random.seed(seed)
    results = []
    start_experiment = time.perf_counter()

    for request_id in range(1, number_of_requests + 1):
        request = create_request(request_id)
        request_start = time.perf_counter()

        try:
            response = send_json(
                NODE_HOST, NODE2_PORT, request,
                timeout=REQUEST_TIMEOUT
            )
            latency = time.perf_counter() - request_start
            response["end_to_end_latency"] = latency
            response["success"] = "error" not in response
        except Exception as error:
            latency = time.perf_counter() - request_start
            response = {
                "request_id": request_id,
                "success": False,
                "error": str(error),
                "end_to_end_latency": latency
            }

        results.append(response)

    total_time = time.perf_counter() - start_experiment
    return results, total_time

if __name__ == "__main__":
    results, total_time = run_workload()
    print(f"Completed workload in {total_time:.3f} seconds")
