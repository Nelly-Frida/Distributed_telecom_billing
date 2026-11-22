import time, random, threading
import concurrent.futures
from common.communication import send_json
from common.http_client import post_json
from common.config import (
    NODE_HOST, NODE2_PORT, APACHE_HOST, APACHE_PORT,
    USE_HTTP_GATEWAY, WORKLOAD_SIZE, REQUEST_TIMEOUT,
)

SERVICES = ["voice", "sms", "data"]


def create_request(request_id, run_tag=None):
    service = random.choice(SERVICES)
    if service == "voice":
        usage = random.uniform(10, 300)
    elif service == "sms":
        usage = random.uniform(1, 50)
    else:
        usage = random.uniform(10, 1000)
    request = {
        "request_id": request_id,
        "customer_id": f"C{request_id:05d}",
        "service": service,
        "usage": usage,
        "timestamp": time.time(),
    }
    if run_tag:
        request["run_tag"] = run_tag
    return request


def _send(request):
    if USE_HTTP_GATEWAY:
        url = f"http://{APACHE_HOST}:{APACHE_PORT}/api/bill"
        return post_json(url, request, timeout=REQUEST_TIMEOUT)
    return send_json(NODE_HOST, NODE2_PORT, request, timeout=REQUEST_TIMEOUT)


def run_workload(number_of_requests=WORKLOAD_SIZE, seed=42):
    """Original sequential runner. Kept as-is (unchanged behavior) so the
    Week 3 results already collected with it stay reproducible. One
    request at a time - fine at n=1000, far too slow at n=100000."""
    random.seed(seed)
    results = []
    start_experiment = time.perf_counter()
    for request_id in range(1, number_of_requests + 1):
        request = create_request(request_id)
        request_start = time.perf_counter()
        try:
            response = _send(request)
            latency = time.perf_counter() - request_start
            response["end_to_end_latency"] = latency
            response["success"] = "error" not in response
        except Exception as error:
            latency = time.perf_counter() - request_start
            response = {
                "request_id": request_id,
                "success": False,
                "error": str(error),
                "end_to_end_latency": latency,
            }
        results.append(response)
    total_time = time.perf_counter() - start_experiment
    return results, total_time


def run_workload_concurrent(number_of_requests=WORKLOAD_SIZE, seed=42,
                             concurrency=50, run_tag=None, progress_every=5000):
    """Concurrent runner for realistic large-scale workloads (e.g. 100k
    requests). Fires `concurrency` requests at once through a thread
    pool, modeling many simultaneous customers hitting the real pipeline
    (Apache -> billing_api -> NODE-2 -> dispatcher -> rating node), not a
    simulation - every one of these is a genuine socket round trip."""
    random.seed(seed)
    requests_list = [create_request(rid, run_tag) for rid in range(1, number_of_requests + 1)]
    results = [None] * number_of_requests
    completed = 0
    progress_lock = threading.Lock()
    start_experiment = time.perf_counter()

    def _worker(idx, request):
        nonlocal completed
        request_start = time.perf_counter()
        try:
            response = _send(request)
            latency = time.perf_counter() - request_start
            response["end_to_end_latency"] = latency
            response["success"] = "error" not in response
        except Exception as error:
            latency = time.perf_counter() - request_start
            response = {
                "request_id": request["request_id"],
                "success": False,
                "error": str(error),
                "end_to_end_latency": latency,
            }
        results[idx] = response
        with progress_lock:
            completed += 1
            if completed % progress_every == 0 or completed == number_of_requests:
                elapsed = time.perf_counter() - start_experiment
                rate = completed / elapsed if elapsed else 0
                print(f"    ... {completed:,}/{number_of_requests:,} "
                      f"({completed/number_of_requests*100:.1f}%) "
                      f"@ {rate:.1f} req/s so far")

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(_worker, i, req) for i, req in enumerate(requests_list)]
        concurrent.futures.wait(futures)

    total_time = time.perf_counter() - start_experiment
    return results, total_time


if __name__ == "__main__":
    results, total_time = run_workload()
    print(f"Completed workload in {total_time:.3f} seconds")
