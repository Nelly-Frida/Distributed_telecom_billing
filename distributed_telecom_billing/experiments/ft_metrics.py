"""Shared reliability metrics for the Week 7 and Week 8 experiments.

Availability = T_uptime / T_total (assignment, section 4.7).

The run is cut into fixed-width time buckets. A bucket is UP when requests
completed in it and at least `threshold` of them were both successful and
correct; a bucket with no completions delivered no service and is DOWN.
Availability is the share of UP buckets. This is a service-level definition
(what a client experiences), not a process-level one.

MTTR is measured from the moment a fault is injected until the service has
been UP for `stable` consecutive buckets after its first DOWN bucket. It
therefore includes detection time. A fault that never makes a bucket DOWN is
reported as masked (MTTR = 0): the failure was hidden from clients.
"""
import math


def percentile(values, p):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((p / 100.0) * (len(ordered) - 1)))))
    return ordered[index]


def bucketize(rows, total_s, width=0.5, threshold=0.95):
    """rows: dicts with t_end_s, ok, correct. Returns one dict per bucket."""
    count = max(1, int(math.ceil(total_s / width)))
    buckets = [{"bucket_start_s": round(i * width, 3), "completed": 0, "good": 0, "failed": 0, "wrong": 0}
               for i in range(count)]
    for r in rows:
        i = int(r["t_end_s"] // width)
        if 0 <= i < count:
            b = buckets[i]
            b["completed"] += 1
            if r["ok"] and r["correct"]:
                b["good"] += 1
            elif r["ok"]:
                b["wrong"] += 1
            else:
                b["failed"] += 1
    for b in buckets:
        b["up"] = bool(b["completed"] and b["good"] / b["completed"] >= threshold)
    return buckets


def availability(buckets, start_s=0.0, end_s=None):
    chosen = [b for b in buckets if b["bucket_start_s"] >= start_s and (end_s is None or b["bucket_start_s"] < end_s)]
    if not chosen:
        return None
    return sum(b["up"] for b in chosen) / len(chosen)


def mttr(buckets, inject_s, width=0.5, stable=2):
    after = [b for b in buckets if b["bucket_start_s"] + width > inject_s]
    first_down = next((i for i, b in enumerate(after) if not b["up"]), None)
    if first_down is None:
        return {"masked": True, "mttr_s": 0.0, "outage_s": 0.0}
    outage = sum(1 for b in after[first_down:] if not b["up"]) * width
    for j in range(first_down + 1, len(after) - stable + 1):
        if all(after[k]["up"] for k in range(j, j + stable)):
            return {"masked": False, "mttr_s": round(max(0.0, after[j]["bucket_start_s"] - inject_s), 3),
                    "outage_s": round(outage, 3)}
    return {"masked": False, "mttr_s": None, "outage_s": round(outage, 3)}


def summarize_rows(rows):
    """Request-level view of a list of per-request rows."""
    total = len(rows)
    if not total:
        return {"requests": 0, "success_pct": 0.0, "wrong_pct": 0.0, "failed_pct": 0.0,
                "mean_latency_ms": 0.0, "p95_latency_ms": 0.0}
    ok = [r for r in rows if r["ok"] and r["correct"]]
    wrong = [r for r in rows if r["ok"] and not r["correct"]]
    lat = [r["latency_ms"] for r in ok] or [0.0]
    return {"requests": total, "success_pct": round(100.0 * len(ok) / total, 3),
            "wrong_pct": round(100.0 * len(wrong) / total, 3),
            "failed_pct": round(100.0 * (total - len(ok) - len(wrong)) / total, 3),
            "mean_latency_ms": round(sum(lat) / len(lat), 3), "p95_latency_ms": round(percentile(lat, 95), 3)}
