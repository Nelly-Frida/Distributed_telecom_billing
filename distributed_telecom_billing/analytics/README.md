# Spark analytics

The PySpark jobs are intentionally separate from the transaction path (batch analysis of experiment outputs).

## Weeks 5 and 6
```powershell
docker compose --profile analytics run --rm spark-analytics
```
`transaction_analytics.py` aggregates `week5_transactions.csv` and `week6_concurrency.csv` into `results/spark_analytics/week5_*` and `week6_metrics`.

## Weeks 9 and 10
Run the two experiments first, then:
```powershell
docker compose --profile analytics910 run --rm spark-analytics-week9-10
# or, with Java and pyspark installed locally:
python analytics/transparency_naming_analytics.py results results/spark_analytics
```
`transparency_naming_analytics.py RESULTS_DIR OUTPUT_DIR` reads the raw rows and writes:

| Table (folder in `OUTPUT_DIR`) | Content |
|---|---|
| `week9_request_metrics` | requests, errors, error %, mean / p50 / p95 / p99 / max latency per experiment and variant (transparent versus direct) |
| `week9_error_timeline` | errors and latency per second for the migration, failure and replication experiments (the event shows as the second where the direct client starts failing) |
| `week9_overhead` | latency per variant and operation: direct physical access versus gateway R = 1, 2, 3 |
| `week9_overhead_check` | the same mean compared with the experiment summary; the difference must be 0.0 |
| `week10_resolution_metrics` | latency percentiles, messages per lookup, zero-message share and correctness per resolution strategy |
| `week10_latency_by_messages` | latency by number of messages (what one extra message costs) |
| `week10_resolution_check` | cross-check against the experiment summary |

## Weeks 7 and 8
Run the two experiments first, then:
```powershell
docker compose --profile analytics78 run --rm spark-analytics-week7-8
# or, with Java and pyspark installed locally:
python analytics/fault_model_analytics.py results results/spark_analytics
```
`fault_model_analytics.py RESULTS_DIR OUTPUT_DIR [BUCKET_SECONDS]` reads the raw per-request rows and writes:

| Table (folder in `OUTPUT_DIR`) | Content |
|---|---|
| `week7_phase_metrics` | success, wrong and failed share, mean and p95 latency per scenario, mode and phase (pre, fault, post) |
| `week7_availability` | availability recomputed from the raw rows (time buckets, 95 % good-request threshold), mean/min/max over repeats |
| `week7_availability_check` | the same compared with the experiment script; `difference_pp` must be 0.0 |
| `week7_failover_effectiveness` | requests served after failover, failed and wrong share during the fault |
| `week8_model_metrics` | throughput and latency percentiles per model, load type and client count (warm-up removed) |
| `week8_scalability` | throughput speed-up versus one client |
| `week8_service_latency` | latency by service class (voice, sms, data) per model |
| `week8_node_balance` | load imbalance (coefficient of variation) across the rating nodes |

Why Spark here: the raw files grow with every repeat and client count (about 50,000 rows for one Week 7 pass), and the same aggregations
(grouping, percentiles, window functions for speed-up, a time-bucket grid) are what a larger campaign would need. The independent
recomputation also acts as a check on the experiment code: it exposed a real measurement bug during development (see `logs/week7_engineering_log.md`, entry 5).
