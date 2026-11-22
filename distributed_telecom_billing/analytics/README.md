# Spark analytics

The PySpark jobs are intentionally separate from the transaction path (batch analysis of experiment outputs).

## Weeks 5 and 6
```powershell
docker compose --profile analytics run --rm spark-analytics
```
`transaction_analytics.py` aggregates `week5_transactions.csv` and `week6_concurrency.csv` into `results/spark_analytics/week5_*` and `week6_metrics`.

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
