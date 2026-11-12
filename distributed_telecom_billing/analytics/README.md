# Spark analytics

The PySpark batch job is intentionally separate from the transaction path. Run the two practical experiments first, then execute:

```powershell
docker compose --profile analytics run --rm spark-analytics
```

It aggregates the generated Week 5 transaction records and Week 6 concurrency records into CSV part files under `results/spark_analytics/`.
