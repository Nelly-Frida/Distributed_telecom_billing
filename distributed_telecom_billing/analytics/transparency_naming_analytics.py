"""Spark batch analytics for Assignment 3, Weeks 9 and 10.

Reads the raw per-operation rows written by the experiments and recomputes the headline numbers
independently of the Python experiment code:

  Week 9   results/week9_requests.csv  -> latency and error rate per experiment and variant, a one-second
                                          error timeline (the migration / crash story), overhead per operation,
                                          and a cross-check of the mean latency against the experiment's summary
  Week 10  results/week10_lookups.csv  -> latency percentiles, messages per lookup and correctness per
                                          resolution strategy, and a cross-check against the experiment's summary

Usage:  spark-submit transparency_naming_analytics.py RESULTS_DIR OUTPUT_DIR
Output: CSV part files under OUTPUT_DIR/<table>/ (one folder per table).
"""
import os
import sys
from pyspark.sql import SparkSession, functions as F


def truthy(col):
    return F.lower(F.col(col).cast("string")) == "true"


def save(df, out, name):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/{name}")


def pct(q, col="latency_ms"):
    return F.round(F.expr(f"percentile_approx(CASE WHEN ok_flag = 1 THEN {col} END, {q})"), 3)


def week9(spark, results, out):
    path = os.path.join(results, "week9_requests.csv")
    if not os.path.exists(path):
        print(f"[skip] {path} not found; run experiments/week9_transparency.py first")
        return
    df = (spark.read.option("header", True).option("inferSchema", True).csv(path)
          .withColumn("ok_flag", truthy("ok").cast("int"))
          .withColumn("second", F.floor("t_start_s").cast("int")))

    metrics = (df.groupBy("experiment", "variant").agg(
        F.count("*").alias("requests"),
        F.sum(1 - F.col("ok_flag")).alias("errors"),
        F.round(100 * (1 - F.avg("ok_flag")), 2).alias("error_pct"),
        F.round(F.avg(F.when(F.col("ok_flag") == 1, F.col("latency_ms"))), 3).alias("mean_latency_ms"),
        pct(0.50).alias("p50_latency_ms"), pct(0.95).alias("p95_latency_ms"), pct(0.99).alias("p99_latency_ms"),
        F.round(F.max(F.when(F.col("ok_flag") == 1, F.col("latency_ms"))), 3).alias("max_latency_ms"),
    ).orderBy("experiment", "variant"))
    print("\n=== WEEK 9: LATENCY AND ERRORS PER EXPERIMENT AND VARIANT ===")
    metrics.show(60, truncate=False)
    save(metrics, out, "week9_request_metrics")

    timeline = (df.filter(F.col("experiment").isin("migration", "failure", "replication"))
                  .groupBy("experiment", "variant", "second").agg(
                      F.count("*").alias("requests"), F.sum(1 - F.col("ok_flag")).alias("errors"),
                      F.round(100 * (1 - F.avg("ok_flag")), 2).alias("error_pct"),
                      F.round(F.avg(F.when(F.col("ok_flag") == 1, F.col("latency_ms"))), 3).alias("mean_latency_ms"))
                  .orderBy("experiment", "variant", "second"))
    print("\n=== WEEK 9: ERROR TIMELINE (one-second buckets; the event happens at the bucket where direct errors start) ===")
    timeline.filter(F.col("experiment") == "migration").show(30, truncate=False)
    save(timeline, out, "week9_error_timeline")

    overhead = (df.filter((F.col("experiment") == "overhead") & (F.col("t_start_s") >= 0.5))
                  .groupBy("variant", "op").agg(
                      F.count("*").alias("requests"), F.round(F.avg("latency_ms"), 3).alias("mean_ms"),
                      pct(0.50).alias("p50_ms"), pct(0.95).alias("p95_ms"), pct(0.99).alias("p99_ms"))
                  .orderBy("variant"))
    print("\n=== WEEK 9: OVERHEAD OF TRANSPARENCY PER VARIANT (direct physical access versus gateway R=1,2,3) ===")
    overhead.show(20, truncate=False)
    save(overhead, out, "week9_overhead")

    summary_path = os.path.join(results, "week9_summary.csv")
    if os.path.exists(summary_path):
        py = (spark.read.option("header", True).option("inferSchema", True).csv(summary_path)
              .filter((F.col("experiment") == "overhead") & (F.col("metric") == "mean_ms"))
              .select("variant", F.col("value").cast("double").alias("experiment_mean_ms")))
        check = (overhead.join(py, "variant")
                 .withColumn("difference_ms", F.round(F.col("mean_ms") - F.col("experiment_mean_ms"), 3))
                 .select("variant", "mean_ms", "experiment_mean_ms", "difference_ms").orderBy("variant"))
        print("\n=== WEEK 9: CROSS-CHECK (Spark versus the experiment script; differences are rounding only) ===")
        check.show(20, truncate=False)
        save(check, out, "week9_overhead_check")


def week10(spark, results, out):
    path = os.path.join(results, "week10_lookups.csv")
    if not os.path.exists(path):
        print(f"[skip] {path} not found; run experiments/week10_naming_processes.py first")
        return
    df = (spark.read.option("header", True).option("inferSchema", True).csv(path)
          .withColumn("ok_flag", truthy("correct").cast("int")))
    metrics = (df.groupBy("strategy").agg(
        F.count("*").alias("lookups"),
        F.round(F.avg("latency_ms"), 4).alias("mean_ms"),
        F.round(F.expr("percentile_approx(latency_ms, 0.50)"), 4).alias("p50_ms"),
        F.round(F.expr("percentile_approx(latency_ms, 0.95)"), 4).alias("p95_ms"),
        F.round(F.expr("percentile_approx(latency_ms, 0.99)"), 4).alias("p99_ms"),
        F.round(F.avg("messages"), 3).alias("messages_per_lookup"),
        F.round(100 * F.avg((F.col("messages") == 0).cast("int")), 2).alias("zero_message_pct"),
        F.round(100 * F.avg("ok_flag"), 2).alias("correct_pct"),
    ).orderBy("mean_ms"))
    print("\n=== WEEK 10: RESOLUTION STRATEGIES (sorted from fastest) ===")
    metrics.show(20, truncate=False)
    save(metrics, out, "week10_resolution_metrics")

    by_msgs = (df.groupBy("strategy", "messages").agg(
        F.count("*").alias("lookups"), F.round(F.avg("latency_ms"), 4).alias("mean_ms")).orderBy("strategy", "messages"))
    print("\n=== WEEK 10: LATENCY BY NUMBER OF MESSAGES (what one extra message costs) ===")
    by_msgs.show(30, truncate=False)
    save(by_msgs, out, "week10_latency_by_messages")

    summary_path = os.path.join(results, "week10_summary.csv")
    if os.path.exists(summary_path):
        py = (spark.read.option("header", True).option("inferSchema", True).csv(summary_path)
              .filter((F.col("experiment") == "resolution") & (F.col("metric") == "mean_ms"))
              .select(F.col("variant").alias("strategy"), F.col("value").cast("double").alias("experiment_mean_ms")))
        check = (metrics.join(py, "strategy").select("strategy", "mean_ms", "experiment_mean_ms")
                 .withColumn("difference_ms", F.round(F.col("mean_ms") - F.col("experiment_mean_ms"), 4)).orderBy("strategy"))
        print("\n=== WEEK 10: CROSS-CHECK (Spark versus the experiment script) ===")
        check.show(20, truncate=False)
        save(check, out, "week10_resolution_check")


def main():
    if len(sys.argv) < 3:
        raise SystemExit("Usage: transparency_naming_analytics.py RESULTS_DIR OUTPUT_DIR")
    results, out = sys.argv[1], sys.argv[2]
    spark = SparkSession.builder.appName("TelecomBillingWeek9Week10Analytics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    week9(spark, results, out)
    week10(spark, results, out)
    spark.stop()


if __name__ == "__main__":
    main()
