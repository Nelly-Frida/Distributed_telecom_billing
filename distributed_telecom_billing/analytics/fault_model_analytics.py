"""Spark batch analytics for Assignment 3, Weeks 7 and 8.

Reads the raw per-request records written by the experiments and recomputes the
headline metrics independently of the Python experiment code:

  Week 7  results/week7_requests.csv   -> phase metrics, availability per scenario/mode,
                                          failover effectiveness, and a cross-check against
                                          the availability reported by the experiment
  Week 8  results/week8_requests.csv   -> latency percentiles and throughput per model and
                                          client count, latency by service class, and the
                                          load balance across rating nodes

Usage:  spark-submit fault_model_analytics.py RESULTS_DIR OUTPUT_DIR [BUCKET_SECONDS]
Output: CSV part files under OUTPUT_DIR/<table>/ (one folder per table).
"""
import os
import sys
from pyspark.sql import SparkSession, Window, functions as F


def truthy(col):
    return F.lower(F.col(col).cast("string")) == "true"


def save(df, out, name):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/{name}")


def week7(spark, results, out, width):
    path = os.path.join(results, "week7_requests.csv")
    if not os.path.exists(path):
        print(f"[skip] {path} not found; run experiments/week7_fault_tolerance.py first")
        return
    raw = spark.read.option("header", True).option("inferSchema", True).csv(path)
    if "repeat" not in raw.columns:
        raw = raw.withColumn("repeat", F.lit(1))
    df = (raw.withColumn("good", (truthy("ok") & truthy("correct")).cast("int"))
             .withColumn("wrong", (truthy("ok") & ~truthy("correct")).cast("int"))
             .withColumn("failed", (~truthy("ok")).cast("int"))
             .withColumn("failover", (F.col("failover_attempts") > 0).cast("int")))

    phase = (df.groupBy("scenario", "mode", "phase").agg(
        F.count("*").alias("requests"),
        F.round(100 * F.avg("good"), 3).alias("success_pct"),
        F.round(100 * F.avg("wrong"), 3).alias("wrong_pct"),
        F.round(100 * F.avg("failed"), 3).alias("failed_pct"),
        F.round(F.avg(F.when(F.col("good") == 1, F.col("latency_ms"))), 2).alias("mean_latency_ms"),
        F.round(F.expr("percentile_approx(CASE WHEN good = 1 THEN latency_ms END, 0.95)"), 2).alias("p95_latency_ms"),
        F.sum("failover").alias("failovers"),
    ).orderBy("scenario", "mode", "phase"))
    print("\n=== WEEK 7: METRICS PER SCENARIO, MODE AND PHASE ===")
    phase.show(60, truncate=False)
    save(phase, out, "week7_phase_metrics")

    # availability: share of time buckets in which >= 95 % of completed requests were good
    df = df.withColumn("bucket", F.floor(F.col("t_end_s") / F.lit(width)).cast("long"))
    per_bucket = df.groupBy("scenario", "mode", "repeat", "bucket").agg(
        F.count("*").alias("completed"), F.sum("good").alias("good"))
    runs = df.groupBy("scenario", "mode", "repeat").agg(F.ceil(F.first("run_s") / F.lit(width)).cast("long").alias("n_buckets"))
    grid = runs.join(spark.range(0, 100000).withColumnRenamed("id", "bucket"),
                     F.col("bucket") < F.col("n_buckets"))
    full = (grid.join(per_bucket, ["scenario", "mode", "repeat", "bucket"], "left")
                .fillna(0, ["completed", "good"])
                .withColumn("up", ((F.col("completed") > 0) & (F.col("good") / F.col("completed") >= 0.95)).cast("int")))
    per_run = full.groupBy("scenario", "mode", "repeat").agg(
        F.count("*").alias("buckets"), F.sum("up").alias("up_buckets"),
        F.round(100 * F.avg("up"), 2).alias("availability_pct"))
    avail = (per_run.groupBy("scenario", "mode").agg(
        F.count("*").alias("runs"), F.round(F.avg("availability_pct"), 2).alias("spark_availability_pct"),
        F.round(F.min("availability_pct"), 2).alias("min_pct"), F.round(F.max("availability_pct"), 2).alias("max_pct"))
        .orderBy("scenario", "mode"))
    print("\n=== WEEK 7: AVAILABILITY RECOMPUTED IN SPARK (Availability = T_up / T_total) ===")
    avail.show(40, truncate=False)
    save(avail, out, "week7_availability")

    summary_path = os.path.join(results, "week7_summary.csv")
    if os.path.exists(summary_path):
        py = (spark.read.option("header", True).option("inferSchema", True).csv(summary_path)
              .groupBy("scenario", "mode").agg(F.round(F.avg("availability_pct"), 2).alias("experiment_availability_pct")))
        check = (avail.join(py, ["scenario", "mode"])
                 .withColumn("difference_pp", F.round(F.col("spark_availability_pct") - F.col("experiment_availability_pct"), 2))
                 .orderBy("scenario", "mode"))
        print("\n=== WEEK 7: CROSS-CHECK (Spark vs experiment script; both should agree) ===")
        check.show(40, truncate=False)
        save(check, out, "week7_availability_check")

    failover = (df.filter(F.col("phase") == "fault").groupBy("scenario", "mode").agg(
        F.count("*").alias("requests_in_fault"),
        F.sum("failover").alias("served_after_failover"),
        F.round(100 * F.avg("failed"), 2).alias("failed_pct"),
        F.round(100 * F.avg("wrong"), 2).alias("wrong_pct")).orderBy("scenario", "mode"))
    print("\n=== WEEK 7: FAULT PHASE, FAILOVER EFFECTIVENESS ===")
    failover.show(40, truncate=False)
    save(failover, out, "week7_failover_effectiveness")


def week8(spark, results, out):
    path = os.path.join(results, "week8_requests.csv")
    if not os.path.exists(path):
        print(f"[skip] {path} not found; run experiments/week8_system_models.py first")
        return
    raw = spark.read.option("header", True).option("inferSchema", True).csv(path)
    df = (raw.filter(F.col("t_start_s") >= F.col("warmup_s"))               # drop warm-up
             .filter(F.col("t_end_s") <= F.col("duration_s") + 0.5)
             .withColumn("good", (truthy("ok") & truthy("correct")).cast("int"))
             .withColumn("window_s", F.col("duration_s") - F.col("warmup_s")))

    models = (df.groupBy("model", "load_type", "clients").agg(
        F.count("*").alias("requests"),
        F.round(100 * F.avg("good"), 2).alias("success_pct"),
        F.round(F.sum("good") / F.first("window_s"), 2).alias("throughput_rps"),
        F.round(F.avg(F.when(F.col("good") == 1, F.col("latency_ms"))), 2).alias("mean_latency_ms"),
        F.round(F.expr("percentile_approx(CASE WHEN good = 1 THEN latency_ms END, 0.50)"), 2).alias("p50_latency_ms"),
        F.round(F.expr("percentile_approx(CASE WHEN good = 1 THEN latency_ms END, 0.95)"), 2).alias("p95_latency_ms"),
        F.round(F.expr("percentile_approx(CASE WHEN good = 1 THEN latency_ms END, 0.99)"), 2).alias("p99_latency_ms"),
    ).orderBy("load_type", "model", "clients"))
    print("\n=== WEEK 8: LATENCY AND THROUGHPUT PER MODEL, LOAD TYPE AND CLIENT COUNT ===")
    models.show(80, truncate=False)
    save(models, out, "week8_model_metrics")

    w = Window.partitionBy("load_type", "model").orderBy("clients")
    base = models.filter(F.col("load_type") == "saturation")
    scal = (base.withColumn("first_tp", F.first("throughput_rps").over(w))
                .withColumn("speedup", F.round(F.col("throughput_rps") / F.col("first_tp"), 3))
                .select("model", "clients", "throughput_rps", "speedup").orderBy("model", "clients"))
    print("\n=== WEEK 8: SCALABILITY (throughput speed-up versus one client, saturation runs) ===")
    scal.show(60, truncate=False)
    save(scal, out, "week8_scalability")

    by_service = (df.filter(F.col("good") == 1).groupBy("model", "load_type", "service").agg(
        F.count("*").alias("requests"),
        F.round(F.avg("latency_ms"), 2).alias("mean_latency_ms"),
        F.round(F.expr("percentile_approx(latency_ms, 0.95)"), 2).alias("p95_latency_ms")).orderBy("load_type", "model", "service"))
    print("\n=== WEEK 8: LATENCY BY SERVICE CLASS ===")
    by_service.show(80, truncate=False)
    save(by_service, out, "week8_service_latency")

    nodes = (df.filter((F.col("good") == 1) & F.col("served_by").startswith("NODE")).groupBy("model", "load_type", "clients", "served_by")
               .agg(F.count("*").alias("requests")))
    balance = (nodes.groupBy("model", "load_type", "clients").agg(
        F.count("*").alias("nodes_used"), F.sum("requests").alias("requests"),
        F.round(F.stddev_pop("requests") / F.avg("requests"), 3).alias("load_imbalance_cv")).orderBy("load_type", "model", "clients"))
    print("\n=== WEEK 8: LOAD BALANCE ACROSS RATING NODES (coefficient of variation; 0 = perfectly even) ===")
    balance.show(80, truncate=False)
    save(balance, out, "week8_node_balance")


def main():
    if len(sys.argv) < 3:
        raise SystemExit("Usage: fault_model_analytics.py RESULTS_DIR OUTPUT_DIR [BUCKET_SECONDS]")
    results, out = sys.argv[1], sys.argv[2]
    width = float(sys.argv[3]) if len(sys.argv) > 3 else 0.5
    spark = SparkSession.builder.appName("TelecomBillingWeek7Week8Analytics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    week7(spark, results, out, width)
    week8(spark, results, out)
    spark.stop()


if __name__ == "__main__":
    main()
