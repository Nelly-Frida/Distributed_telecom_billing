"""Spark batch analytics for Assignment 3 transaction and concurrency CSVs."""
import os
import sys
from pyspark.sql import SparkSession, functions as F


def main():
    if len(sys.argv) != 4:
        raise SystemExit("Usage: transaction_analytics.py WEEK5_CSV WEEK6_CSV OUTPUT_DIR")
    week5_path, week6_path, output_dir = sys.argv[1:]
    spark = SparkSession.builder.appName("TelecomBillingAssignment3Analytics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    tx = spark.read.option("header", True).option("inferSchema", True).csv(week5_path)
    print("\n=== WEEK 5: TRANSACTION STATUS COUNTS ===")
    tx.groupBy("status").count().orderBy("status").show(truncate=False)
    if "total_ms" in tx.columns:
        tx_metrics = tx.where(F.col("total_ms").isNotNull()).groupBy("status").agg(
            F.count("*").alias("transactions"),
            F.round(F.avg("total_ms"), 3).alias("mean_total_ms"),
            F.round(F.expr("percentile_approx(total_ms, 0.95)"), 3).alias("p95_total_ms"),
        )
        tx_metrics.show(truncate=False)
        tx_metrics.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/week5_metrics")

    load_path=os.path.join(os.path.dirname(week5_path),"week5_load_test.csv")
    if os.path.exists(load_path):
        load=spark.read.option("header",True).option("inferSchema",True).csv(load_path)
        print("\n=== WEEK 5: CONCURRENT 2PC LOAD TEST ===")
        load.groupBy("status").count().orderBy("status").show(truncate=False)
        load_metrics=load.agg(
            F.count("*").alias("submitted"),
            F.round(F.avg("latency_ms"),3).alias("mean_latency_ms"),
            F.round(F.expr("percentile_approx(latency_ms, 0.95)"),3).alias("p95_latency_ms"),
        )
        load_metrics.show(truncate=False)
        load_metrics.coalesce(1).write.mode("overwrite").option("header",True).csv(f"{output_dir}/week5_load_metrics")

    conc = spark.read.option("header", True).option("inferSchema", True).csv(week6_path)
    if "experiment" not in conc.columns:
        conc=conc.withColumn("experiment",F.lit("legacy_week6_run"))
    print("\n=== WEEK 6: CONCURRENCY OUTCOMES ===")
    conc.groupBy("experiment", "result").count().orderBy("experiment", "result").show(truncate=False)
    conc_metrics = conc.groupBy("experiment", "result").agg(
        F.count("*").alias("operations"),
        F.round(F.avg("duration_ms"), 3).alias("mean_duration_ms"),
        F.round(F.max("duration_ms"), 3).alias("max_duration_ms"),
    )
    conc_metrics.show(truncate=False)
    conc_metrics.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir}/week6_metrics")
    spark.stop()


if __name__ == "__main__":
    main()
