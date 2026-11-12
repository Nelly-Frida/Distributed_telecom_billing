# Power BI — Assignment 3 (Weeks 5–6)

Import the generated CSV files after running both experiments.

## Week 5 transaction page
Source: `results/week5_transactions.csv`
- Cards: total transactions, COMMIT count, ABORT count, IN_DOUBT count.
- Clustered column chart: transaction count by status.
- Line chart: total transaction latency by transaction ID or timestamp.
- Clustered column chart: PREPARE time versus COMMIT time.
- Table: transaction ID, customer ID, rating node, status, failure.

Import `results/week5_load_test.csv` for the concurrent workload page:
- Cards: submitted, committed, aborted, errors/in-doubt.
- Bar chart: latency by transaction ID.
- Cards: mean latency and p95 latency from `week5_load_test_summary.json`.

Use `results/week5_experiment_summary.json` to verify participant consistency and recovery. A committed transaction should have the same charge in the rating participant and ledger.

## Week 6 concurrency page
Source: `results/week6_concurrency.csv`
- Clustered column chart: result counts by experiment.
- Bar chart: mean duration by experiment and result.
- Cards: deadlock victim, transactions completed, timeouts/aborts.
- Add balance-conservation and mutex correctness indicators from `week6_experiment_summary.json`.

## Spark outputs
If Spark analytics has been run, import the CSV part files from `results/spark_analytics/week5_metrics` and `results/spark_analytics/week6_metrics`. These are aggregate datasets suitable for dashboard visuals.
