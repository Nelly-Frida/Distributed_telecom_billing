# Power BI — Assignment 3, Weeks 7 and 8

Import with **Get Data -> Text/CSV**. Use Import mode. For `week7_requests.csv` and `week8_requests.csv` (tens of thousands of rows) keep only the columns you use.
In Power Query set the types: `ok`, `correct`, `up`, `masked` -> True/False; `t_start_s`, `t_end_s`, `latency_ms`, `bucket_start_s` -> Decimal number.

## Week 7 page — fault tolerance
| Table | File |
|---|---|
| Summary | `results/week7_summary.csv` (one row per run) and `week7_summary_aggregate.csv` (mean/min/max) |
| Timeline | `results/week7_timeline.csv` |
| Events | `results/week7_detector_events.csv` (`timestamp` is Unix time in seconds; in Power Query add a column `#datetime(1970,1,1,0,0,0) + #duration(0,0,0,[timestamp])`) |
| Spark | `results/spark_analytics/week7_availability`, `week7_phase_metrics`, `week7_failover_effectiveness` (part-*.csv files) |

Slicers: `scenario`, `mode` (baseline / resilient / resilient_verify).

Visuals
1. Clustered bar — `availability_mean_pct` by `scenario`, legend `mode` (aggregate table). Add error bars from `availability_min_pct` / `availability_max_pct`.
2. Clustered bar — `mttr_mean_s` by scenario and mode. Add a card `masked_runs` / `runs`.
3. **Line chart — availability over time (the failure story).** Axis `bucket_start_s`, values `Good ratio`, one line per `mode`, slicer on `scenario`. Add a constant line at 0.95. The baseline line dips at the fault; the resilient line stays up.
4. Clustered column — `fault_phase_success_pct` versus `availability_during_fault_pct` per scenario: shows why request-level success overstates availability.
5. Clustered column — `wrong_result_pct` per mode for `rating_byzantine`.
6. Table — recovery evidence from `week7_experiment_summary.json` (`consistent_after_recovery`, `post_run_convergence_s`, `pending_after_auto_recovery`, `auto_recovery_s`) — these are also columns of `week7_summary.csv`.
7. Table — `week7_detector_events.csv` filtered to one scenario: SUSPECTED, DOWN, RECOVERED, RESYNC, REDRIVE with the node.

Measures (DAX)
```
Good ratio = DIVIDE(SUM(week7_timeline[good]), SUM(week7_timeline[completed]))
Availability % = DIVIDE(CALCULATE(COUNTROWS(week7_timeline), week7_timeline[up] = TRUE()), COUNTROWS(week7_timeline))
Failed % = DIVIDE(SUM(week7_timeline[failed]), SUM(week7_timeline[completed]))
```

## Week 8 page — system models
| Table | File |
|---|---|
| Runs | `results/week8_runs.csv` (filter `load_type`: `saturation` or `fixed_rate`) |
| Reliability | `results/week8_reliability.csv` |
| Decision | `results/week8_decision_matrix.csv` |
| Spark | `results/spark_analytics/week8_model_metrics`, `week8_scalability`, `week8_service_latency`, `week8_node_balance` |

Visuals
1. Line chart — `throughput_rps` by `clients`, legend `model`, filter `load_type = saturation` (capacity and scalability).
2. Line chart — `p95_latency_ms` by `clients`, legend `model`, same filter.
3. Clustered bar — `utilization_pct`, `p95_latency_ms`, `cost_per_1000_requests_usd` by `model`, filter `load_type = fixed_rate`. Use one small chart per metric.
4. Clustered bar — `fault_success_pct` and `availability_pct` by `model` (reliability table).
5. Table — decision matrix with `raw_*` columns, `score_balanced`, `score_telecom_availability`, `rank_*_after_gate`, `gate_central_state` and `gate_reason`; use conditional formatting on the scores and mark `gate_central_state = fail` in red.
6. Bar — `week8_service_latency`: mean latency by `service` and `model` (shows who suffers for heavy data requests).
7. Cards — recommended model (the row with `rank_balanced_after_gate = 1`) and the capacity-versus-bound table typed from `WEEK7_8_DESIGN.md`.

Say on the slide that the weights and the unit prices are assumptions (`week8_experiment_summary.json` stores both).
