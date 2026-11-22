# Reference run — for checking your output only

**Do not submit these numbers as your own.** Re-run the experiments on your machine and report that output (the brief requires a reproducible, documented environment).

## How it was produced
| | |
|---|---|
| Where | the Linux sandbox of the AI assistant that built this package: 1 vCPU, Python 3.12, local `python launcher.py` (supervised), no Docker |
| Config | `REQUEST_TIMEOUT=3`, defaults for detector (0.5 s probes, 3 misses), `NODE_SERVICE_SLOTS=1`, supervisor delay 1.0 s |
| Week 7 | `--quick` phases (3 s before, 4 s fault, 3-4 s after), **one run** per scenario and mode, 8 clients, seed 42; two batches run back to back and merged (`week7_experiment_summary.json` says so) |
| Week 8 | `--clients 1,2,4,8,16 --duration 4.5 --warmup 1.2` (shorter than the default 7 s), 8 clients for the fixed-load and fault tests |
| Spark | `analytics/fault_model_analytics.py` over the raw rows, Spark 3.5.1 in local mode; availability cross-check difference 0.0 pp for all 16 scenario/mode pairs |

## Limits of this data
- One run per Week 7 scenario: single runs vary (for example `tx_rating_crash` resilient gave 100 % availability in two single runs and 95.45 % in all three runs of a separate `--repeats 3` batch). Use `--repeats 3` or more.
- One machine with one vCPU: absolute latencies depend on the machine; the Week 8 capacities are emulated and follow the bounds in `WEEK7_8_DESIGN.md`.
- The container workflow was not executed here.

## Files
`week7_*` and `week8_*` as described in `ASSIGNMENT3_WEEK7_8_STEP_BY_STEP.md` section 4, and `spark_analytics/` (one folder per table).
