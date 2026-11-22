# Reference run — for checking your output only

**Do not submit these numbers as your own.** Re-run the experiments on your machine and report that output (the brief requires a documented, reproducible environment).

## How it was produced
| | |
|---|---|
| Where | the Linux sandbox of the AI assistant that built this package: 1 vCPU, Python 3.12, local `python launcher.py` (supervised), no Docker |
| Week 9 | `python experiments/week9_transparency.py --phase 4` (12 partitions of 100 accounts, 8 clients, 4 + 4 for the paired tests, seed 42) |
| Week 10 | `python experiments/week10_naming_processes.py` with the defaults (2,000 Zipf lookups over 100 partitions, DNS TTL 2 s in the staleness test, 3 repeats of the scheduling test, 4 s name-server outage) |
| Spark | `analytics/transparency_naming_analytics.py` over the raw rows, Spark 3.5.1 in local mode; the cross-checks against the experiment summaries agree (differences of 0.0) |

## Limits of this data
- One run of each experiment: single runs vary, especially the time-based ones (crash, migration freeze). Repeat them and quote ranges.
- One machine with one vCPU: absolute latencies depend on the machine, and the caching-resolver throughput figures are bounded by one Python process. The shapes (linear broadcast cost, saturating central server, 0 errors versus permanent failure) should not.
- The broadcast scalability test uses a farm of synthetic endpoints on the same machine.
- The container workflow was not executed here.

## Files
`week9_*` and `week10_*` as described in `ASSIGNMENT3_WEEK9_10_STEP_BY_STEP.md` section 4, and `spark_analytics/` (one folder per table).
