# Assignment 3, Practical 1 — Weeks 7 and 8 (step by step)

Week 7: fault injection, failure detection, replication, failover, recovery (availability and MTTR).
Week 8: compare host-based, server-based, processor-pool, workstation and integrated models.
Both build on the Docker + Spark system of Weeks 5 and 6. Read `WEEK7_8_DESIGN.md` first (10 minutes).

## 0. What is new in this version

| Area | New files |
|---|---|
| Fault injection | `common/fault_state.py` (every node), control messages `FAULT_INJECT / FAULT_CLEAR / FAULT_STATUS` |
| Detection and failover | `fault_tolerance/failure_detector.py`, `fault_tolerance/resilient_dispatcher.py` |
| Replication and recovery | `transactions/two_phase_commit.py` (ledger replica group, resync, failover), `nodes/node2_billing.py` (anti-entropy, re-drive), `LEDGER-2` |
| Crash restart | `launcher.py` supervisor (local) or `restart: unless-stopped` (Docker) |
| Week 8 emulation | `common/emulated_work.py` (node capacity), `NODE_STATS` on the rating nodes |
| Experiments | `experiments/week7_fault_tolerance.py`, `week7_live_demo.py`, `week8_system_models.py`, `ft_metrics.py` |
| Big-data analytics | `analytics/fault_model_analytics.py` (Spark) |
| Tests | `tests/test_week7_8.py` (29 tests in total with the Week 5-6 ones) |

## 1. Option A: run locally on Windows (PowerShell)

```powershell
pip install -r requirements.txt
python -m unittest discover -s tests            # 29 tests must pass
$env:REQUEST_TIMEOUT = "3"                       # optional: shorter baseline waits during omission faults
python launcher.py                               # terminal 1, supervised: crashed nodes restart after 1 s
```

Terminal 2 (the system must stay running):

```powershell
python experiments/week7_live_demo.py --mode baseline     # defence demo, contrast the two systems
python experiments/week7_live_demo.py --mode resilient
python experiments/week7_fault_tolerance.py --repeats 3   # full Week 7 experiment (about 25 minutes)
python experiments/week8_system_models.py                 # full Week 8 experiment (about 8 minutes)
```

Shorter runs while developing: add `--quick`. Only some scenarios: `--scenarios rating_crash,ledger_crash`.
Do not use the machine for anything heavy while an experiment runs. Close and restart `launcher.py` between big runs.

Week 4 note: `experiments/week4_failure.py` kills NODE-6. With the supervisor ON, NODE-6 restarts within about 1 s and, as the highest id, correctly resumes as leader. To see NODE-5 take over, start `python launcher.py --no-supervise`.
To watch the election messages, start with `$env:ELECTION_DEBUG="1"; python launcher.py`.

Scenarios: `rating_crash`, `rating_omission`, `rating_drop`, `rating_error`, `rating_byzantine`, `tx_rating_crash`, `ledger_crash`, `coordinator_crash`.

## 2. Option B: Docker Compose

```powershell
docker compose up --build -d                                       # nodes, ledger + replica, NODE-2, API
docker compose --profile week7 run --rm week7-fault-tolerance
docker compose --profile week8 run --rm week8-system-models
docker compose --profile analytics78 run --rm spark-analytics-week7-8
```

Docker restarts a crashed container itself (`restart: unless-stopped`), which is the supervisor here. To shorten baseline waits:
`$env:REQUEST_TIMEOUT="3"; docker compose up -d`. Results appear in `./results`.
Pass extra options by overriding the command, for example
`docker compose --profile week7 run --rm week7-fault-tolerance python experiments/week7_fault_tolerance.py --repeats 3 --quick`.

## 3. Option C: Spark without Docker

```powershell
pip install pyspark==3.5.1          # needs Java 11, 17 or 21 on the PATH
python analytics/fault_model_analytics.py results results/spark_analytics
```

The job recomputes availability from the raw per-request rows and prints a cross-check against the experiment script: the
difference column must be 0.0. It also produces latency percentiles, scalability, latency by service class and load balance.

## 4. Output files (all in `results/`)

| File | Content |
|---|---|
| `week7_summary.csv`, `week7_summary_aggregate.csv` | one row per run; mean/min/max per scenario and mode |
| `week7_requests.csv` | every request (large; the Spark input) |
| `week7_timeline.csv` | availability buckets over time (Power BI line chart) |
| `week7_detector_events.csv`, `week7_supervisor_events.csv` | detection, recovery, resync and restart events with timestamps |
| `week7_experiment_summary.json` | full detail, consistency reports, environment, parameters |
| `week8_runs.csv` | capacity sweep (saturation) and fixed-load rows |
| `week8_reliability.csv`, `week8_decision_matrix.csv` | fault test and weighted decision |
| `week8_requests.csv`, `week8_experiment_summary.json` | raw rows, parameters, unit prices, weights |
| `spark_analytics/*` | Spark tables |

`results/reference_run/` holds a complete run produced while building this package, in a Linux sandbox with 1 vCPU. **Use it only to
check that your output looks right. Report the numbers from your own machine.**

## 5. Reading the results

- Week 7: compare `baseline` and `resilient` for each scenario. Look at availability, MTTR (masked = hidden from clients), detection time,
  wrong results (Byzantine), `consistent_after_recovery`, `pending_after_auto_recovery` (must be 0) and `post_run_convergence_s`.
- A single run varies (where the crash lands relative to in-flight requests). Use `--repeats 3` or more for numbers you defend, and quote mean and range.
- Week 8: capacity (saturation), then utilisation, tail latency and cost at the fixed load, then reliability, then the matrix. Check your
  capacity numbers against the bounds in `WEEK7_8_DESIGN.md` section 4; large deviations mean something else is limiting.

## 6. Presentation (Part 1 concepts, Part 2 implementation, Part 3 evolution)

**Part 1 — concepts (about 4 slides):** fault classes and the fault model; failure detection (heartbeat, timeout, false positives, the detection-time formula);
replication and failover (primary-backup, write quorum, anti-entropy); availability and MTTR definitions; the five system models and the queueing view
(`rho = lambda / (c*mu)`).
**Part 2 — implementation and demo:** run `week7_live_demo.py` twice (baseline, resilient) and point at the detector events and the failed-request counts; show
`week7_summary_aggregate.csv`; run `week8_system_models.py --quick --models host_based,pool_least_loaded,workstation --clients 1,4,8`; show the Power BI pages and the Spark cross-check.
**Part 3 — system evolution:** one slide that maps each earlier week to what Week 7/8 reuse: Week 4 election and Lamport clocks (leader), Week 5 2PC and journal (recovery, exactly-once),
Week 6 locks (thread safety of the dispatcher and the coordinator), Weeks 5-6 Docker and Spark (deployment and analytics).

## 7. Failure-driven engineering log

`logs/week7_engineering_log.md` and `logs/week8_engineering_log.md` contain the failures observed while this version was built and tested.
Keep what is true for you, and **add what you observe on your own machine** (new failures, different numbers). The brief requires the six answers for every week.

## 8. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `System did not become healthy` | the stack is not running or a node is still restarting; start `launcher.py` / `docker compose up -d` and wait 10 s |
| Port already in use | an old `launcher.py` is still running; stop all Python processes, then start again |
| Crash scenarios never recover | you started `launcher.py --no-supervise`, or a Docker service lacks `restart: unless-stopped` |
| Baseline omission runs are very slow | each stuck request waits for `REQUEST_TIMEOUT` (8 s in Docker); set it to 3 |
| Spark: `JAVA_HOME` or Java not found | install Java 17, or use the Docker Spark profile |
| `week7_summary.csv` overwritten | each run writes the same file names; copy `results/` to a dated folder first |
