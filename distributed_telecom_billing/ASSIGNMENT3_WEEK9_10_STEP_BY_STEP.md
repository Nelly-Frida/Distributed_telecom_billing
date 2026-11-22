# Assignment 3, Practical 2 — Weeks 9 and 10 (step by step)

Week 9: transparency (access, location, replication, migration, failure) and its overhead.
Week 10: naming (static map, broadcast, name server, DNS-style hierarchy, prefix table) and distributed process management.
Builds on the Docker + Spark system of Weeks 5 to 8. Read `WEEK9_10_DESIGN.md` first.

## 0. What is new

| Area | New files |
|---|---|
| Naming | `naming/naming_store.py`, `name_server.py` (4 deployments), `resolvers.py` (5 strategies + admin), `endpoint_farm.py` |
| Account service | `accounts/account_store.py` (replicated partitions), `nodes/node_extensions.py` (messages added to the rating nodes) |
| Transparency | `transparency/gateway.py` (in NODE-2), `transparency/placement.py`, `common/transparent_client.py` (transparent + direct client) |
| Process management | `process_mgmt/node_runtime.py` (in the nodes), `process_mgmt/process_manager.py` (in NODE-2) |
| Experiments | `experiments/week9_transparency.py`, `experiments/week10_naming_processes.py` |
| Analytics and tests | `analytics/transparency_naming_analytics.py` (Spark), `tests/test_week9_10.py` (58 tests in total) |
| Deployment | four name-server services, per-node account storage, experiment and Spark profiles in `docker-compose.yml`; the launcher starts the name servers |

## 1. Option A: local (PowerShell)

```powershell
pip install -r requirements.txt
python -m unittest discover -s tests            # 58 tests must pass
python launcher.py                              # terminal 1 (supervised)
```
Terminal 2:
```powershell
python experiments/week9_transparency.py              # about 4 minutes; add --quick to try it
python experiments/week10_naming_processes.py         # about 4 minutes; add --quick to try it
```
Only some parts: `--experiments migration,failure` (Week 9: access, location, replication, migration, failure, overhead) and
`--parts latency,staleness` (Week 10: latency, staleness, scalability, outage, lifecycle, scheduling, coordination, recovery, migration).
The crash experiments need the supervisor (default) so the crashed node comes back. Do not use the machine for anything heavy while they run.

## 2. Option B: Docker Compose

```powershell
docker compose up --build -d                  # nodes, ledgers, NODE-2, API and the four name servers
docker compose ps                             # expect 12 running (ledger x2, ns x4, node2-6, billing-api)
python experiments/week9_transparency.py     # from your venv, through the published ports
python experiments/week10_naming_processes.py
docker compose --profile analytics910 run --rm spark-analytics-week9-10
Get-ChildItem .\results\spark_analytics -Recurse
```
Or run the experiments inside Docker: `docker compose --profile week9 run --rm week9-transparency` and `... --profile week10 run --rm week10-naming-processes`.
Docker restarts a crashed container itself, which is the supervisor here; the measured recovery therefore includes Docker's restart.
Docker was not available where this package was built, so the container workflow is untested: if a step fails, send the error.

## 3. Spark without Docker
```powershell
pip install pyspark==3.5.1          # needs Java 11, 17 or 21
python analytics/transparency_naming_analytics.py results results/spark_analytics
```
The cross-check tables must show `difference_ms` of 0.0 (or rounding only).

## 4. Output files (all in `results/`)

| File | Content |
|---|---|
| `week9_summary.csv` | one row per metric: experiment, variant, metric, value, unit (long format) |
| `week9_requests.csv` | every client operation (Spark input; Power BI timelines) |
| `week9_experiment_summary.json` | environment, parameters, reconfiguration and verification details |
| `week10_summary.csv`, `week10_lookups.csv` | the same for Week 10; one row per lookup |
| `week10_process_events.csv` | process life-cycle events (created, rescheduled, migrated, terminated) |
| `spark_analytics/*` | Spark tables |

`results/reference_run_week9_10/` holds a complete run produced while building this package in a Linux sandbox with 1 vCPU.
**Use it only to check your output. Report your own numbers.**

## 5. Reading the results

- Week 9: for migration and failure compare `transparent` and `direct` before, during and after the event (error %, latency), then `lost_writes` and `replicas_converged` (must be 0 and True).
  Overhead: direct versus gateway R = 1, 2, 3; write latency grows with R, read latency barely does.
- Week 10 naming: latency follows messages per lookup; check staleness (time to correct, wrong lookups) and the outage table; scalability: broadcast against N, central against threads, prefix table load against the number of names.
- Week 10 processes: `result_exact` must be True everywhere (crash, migration, group); scheduling makespan by policy; speed-up limited by 4 nodes; nothing left behind after terminate.
- A single run varies. Repeat the experiments and quote ranges (the scripts take `--seed`).

## 6. Presentation (Part 1 concepts, Part 2 implementation, Part 3 evolution)

**Part 1 (about 4 slides):** the five transparencies and what hides what; replication and consistency (idempotent operations, union repair, write-all-available and what it does not guarantee); migration protocol (pre-copy, freeze, switch, clean-up); naming strategies and the message-cost table; process life cycle and checkpoints.
**Part 2:** run `python experiments/week9_transparency.py --experiments migration,failure --quick` and show transparent versus direct error rates; run `week10_naming_processes.py --parts latency,staleness,recovery --quick`; show the Power BI pages and the Spark cross-check.
**Part 3:** Week 7 failure detector (used to schedule and to restart tasks), Week 8 capacity model (tasks compete with requests), Week 5 idempotent operations (same idea as operation ids), Week 4/6 coordination and locking ideas (freeze gate), Docker and Spark (deployment, analysis).

## 7. Engineering logs
`logs/week9_engineering_log.md` and `logs/week10_engineering_log.md` hold what was observed while this version was built. Keep what is true for you and **add what you observe on your machine**; the brief requires the six answers each week.

## 8. Troubleshooting
| Symptom | Cause and fix |
|---|---|
| `system not healthy` | the stack is not running or a node is restarting; start it and wait 10 s |
| `PORT ... in use` / old processes | stop all Python processes and start again; ports 5021-5024 are the name servers |
| Week 9 HTTP access test skipped | the billing API (port 8000) is not running; the other tests still run |
| `NXDOMAIN` or `NOT_OWNER` errors after an interrupted run | the experiment resets placement at the start of every part; run it again |
| Static-map resolver fails | `data/static_map.json` is written by the experiments at the start of each part; do not delete `data/` while running |
| Crash tests never recover | `launcher.py --no-supervise`, or a service without a restart policy |
