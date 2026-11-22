# Report template — Milestones 7 and 8 (Assignment 3, Practical 1)

Fill every `<...>` from **your own runs** (copy `results/` to a dated folder first). Delete this line and the instructions in italics.
Follow the brief's method for each experiment: Hypothesis -> Experimental design -> Implementation -> Measurement -> Analysis -> Conclusion.

---
## 0. Reproducibility record (brief, section 9)
| Item | Value |
|---|---|
| Hardware (CPU, cores, RAM) | <...> |
| Operating system | <...> |
| Deployment | local `launcher.py` / Docker Compose (Docker version <...>) |
| Network topology | all nodes on one host; ports 5002-5008, 8000, 80 (list from `WEEK7_8_DESIGN.md` section 1) |
| Language and versions | Python <...>; Spark 3.5.1; Java <...> |
| Configuration | `FT_PROBE_INTERVAL=0.5`, `FT_MISS_THRESHOLD=3`, `FT_ATTEMPT_TIMEOUT=1.5`, `REQUEST_TIMEOUT=<...>`, `NODE_SERVICE_SLOTS=1`, supervisor delay 1.0 s |
| Workload and seeds | Week 7: 8 closed-loop clients, think time 0.02 s, seed 42, phases 5/8/8 s, repeats <...>. Week 8: service demand 20/5/40 ms, mix 50/30/20, seed 42, clients 1,2,4,8,16, fixed load 80 req/s |
| Commands | `python experiments/week7_fault_tolerance.py --repeats <...>`; `python experiments/week8_system_models.py` |
| Data | `results/week7_*`, `results/week8_*`, `results/spark_analytics/*` (attach or link) |

---
## Milestone 7 — Fault tolerance and recovery

### 7.1 Hypotheses
- H7-1 With detection, immediate failover and a replicated ledger, a single node failure is masked from clients; the baseline loses every request routed to the failed component while it is down.
- H7-2 Failover costs one extra attempt for the first request that hits the dead node and nothing afterwards.
- H7-3 A Byzantine node is invisible to crash-oriented failover; only majority voting removes wrong results, at about three times the work.
- H7-4 After a replica or the coordinator restarts, automatic recovery leaves a consistent state with no pending transaction.
- *Add your own hypothesis about a trade-off (for example, detection time against false positives).*

### 7.2 Experimental design
| | |
|---|---|
| Independent variables | system mode (baseline, resilient, resilient_verify); fault type (crash, omission, communication, service error, Byzantine, ledger crash, coordinator crash, rating crash during 2PC) |
| Dependent variables | availability, MTTR, detection time, request success, wrong results, latency during the fault, consistency after recovery, pending transactions after automatic recovery |
| Controlled variables | client count and think time, request mix and seed, fault timing (injection at 5 s for 8 s), node set, timeouts, starting state (replicas in sync) |

### 7.3 Implementation (3 to 5 sentences each; point to the file)
Fault model and injection (`common/fault_state.py`); failure detector (`fault_tolerance/failure_detector.py`, timing formula `K*I + T`); failover and voting (`fault_tolerance/resilient_dispatcher.py`);
replicated ledger, resync and anti-entropy, 2PC failover (`transactions/two_phase_commit.py`, `nodes/node2_billing.py`); supervisor / Docker restart policy; measurement (`experiments/ft_metrics.py`).
*Include the architecture figure and the state diagram UP -> SUSPECT -> DOWN.*

### 7.4 Measurement and results
Table A — availability and MTTR (mean over `<n>` runs, range in brackets), from `week7_summary_aggregate.csv`.
| Scenario | Mode | Availability % | MTTR s (masked runs) | Detection s | Success % | Wrong % |
|---|---|---|---|---|---|---|
| rating_crash | baseline | <...> | <...> | <...> | <...> | <...> |
| rating_crash | resilient | <...> | <...> | <...> | <...> | <...> |
| *(one row per scenario and mode, 16 rows)* | | | | | | |

Table B — recovery evidence (`week7_experiment_summary.json`): consistency report after each stateful scenario, replicas' committed counts, `pending_after_auto_recovery`, `auto_recovery_s`, replay of failed transactions (exactly-once).
Figure — availability over time for `rating_crash` baseline versus resilient (Power BI line chart from `week7_timeline.csv`).
Cross-check — Spark recomputed availability; `difference_pp` = <...> for all rows.

### 7.5 Analysis
Per hypothesis: supported / not supported, with the numbers. Discuss: why masked and not masked cases differ; the cost of resilience (3x work for voting, +50 % messages per transaction with a second ledger replica,
12 probes/s); why request-level success overstates availability under omission; why the coordinator remains a single point of failure; the effect of the repeat-to-repeat variation.
*Mathematical model:* `Availability = T_up/T_total`; `A_replicated = 1 - (1-a)^R`; detection time `K*I + T`; compare your measured detection time with the formula.

### 7.6 Conclusion and failure log
Three sentences. Then copy the table from `logs/week7_engineering_log.md` and add your own entries (the brief requires: what changed, what failed, why, how fixed, alternative, what was learned).

---
## Milestone 8 — Distributed system models

### 8.1 Hypotheses
- H8-1 Host-based throughput is capped by one node and server-based by its busiest server; pooled models scale until the pool saturates.
- H8-2 Least-loaded allocation improves tail latency over round robin when request sizes differ, at no extra cost.
- H8-3 The workstation model scales with clients but wastes idle capacity and costs most per request at moderate load.
- H8-4 Under one component failure only models with failover keep serving fully.

### 8.2 Experimental design
Independent: model (6 variants), clients (1,2,4,8,16). Dependent: throughput, mean/p95/p99 latency, queue wait, utilisation, scalability, reliability, cost per 1,000 requests.
Controlled: request mix and sequence, service demand, node capacity (1 slot), run length and warm-up, unit prices, weights. Three load types: saturation, fixed 80 req/s, paced fault test.

### 8.3 Implementation
Model definitions (`WEEK7_8_DESIGN.md` section 4); emulated capacity (`common/emulated_work.py`); least-loaded policy; cost model; decision matrix and gate (`experiments/week8_system_models.py`).

### 8.4 Measurement and results
Table C — capacity versus bound at 8 clients: `<measured>` req/s against `1/E[S]`, `1/max(p_i*S_i)`, `4/E[S]`, `N/E[S]` (see design document).
Table D — fixed load (80 req/s): served req/s, p95 ms, utilisation %, cost per 1,000 requests for each model.
Table E — reliability: share served during the fault, availability, per model.
Table F — decision matrix with the two weight profiles and the gate.
Figures — throughput and p95 latency versus clients (Power BI), latency by service class (Spark `week8_service_latency`), node balance.

### 8.5 Analysis and architectural justification
State which model fits the telecom billing application and why, using the measured criteria **and** the requirement the numbers cannot see (one authoritative ledger, transactions of Week 5, auditability).
Discuss: why the workstation model wins raw metrics yet fails the gate; the effect of weights (show that the recommendation under both profiles is `<...>`); the cost assumptions (replace the illustrative prices with yours and re-run);
the limits of emulated capacity and a single machine.

### 8.6 Conclusion and failure log
Three sentences. Copy `logs/week8_engineering_log.md` and add your own entries.

---
## System evolution (Part 3 of the presentation)
One paragraph and one figure: Week 4 election and Lamport clocks -> Week 5 2PC and durable journal -> Week 6 locks and deadlock handling -> Week 7 detection, replication, failover, recovery -> Week 8 model choice (processor pool with least-loaded allocation).
Say what is reused (journal for recovery, locks for thread safety, containers and Spark for deployment and analysis) and what the next milestones need (transparency, naming).

## Baseline versus proposed (brief, section 6)
| Baseline | Proposed improvement | Evidence |
|---|---|---|
| Round-robin dispatcher without failure handling, single ledger | Failure detector + failover + replicated ledger + automatic recovery | Table A and B |
| Round-robin processor pool | Least-loaded pool | Table C and D |
