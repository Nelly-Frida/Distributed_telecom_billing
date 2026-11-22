# Report template — Milestones 9 and 10 (Assignment 3, Practical 2)

Fill every `<...>` from **your own runs** (copy `results/` to a dated folder first). Follow the brief's method for each experiment:
Hypothesis -> Experimental design -> Implementation -> Measurement -> Analysis -> Conclusion.

## 0. Reproducibility record (brief, section 9)
| Item | Value |
|---|---|
| Hardware, OS | <CPU, cores, RAM, OS> |
| Deployment | local `launcher.py` / Docker Compose (Docker version <...>) |
| Topology | NODE-2 (gateway, process manager); NODE-3..6 (rating, accounts, process runtime); NS-CENTRAL, NS-ROOT, NS-ACCT, NS-SVC; ports 5002-5008, 5021-5024, 8000 |
| Language, versions | Python <...>, Spark 3.5.1, Java <...> |
| Configuration | `NAMING_MODE=prefix`, `NAMING_TTL_S=5`, `ACCOUNT_PREFIX_DIGITS=3`, supervisor delay 1 s, detector as in Week 7 |
| Workload and seeds | Week 9: 12 partitions of 100 accounts, 8 clients (4 + 4 for the paired tests), 50/50 read/write, seed 42, phases <...> s. Week 10: 100 partitions, Zipf s = 1.1, <...> lookups, 24 skewed tasks, seed 42 |
| Commands | `python experiments/week9_transparency.py`, `python experiments/week10_naming_processes.py` |
| Data | `results/week9_*`, `results/week10_*`, `results/spark_analytics/*` |

---
## Milestone 9 — Transparency and reliability

### 9.1 Hypotheses
H9-1 access (same result over TCP and HTTP) · H9-2 location (data sits where the name says, whatever the naming strategy) · H9-3 replication (R can change under load with no error and no lost write; write latency grows with R) ·
H9-4 migration (no client error, millisecond freeze, no lost write; the baseline fails permanently) · H9-5 failure (masked and repaired; the baseline fails for the whole outage) · H9-6 overhead (one hop plus one write per replica).
*Add your own hypothesis, for example about the freeze time against the partition size.*

### 9.2 Experimental design
Independent: client kind (transparent / direct), replication factor, naming strategy, event (reconfigure, migrate, crash). Dependent: error rate, latency (mean, p95, max), freeze time, lost writes, convergence, throughput.
Controlled: workload mix and seed, partitions, node set, client count, run length.

### 9.3 Implementation
Gateway (`transparency/gateway.py`): logical invocation, replicated writes, failover, NOT_OWNER handling, repair, reconfiguration with a freeze gate. Account store (`accounts/account_store.py`): operation sets, idempotence.
Placement (`transparency/placement.py`), clients (`common/transparent_client.py`). *Include the architecture figure, the migration sequence diagram (pre-copy, freeze, switch, clean-up) and the consistency model (section 2.1 of the design).*

### 9.4 Results
Table A — migration and crash: requests, errors and error % before / during / after, for transparent and direct (from `week9_summary.csv`, Spark `week9_error_timeline`).
Table B — replication: write and read latency per phase R = 1, 2, 3, 2; freeze time and operations copied per reconfiguration; lost writes and convergence.
Table C — overhead: direct versus gateway R = 1, 2, 3 for reads and writes (mean, p95, throughput); cold and warm resolution.
Table D — location and access: partitions where data differs from the name record; identical results over TCP and HTTP; resolve time per naming strategy at the gateway.
Figures — error rate per second around the migration and the crash (Power BI line chart), write latency against R.

### 9.5 Analysis
Per hypothesis: supported or not, with the numbers. Discuss: why the direct client fails permanently after a move (a stale location nobody corrects); why the gateway needs `NOT_OWNER`; the consistency model and what it does not guarantee (not linearizable; a write on one disk only);
the freeze time against the operations copied; the overhead against the service time of a real rating step (Week 8: 5 to 40 ms).

### 9.6 Conclusion and failure log
Three sentences, then the table from `logs/week9_engineering_log.md` plus your own entries.

---
## Milestone 10 — Naming and distributed process management

### 10.1 Hypotheses
H10-1 latency (cached strategies need no message; central 1; DNS cold 2; broadcast N) · H10-2 scalability (broadcast linear in N; a central server saturates; a prefix table pays in transfer size) ·
H10-3 staleness (static forever, DNS one TTL, prefix until NOT_OWNER, central and broadcast never) · H10-4 availability (caches survive a name-server outage) · H10-5 scheduling (least loaded wins on skewed work) ·
H10-6 coordination (small constant overhead, speed-up bounded by the number of nodes) · H10-7 recovery (exactly-once after a crash and after a migration; nothing left behind after terminate).

### 10.2 Experimental design
Independent: resolution strategy, number of nodes / threads / names, scheduling policy, number of tasks, event (crash, migration). Dependent: latency, messages per lookup, correctness, time to correct, throughput, makespan, speed-up, exactness of results, leaked names and tasks.

### 10.3 Implementation
Name store and servers, five resolvers, admin harness; process runtime and manager (life cycle, policies, monitor, checkpoint). *Include the naming architecture figure (root -> zone referral), the strategy table of the design document and the process state diagram CREATED -> RUNNING -> DONE / TERMINATED / FAILED, with RESCHEDULING and MIGRATING.*

### 10.4 Results
Table E — resolution: mean / p95 latency, messages per lookup, hit ratio, correctness for the seven variants. Table F — staleness after a move. Table G — scalability: broadcast against N, throughput against threads, prefix table load against names. Table H — outage: share resolved per strategy.
Table I — scheduling (makespan, imbalance by policy). Table J — coordination (create time, makespan, barrier overhead, speed-up for 1 to 16 tasks). Table K — recovery and migration (time to reschedule, downtime, exactness). Table L — life cycle (names and tasks left behind).
Figures — latency against messages; broadcast cost against N; makespan by policy; speed-up against tasks.

### 10.5 Analysis and architectural justification
Which naming strategy fits the billing platform and why (combine a cache with NOT_OWNER redirects and a central or hierarchical source of truth); what the process manager guarantees and what it needs (deterministic, idempotent tasks); limitations (section 5 of the design).

### 10.6 Conclusion and failure log
Three sentences, then `logs/week10_engineering_log.md` plus your own entries.

---
## System evolution (Part 3) and baseline versus proposed
| Baseline | Proposed improvement | Evidence |
|---|---|---|
| Application talks to physical nodes | Logical names through a transparency gateway | Tables A, B, C |
| Hard-coded placement (static map) | Naming service with a cache and redirect on `NOT_OWNER` | Tables E, F, H |
| Round-robin task placement | Least-loaded scheduling with checkpointed recovery | Tables I, K |
