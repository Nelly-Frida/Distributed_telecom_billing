# Weeks 7 and 8 — Design: fault tolerance and distributed-system models

Evolution so far: S1 prototype -> S3 Apache multi-tier -> S4 leader election and Lamport clocks ->
S5 durable two-phase commit -> S6 concurrency and deadlock handling -> **S7 fault tolerance and recovery**
-> **S8 distributed-system model comparison**. Nothing from earlier weeks was replaced. Every new
behaviour can be switched off per request (`ft_mode: "baseline"`), so the baseline stays reproducible.

## 1. Architecture (Week 7 additions marked with *)

```
 clients --> [Apache :80] --> billing_api :8000 --> NODE-2 (billing front-end + 2PC coordinator)
                                                      |   * failure detector (PROBE every 0.5 s)
                                                      |   * resilient dispatcher (failover, least-loaded, vote)
                                                      |   * anti-entropy + automatic 2PC re-drive
                    +---------------+--------+--------+--------+
                    v               v        v        v        v
                 NODE-3          NODE-4   NODE-5   NODE-6   LEDGER-1  <-- * LEDGER-2 (replica)
                 (rating nodes, bully election, Lamport clocks)         (durable SQLite state)

 * every process owns a FaultState; faults are injected with FAULT_INJECT control messages
 * supervisor (launcher.py locally, `restart: unless-stopped` in Docker) restarts crashed processes
```

## 2. Fault model (Week 7)

| Failure class (brief, 4.7) | Injected as | What it models | Where it is handled |
|---|---|---|---|
| Crash / node failure | `crash`: `os._exit(137)` | power loss, kill -9, OOM kill | detector + failover; supervisor restart |
| Communication failure | `drop`: connection closed, no reply | broken link, reset | failover on the failed attempt |
| Omission failure | `omission`: accept and never answer | hung process, lost reply | per-attempt timeout (1.5 s) then failover |
| Service failure | `error`: error reply | application bug, dependency down | failover on the error reply |
| Performance failure | `delay` | slow node | not masked (documented) |
| Byzantine behaviour | `byzantine`: plausible wrong charge | corrupted state, malicious node | only majority vote (`resilient_verify`) |
| Stateful node failure | crash of LEDGER-1 or NODE-2 | lost participant / lost coordinator | ledger replication + resync; journal recovery |

Control messages (`FAULT_INJECT`, `FAULT_CLEAR`, `FAULT_STATUS`) are never affected by an injected fault, so a node
can always be repaired remotely. The same messages work for local processes and Docker containers.

## 3. Mechanisms

### 3.1 Failure detector (`fault_tolerance/failure_detector.py`)
States `UP -> SUSPECT -> DOWN`. Every `I` = 0.5 s each target gets a PROBE with timeout `T` = 0.5 s.
One missed probe makes a node SUSPECT, `K` = 3 consecutive misses make it DOWN, one success makes it UP.
A failed real request also marks the node SUSPECT immediately (passive detection).

- Worst-case time to declare DOWN is about `K*I + T` = 2.0 s; to SUSPECT it is about `I + T` = 1.0 s; passive detection is the first failed request.
- Trade-off: lower `K` or `I` detects sooner but raises false positives; a wrongly suspected node is restored by the next successful probe.
- Message cost: `N` probes per interval, i.e. `N/I` = 12 messages/s for N = 6 targets (4 rating nodes + 2 ledgers).

### 3.2 Failover (`fault_tolerance/resilient_dispatcher.py`)
Candidate order is UP nodes, then SUSPECT, then DOWN (last resort, so a wrong detector can never remove all capacity).
A failed attempt is retried on the next candidate (at most 3 attempts) with a per-attempt timeout instead of the global one.
Round robin is the baseline pool policy; `least_loaded` (fewest requests in flight) is the Week 8 optimisation.

### 3.3 Replicated ledger and two-phase commit (`transactions/two_phase_commit.py`)
The ledger is a replica group (LEDGER-1, LEDGER-2). PREPARE, COMMIT and ABORT go to every replica and succeed when at
least one acknowledges (write quorum W = 1 of 2). A replica that missed a write sets `ledger_dirty`.

- **Resync** (`resync_ledger`) compares the coordinator journal with each replica's committed set, copies the missing
  transactions with parallel idempotent PREPARE+COMMIT, and aborts orphaned PREPARED rows.
- **Anti-entropy** (NODE-2 background thread) repeats resync while a replica is lagging and reachable. It is needed
  because a transaction that was in flight when a replica came back can commit on the other replica only.
- **Automatic re-drive:** when the detector sees a node recover, NODE-2 re-drives pending transactions; a periodic pass
  catches transactions that ended IN_DOUBT after a recovery snapshot. Recovery skips transactions this process is still
  executing (otherwise presumed-abort would abort live transactions).
- **Rating-node failover for 2PC:** if PREPARE cannot reach the rating node, the first attempt carries a durable ABORT
  decision (its status may stay IN_DOUBT until the dead node returns, because the ABORT cannot be delivered yet). It can never
  commit, so a second attempt on another node under a new id `<id>-R1` is safe.
- **Coordinator crash:** the SQLite journal survives; on restart NODE-2 recovers pending transactions (decided COMMIT is
  completed, undecided is presumed-aborted). Clients that retry with the same transaction id get the final outcome (exactly-once).

Message cost per transaction: `4 * (1 + R)` messages (2 phases x 2 directions x participants) = 12 with R = 2 ledger replicas versus 8 with one.

### 3.4 Byzantine defence
`process_verified` asks 3 nodes in parallel and accepts the majority charge (tolerates 1 faulty node out of 3; needs at least
2 answers). Deviating nodes are marked SUSPECT. Cost: 3x the rating work per request. It is only enabled when requested
(`ft_verify`), because crash-oriented failover cannot see a node that answers quickly with a wrong value.

### 3.5 Availability model
`Availability = T_up / T_total`, measured per 0.5 s bucket: a bucket is UP when at least 95 % of the requests completing in it were
successful *and* correct; an empty bucket is DOWN. `MTTR` runs from fault injection to the start of 2 consecutive UP buckets
after the first DOWN bucket (so it includes detection). A fault that never causes a DOWN bucket is reported as masked (MTTR = 0).
Analytical view: for independent replicas with availability `a` each, a replicated service is available with `1 - (1 - a)^R`
(R = 2: a = 0.99 gives 0.9999). The experiment measures what clients actually see, not this idealisation.

## 4. Week 8 — system models

| Model | How it is implemented here | Provisioned |
|---|---|---|
| Host-based | every client sends to one central host, NODE-3 | 1 node + N terminals |
| Server-based | dedicated servers per function: voice -> NODE-3, sms -> NODE-4, data -> NODE-5 | 3 nodes + N terminals |
| Processor pool (round robin) | clients submit to NODE-2, which allocates each request to any node of the 4-node pool | 4 nodes + dispatcher + N terminals |
| Processor pool (least loaded) | same pool, fewest-in-flight allocation | same cost as round robin |
| Workstation | each client processes its own work locally, no sharing | N workstations |
| Integrated | requests of 10 ms of work or less run on the client's workstation, heavy ones go to the least-loaded pool | N workstations + 4 nodes + dispatcher |

**Service demand** (emulated so results do not depend on the test machine): voice 20 ms, sms 5 ms, data 40 ms; mix 50 / 30 / 20 %,
mean `E[S]` = 19.5 ms. A node serves `NODE_SERVICE_SLOTS` = 1 request at a time and queues the rest, like one core or worker.

**Capacity bounds (theory) versus the reference run** (see `results/reference_run/`):

| Model | Bound | Measured (8 clients) |
|---|---|---|
| host_based | `1/E[S]` = 51.3 req/s | 49.4 |
| server_based | `1/max_i(p_i * S_i)` = 1/(0.5*20 ms) = 100 req/s (the voice server) | 100.9 |
| processor pool | `4/E[S]` = 205 req/s | 208.6 (least loaded), 187.2 (round robin) |
| workstation | `N/E[S]` = 410 req/s | 411.8 |

Utilisation is `rho = lambda / (c * mu)`. Round robin ignores request size, so a node can receive several 40 ms requests in a row and queue them;
least-loaded avoids that, which is why it reaches the bound and round robin does not at saturation.

**Three load types are used on purpose.** (A) Saturation (closed loop, no think time) gives capacity and scalability. (B) A fixed
offered load of 80 req/s for every model gives utilisation, tail latency and cost per request; at saturation workstations look 100 % busy and
cheap, which hides their real weakness (idle capacity). (C) A paced fault test (one component fails) gives reliability.

**Decision matrix.** Metrics are min-max normalised; utilisation is scored by closeness to a 70 % planning target; two weight profiles are
reported (balanced, telecom availability). The weights are judgement calls, not measurements, and the ranking can change with them.
A pass/fail **gate** is applied after the scores: billing needs one authoritative ledger and the Week 5 transactions, and the emulated rating step is
stateless so the numbers cannot see that requirement. The workstation model fails the gate even though it wins on raw metrics.

## 5. Known limitations (state these in the report and the defence)

1. NODE-2 (front-end and coordinator) is a single point of failure. Its crash is recovered by restart (MTTR about the restart time) but not masked.
2. Ledger durability needs at least one replica disk to survive. If the only replica that acknowledged a write is destroyed before resync, that write is only in the coordinator journal.
3. The Week 4 bully election does not re-run when a node returns after an omission or drop fault; the experiments therefore fault NODE-4 or NODE-3, not the leader.
4. Faults are per-node. Network partitions between groups of nodes were not injected.
5. All nodes run on one machine or one Docker host: no WAN latency, one shared clock, shared CPU. Absolute numbers depend on the machine; the shapes should not.
6. Week 8 capacity is emulated, the rating step is stateless and unit prices are illustrative. Replace the prices with your own through `--cost-config`.
7. Request-level success overstates availability when clients block: with an omission fault the baseline completes 99 % of the requests it manages to issue but is UP only 45 % of the time. Report availability, not request success alone.
8. The majority vote tolerates one Byzantine node of three and costs 3x the work; it is only valid for deterministic, stateless rating.
9. Docker Compose files were validated structurally; the container workflow has not been executed by the author of this package (no Docker in the build environment).
