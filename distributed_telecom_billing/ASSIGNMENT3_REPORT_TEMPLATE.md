# Assignment 3 — Distributed Telecom Billing and Rating System
## Milestone 5: Distributed Transactions and Milestone 6: Concurrency and Deadlock Management

> Replace every bracketed prompt with results measured on your own machine. Do not use values from another computer as your experimental results.

## 1. Introduction

This assignment extends the Distributed Telecom Billing and Rating System by introducing durable distributed transactions and concurrency control. The billing coordinator (NODE-2) coordinates a rating participant (NODE-3 by default) and a distributed ledger. Multiple concurrent billing/settlement operations also compete for shared account resources.

## 2. Objectives

### Week 5
- Implement a distributed transaction protocol using Two-Phase Commit.
- Demonstrate ACID-related behavior and durable participant state.
- Handle aborts and coordinator interruption.
- Recover from a durable transaction decision.
- Measure transaction latency, throughput, failure behavior and participant consistency.

### Week 6
- Demonstrate race conditions and mutual exclusion.
- Model resource ownership and waiting dependencies.
- Detect and recover from deadlock.
- Prevent deadlock through a global resource-locking order.
- Evaluate contention, wait time, throughput and starvation indicators.

## 3. System Architecture

```text
Client / Billing API
        |
        v
NODE-2: 2PC Coordinator + Durable Journal
        |                         |
        v                         v
NODE-3: Rating Participant     Ledger Participant
        |                         |
        +------ SQLite state -----+

Concurrent account operations
        |
        v
Resource Manager -> Wait-for Graph -> Deadlock Detector -> Victim Recovery
        |
        +-> Ordered-lock prevention

Experiment CSV/JSON -> Apache Spark batch analytics -> Power BI
```

Docker Compose packages NODE-2, NODE-3 to NODE-6, the ledger and the billing API as separate services. Persistent volumes retain coordinator and participant SQLite state. Spark is used only to analyse experiment output; it is not part of the 2PC commit path.

## 4. Week 5 — Distributed Transactions

### 4.1 Two-Phase Commit

**Phase 1 — Prepare:** NODE-2 sends PREPARE to the selected rating participant and ledger. Each participant validates and durably stores its prepared state before voting yes.

**Phase 2 — Decision:** NODE-2 persists a global COMMIT or ABORT decision. For COMMIT, it then sends COMMIT to both participants. Participants process repeated COMMIT messages idempotently.

The coordinator writes the global COMMIT decision before sending participant commit messages. If the coordinator restarts with a durable COMMIT decision, recovery retries COMMIT. If no decision was persisted, recovery uses presumed abort.

### 4.2 ACID mapping

- **Atomicity:** The coordinator's durable decision is applied to both participants. A commit decision is retried until acknowledgements are received; an undecided transaction is aborted during recovery.
- **Consistency:** The rating engine validates service types and computes the charge; negative usage is rejected. The experiment compares the rating and ledger charge.
- **Isolation:** SQLite transactions and participant locks protect state transitions from concurrent modifications.
- **Durability:** SQLite WAL-backed participant databases and the coordinator journal persist state across process restarts. Docker named volumes preserve the files across container recreation unless volumes are explicitly deleted.

### 4.3 Hypotheses

- H5-1: Successful 2PC transactions will leave both rating and ledger participants in COMMITTED state with equal charges.
- H5-2: A controlled failure before the global decision will produce ABORT.
- H5-3: A coordinator interruption after persisting COMMIT will be recoverable by replaying COMMIT messages.

### 4.4 Experimental design

Run 10 normal transactions, one controlled abort after PREPARE, one simulated coordinator interruption after the durable COMMIT decision, and a concurrent transaction load test (default 50 transactions with 8 workers). Run the recovery procedure and compare participant states and charges. Restart NODE-2, NODE-3 and the ledger, then run the durability check.

### 4.5 Results

| Metric | Measured value |
|---|---:|
| Normal transactions submitted | [ ] |
| Successful commits | [ ] |
| Aborts | [ ] |
| Transactions recovered | [ ] |
| Mean transaction latency (ms) | [ ] |
| Concurrent transaction throughput (transactions/s) | [ ] |
| Concurrent mean / p95 latency (ms) | [ ] / [ ] |
| Mean PREPARE latency (ms) | [ ] |
| Mean COMMIT latency (ms) | [ ] |
| Participant consistency checks passed | [ ] / [ ] |
| Durability check after restart | [PASS/FAIL] |

Evidence: `results/week5_transactions.csv`, `results/week5_experiment_summary.json` and the output of `python experiments/week5_durability_check.py`.

### 4.6 Analysis and conclusion

[Explain whether H5-1 to H5-3 were supported. Discuss the latency cost of contacting two participants, the effect of the extra durable journal writes, and the behavior observed during recovery.]

## 5. Week 6 — Concurrency and Deadlock Management

### 5.1 Race condition and mutual exclusion

The experiment runs concurrent increments against a shared counter. The unsynchronised version allows interleaving between read and write operations. The mutex-protected version serialises the critical section. Compare the final count with the expected number of increments.

### 5.2 Resource-allocation and wait-for graphs

TX-A acquires ACCOUNT-A and requests ACCOUNT-B. TX-B acquires ACCOUNT-B and requests ACCOUNT-A. The resource-allocation graph contains allocated edges from each account to its owner and request edges from each transaction to the resource it is waiting for. The corresponding wait-for graph contains TX-A → TX-B and TX-B → TX-A, forming a cycle.

The detector selects a victim, aborts it and releases its resources. The surviving transaction then acquires the resource and completes.

### 5.3 Deadlock prevention

The prevention experiment requires every transaction to acquire resources in the same sorted order. This removes circular wait from the tested workload. The experiment also checks account-balance conservation.

### 5.4 Hypotheses

- H6-1: Unsynchronised updates will lose increments under forced interleaving, while mutex-protected updates will reach the expected count.
- H6-2: The wait-for graph will reveal the deliberately created cycle.
- H6-3: Aborting one selected victim will release resources and allow the surviving transaction to complete.
- H6-4: Global lock ordering will prevent deadlock in the tested concurrent workload.

### 5.5 Results

| Metric | Measured value |
|---|---:|
| Expected counter value | [ ] |
| Unsynchronised counter value | [ ] |
| Mutex-protected counter value | [ ] |
| Deadlock detected | [YES/NO] |
| Selected victim | [ ] |
| Surviving transaction result | [ ] |
| Ordered-lock transactions completed | [ ] / [ ] |
| Ordered-lock throughput (transactions/s) | [ ] |
| Mean resource wait (ms) | [ ] |
| Timeouts observed | [ ] |
| Final account balance total | [ ] |

Evidence: `results/week6_concurrency.csv` and `results/week6_experiment_summary.json`.

### 5.6 Analysis and conclusion

[Explain the observed lost updates, mutex correctness, wait-for cycle, victim selection and recovery. Compare deadlock detection/recovery with ordered-lock prevention. A bounded experiment with no timeout is evidence that starvation was not observed in that run; it is not a proof that starvation is impossible under every workload.]

## 6. Docker and Big-Data Integration

Docker Compose deploys each billing, rating and ledger service independently and uses persistent volumes for transaction state. This demonstrates service decomposition, deployment repeatability and state persistence.

Apache Spark reads the Week 5 and Week 6 CSV files and computes aggregate transaction counts, mean and p95 transaction latency, concurrency outcomes and wait-time summaries. Power BI can then visualise the raw experiment CSVs and Spark aggregate outputs.

## 7. Limitations

- This is a teaching-scale prototype; SQLite is not a production replicated distributed database.
- Two-Phase Commit can block while the coordinator or a participant is unavailable. The durable journal supports recovery once services return.
- The coordinator interruption test is a controlled failure point after the durable decision, not an exhaustive network-partition test.
- The Week 6 experiment uses a local threaded resource manager to demonstrate concurrency algorithms; it does not claim that the lock manager is a distributed consensus service.
- Spark performs batch analytics and is not involved in transaction execution.

## 8. Overall Conclusion

[Summarise how the system evolved from basic distributed billing to a transaction-aware and concurrency-controlled system. State the measured evidence and the next improvements required for the following milestone.]

## 9. Reproducibility Record

| Item | Recorded environment |
|---|---|
| Operating system | [ ] |
| CPU / cores | [ ] |
| RAM | [ ] |
| Python version | [ ] |
| Docker Desktop / Engine version | [ ] |
| Spark version | [ ] |
| Experiment run date | [ ] |
| Transaction count / workload | [ ] |
