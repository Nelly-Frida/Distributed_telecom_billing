# Assignment 3 — Weeks 5 and 6
## Distributed Telecom Billing and Rating System

This implementation extends the existing Weeks 1–4 system. It does not replace the earlier billing, rating, coordination or Lamport-clock services.

## Deliverables

### Week 5 — Distributed transactions
- Node 2 acts as the 2PC coordinator.
- A rating node and the ledger act as transaction participants.
- Participant state is stored in SQLite with WAL and synchronous durability.
- The coordinator persists its transaction journal before and during the protocol.
- A global COMMIT decision is written durably before participant COMMIT messages are sent.
- Recovery applies presumed-abort when no durable decision exists, and retries COMMIT when the journal records a COMMIT decision.
- The experiment tests normal commits, a controlled abort, a coordinator interruption after the durable COMMIT decision, recovery, and rating/ledger charge consistency.

ACID mapping:
- **Atomicity:** both participants are committed, or a transaction without a commit decision is aborted. A transaction with a durable COMMIT decision is retried until participants converge.
- **Consistency:** input validation and the rating calculation are applied before the transaction is accepted; the experiment compares the rating and ledger charge.
- **Isolation:** each SQLite participant operation uses an immediate transaction and a process-local lock around participant state changes.
- **Durability:** SQLite WAL with `synchronous=FULL` stores participant state and the coordinator journal on persistent files/volumes.

2PC sequence:

```text
Client -> NODE-2 coordinator
              | PREPARE
              +------> Rating participant (NODE-3)
              +------> Ledger participant
              |<------ Prepared votes
              | Persist global COMMIT decision
              +------> COMMIT to rating participant
              +------> COMMIT to ledger
              |<------ Commit acknowledgements
              +------> Transaction result
```

### Week 6 — Concurrency and deadlock management
- A wait-for graph records transaction-to-transaction dependencies.
- A deliberate circular wait is created between TX-A and TX-B.
- The detector finds the cycle and selects a deterministic victim.
- Aborting the victim releases its resources so the surviving transaction completes.
- A global lock-ordering policy is then tested as a deadlock-prevention strategy.
- A race-condition demonstration compares unsynchronised counter updates with mutex-protected updates.
- The ordered-lock workload records throughput, wait time, timeouts/starvation indicators and balance conservation.

## Option A — Run locally on Windows

Use Python 3.11. Open PowerShell in the extracted `distributed_telecom_billing` folder.

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Terminal 1 — start all distributed processes:

```powershell
python launcher.py
```

Terminal 2 — run Week 5:

```powershell
python experiments/week5_transactions.py
```

Run the concurrent transaction throughput test (default 50 transactions and 8 workers):

```powershell
python experiments/week5_transaction_load.py
```

You can increase the workload, for example:

```powershell
python experiments/week5_transaction_load.py --transactions 500 --workers 20
```

Then run Week 6:

```powershell
python experiments/week6_concurrency.py
```

The launcher must remain running while the Week 5 experiment executes. Week 6 is a self-contained concurrency experiment and can run independently.

## Option B — Docker deployment

Install and start Docker Desktop first. From the project root:

```powershell
docker compose up --build -d
docker compose ps
```

View service logs:

```powershell
docker compose logs -f node2 node3 node4 node5 node6 ledger billing-api
```

Wait about 10 seconds for all nodes and the Bully election to settle before sending transactions. Do not run the local `launcher.py` at the same time as Docker because both deployments use the same host ports. The Docker deployment exposes the billing API on port 8000, the coordinator on 5002, the ledger on 5007 and rating participants on 5003–5006. SQLite databases are stored in named Docker volumes.

Run the experiments from a local Python terminal while the Docker services are running:

```powershell
python experiments/week5_transactions.py
python experiments/week5_transaction_load.py --transactions 100 --workers 10
python experiments/week6_concurrency.py
```

To verify participant and coordinator state survives service restarts, restart the stateful services and run the durability check:

```powershell
docker compose restart node2 node3 ledger
python experiments/week5_durability_check.py
```

For local deployment, stop the launcher with `CTRL+C`, start it again with `python launcher.py`, then run `python experiments/week5_durability_check.py`.

Test the HTTP transaction endpoint:

```powershell
$body = '{"transaction_id":"MANUAL-001","customer_id":"C001","service":"data","usage":10}'
Invoke-RestMethod -Uri "http://127.0.0.1:8000/bill/transaction" -Method POST -ContentType "application/json" -Body $body
```

Stop the containers:

```powershell
docker compose down
```

Do not use `docker compose down -v` unless you intentionally want to delete the persistent transaction databases and coordinator journal.

## Spark analytics

Spark is used for post-experiment analytics, not for implementing 2PC or locking. First generate both CSV files by running the Week 5 and Week 6 experiments. Then:

```powershell
docker compose --profile analytics run --rm spark-analytics
```

Spark reads:
- `results/week5_transactions.csv`
- `results/week6_concurrency.csv`

It aggregates transaction counts, mean and p95 transaction latency, concurrency outcomes and mean/max wait duration. Output is written under `results/spark_analytics/` as Spark CSV part files.

## Evidence and generated files

| File | Evidence |
|---|---|
| `results/week5_transactions.csv` | Per-transaction PREPARE/COMMIT timing and status |
| `results/week5_experiment_summary.json` | Controlled abort, recovery and participant consistency |
| `results/week5_load_test.csv` | Concurrent transaction latency and outcome records |
| `results/week5_load_test_summary.json` | Transaction throughput, mean/p95 latency and commit rate |
| `experiments/week5_durability_check.py` | Confirms coordinator, rating and ledger state after restart |
| `results/week6_concurrency.csv` | Deadlock and ordered-lock outcomes |
| `results/week6_experiment_summary.json` | Race condition, wait-for graph, victim, throughput and balance check |
| `data/coordinator.db` | Durable coordinator journal (local deployment) |
| `data/node*_rating_transactions.db` | Durable rating participant state (local deployment) |
| `data/ledger_transactions.db` | Durable ledger state (local deployment) |

## Experimental method

For each experiment document: **Hypothesis → Experimental design → Implementation → Measurement → Analysis → Conclusion**.

Do not copy example measurements from another machine into the report. Run the scripts on your own machine and use the generated CSV/JSON outputs as your measured evidence. Record CPU, memory, OS, Python version, Docker version, dataset size and run count.

## Important scope note

This is a teaching-scale distributed prototype. SQLite provides durable local participant state; it is not a substitute for a production replicated database. The controlled coordinator interruption exercises recovery from a persisted decision; it does not simulate every possible network partition or Byzantine failure. Spark is a batch analytics layer and does not participate in the transaction commit path.
