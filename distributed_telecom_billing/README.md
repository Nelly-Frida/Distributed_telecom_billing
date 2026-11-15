# Distributed Telecom Billing and Rating System

Weeks 3 to 8: Apache gateway, leader election, durable two-phase commit, concurrency and deadlock handling,
**fault tolerance and recovery (Week 7)** and **distributed-system models (Week 8)**, deployed with Docker Compose and analysed with Apache Spark.

Start here:
- `ASSIGNMENT3_WEEK7_8_STEP_BY_STEP.md` — how to run and present Weeks 7 and 8
- `WEEK7_8_DESIGN.md` — fault model, mechanisms, system models, limitations
- `ASSIGNMENT3_WEEK7_8_REPORT_TEMPLATE.md` — report skeleton
- `logs/` — failure-driven engineering logs (weeks 3 to 8)
- `results/reference_run/` — a complete reference run (sandbox, 1 vCPU): use it to check your output, **report your own numbers**

## Quick start (local)
    pip install -r requirements.txt
    python -m unittest discover -s tests          # 29 tests
    python launcher.py                            # terminal 1 (supervised; --no-supervise to leave crashed nodes down)

    # terminal 2
    python experiments/week7_live_demo.py --mode baseline     # failure demo for the defence
    python experiments/week7_live_demo.py --mode resilient
    python experiments/week7_fault_tolerance.py --repeats 3   # Week 7 experiment
    python experiments/week8_system_models.py                 # Week 8 experiment

    # earlier weeks still work
    python experiments/week3_architecture.py ; python experiments/week4_election.py
    python experiments/week5_transactions.py ; python experiments/week6_concurrency.py

## Quick start (Docker)
    docker compose up --build -d
    docker compose --profile week7 run --rm week7-fault-tolerance
    docker compose --profile week8 run --rm week8-system-models
    docker compose --profile analytics78 run --rm spark-analytics-week7-8

## Large-scale workload (Week 3 onwards)
    python experiments/large_scale_workload.py --n 100000 --concurrency 50 --tag baseline

See `powerbi/README.md` and `powerbi/week7_8_dashboard.md` for the dashboards.
