# Distributed Telecom Billing and Rating System

Weeks 3 to 10: Apache gateway, leader election, durable two-phase commit, concurrency and deadlock handling, fault tolerance, distributed-system models,
**transparency (Week 9)** and **naming and process management (Week 10)**, deployed with Docker Compose and analysed with Apache Spark.

Start here:
- `ASSIGNMENT3_WEEK9_10_STEP_BY_STEP.md` and `WEEK9_10_DESIGN.md` — Practical 2 (Weeks 9 and 10)
- `ASSIGNMENT3_WEEK7_8_STEP_BY_STEP.md` and `WEEK7_8_DESIGN.md` — Practical 1 (Weeks 7 and 8)
- `ASSIGNMENT3_WEEK9_10_REPORT_TEMPLATE.md`, `ASSIGNMENT3_WEEK7_8_REPORT_TEMPLATE.md` — report skeletons
- `logs/` — failure-driven engineering logs (weeks 3 to 10)
- `results/reference_run*/` — complete reference runs (sandbox, 1 vCPU): use them to check your output, **report your own numbers**

## Quick start (local)
    pip install -r requirements.txt
    python -m unittest discover -s tests          # 58 tests
    python launcher.py                            # terminal 1 (supervised; --no-supervise to leave crashed nodes down)

    # terminal 2
    python experiments/week9_transparency.py              # Week 9: transparency and overhead
    python experiments/week10_naming_processes.py         # Week 10: naming and process management
    python experiments/week7_fault_tolerance.py --repeats 3
    python experiments/week8_system_models.py

    # earlier weeks still work
    python experiments/week3_architecture.py ; python experiments/week4_election.py
    python experiments/week5_transactions.py ; python experiments/week6_concurrency.py

## Quick start (Docker)
    docker compose up --build -d
    docker compose --profile week9 run --rm week9-transparency        # or run the scripts from your venv
    docker compose --profile week10 run --rm week10-naming-processes
    docker compose --profile analytics910 run --rm spark-analytics-week9-10

## Large-scale workload (Week 3 onwards)
    python experiments/large_scale_workload.py --n 100000 --concurrency 50 --tag baseline

Dashboards: `powerbi/README.md`, `powerbi/week7_8_dashboard.md`, `powerbi/week9_10_dashboard.md`.
