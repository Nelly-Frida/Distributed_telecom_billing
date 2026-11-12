# Distributed Telecom Billing and Rating System

Weeks 3 & 4 — Apache HTTP Server frontend + Power BI dashboard.

See `logs/` for engineering logs and `apache/README.md` for setup.

## Quick start
    pip install -r requirements.txt
    python launcher.py               # in terminal 1
    python experiments/week3_architecture.py   # in terminal 2
    python experiments/week4_election.py
    python experiments/week4_failure.py

## Large-scale workload (e.g. 100,000 requests)
    python experiments/large_scale_workload.py --n 100000 --concurrency 50 --tag baseline

See `powerbi/README.md` for how the --tag flag maps onto Power BI filtering,
and for tips on displaying 100k+ rows cleanly.


## Assignment 3 — Weeks 5 and 6

This version adds durable distributed transactions and expanded concurrency/deadlock experiments. See `ASSIGNMENT3_WEEK5_6_STEP_BY_STEP.md` for the complete Windows, Docker Compose and Spark workflow.

### Week 5 commands
```bash
python experiments/week5_transactions.py
python experiments/week5_transaction_load.py --transactions 100 --workers 10
python experiments/week5_durability_check.py
```

### Week 6 command
```bash
python experiments/week6_concurrency.py
```

### Docker deployment
```bash
docker compose up --build -d
```

### Spark analytics (after both experiments generated their CSV files)
```bash
docker compose --profile analytics run --rm spark-analytics
```

Spark is used for batch analysis of experiment outputs; the 2PC protocol and lock manager remain in the Python distributed services.
