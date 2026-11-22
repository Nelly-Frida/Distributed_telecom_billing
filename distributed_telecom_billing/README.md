# Distributed Telecom Billing and Rating System

<<<<<<< HEAD
## 1. Project Overview
A distributed telecommunications billing and rating prototype. Telecom usage records are generated, sent to a billing/scheduling service, and distributed across rating nodes.

## 2. Architecture
Node 1: Customer/workload generator
Node 2: Billing and scheduler
Nodes 3–6: Rating services

Node 1 → Node 2 → Rating Node → Node 2 → Node 1

## 3. Distributed System Model
Nodes + Processes + Resources + Communication.

## 4. Process Model
Processes include the customer workload generator, billing service, dispatcher, and rating services.

The launcher creates independent OS processes using subprocesses.

## 5. Resource Model
Resources include CPU, memory, TCP connections, socket buffers, and rating computation capacity.

## 6. Scheduling
Node 2 uses round-robin scheduling to distribute requests across active rating nodes.

## 7. Communication
TCP sockets with newline-delimited JSON are used.

## 8. Baseline
One rating node:
Node 1 → Node 2 → Node 3

## 9. Proposed Configurations
Two rating nodes:
Node 1 → Node 2 → Node 3/4

Four rating nodes:
Node 1 → Node 2 → Node 3/4/5/6

## 10. Performance Metrics
- Throughput
- Latency
- Jitter
- Application-level request loss
- CPU utilization
- Memory utilization
- Load distribution

## 11. Bottleneck Analysis
Potential bottlenecks include Node 2, rating nodes, TCP communication, CPU and memory.

## 12. Experimental Method
Hypothesis → Experimental Design → Implementation → Measurement → Analysis → Conclusion.

## 13. Reproducibility
See logs/reproducibility.md.

## 14. Failure-Driven Engineering
See logs/week1_engineering_log.md and logs/week2_engineering_log.md.

## 15. Running the Project

Install dependencies:
`pip install -r requirements.txt`

Select rating nodes in `common/config.py`.

Start distributed processes:
`python launcher.py`

Run experiment:
`python experiments/run_experiment.py`

## 16. Assignment 1 Evolution
Week 1 establishes nodes, processes, resources and communication.
Week 2 distributes rating computation and adds quantitative performance measurement.

Future weeks will extend the same system with distributed architecture, coordination, transactions, concurrency, fault tolerance, transparency, naming, RPC, distributed state and storage.
=======
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
>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a
