# Distributed Telecom Billing and Rating System

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
