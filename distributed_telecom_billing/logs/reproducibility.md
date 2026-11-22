# Reproducibility Information

## Hardware
Computer: [ENTER COMPUTER MODEL]
CPU: [ENTER CPU]
RAM: [ENTER RAM]
Storage: [ENTER STORAGE]
Network adapter: [ENTER NETWORK ADAPTER]

## Operating System
OS: [ENTER OS]
Version: [ENTER VERSION]

## Software
Programming language: Python
Python version: [RUN `python --version`]
psutil version: [RUN `pip show psutil`]

## Network Topology
All nodes initially run on the same host using localhost.

Node 1: 127.0.0.1:5001
Node 2: 127.0.0.1:5002
Node 3: 127.0.0.1:5003
Node 4: 127.0.0.1:5004
Node 5: 127.0.0.1:5005
Node 6: 127.0.0.1:5006

Communication protocol: TCP
Application protocol: JSON over TCP

## Workload
Requests: 1000 / 5000 / 10000
Services: voice, sms, data
Random seed: 42

## Experimental Parameters
Baseline: 1 rating node
Distributed: 2 rating nodes
Scalable: 4 rating nodes
Scheduling: Round Robin
Timeout: 5 seconds

## Reproduction Procedure
1. Install Python.
2. Install requirements.
3. Select ACTIVE_RATING_NODES in common/config.py.
4. Start launcher.py.
5. Run experiments/run_experiment.py.
6. Save CSV results.
7. Repeat with the same workload and seed.
