Distributed_telecom_billing

1. Brief description of the project

The Distributed Telecom Billing and Rating System is a distributed computing application designed to process telecommunications usage records from multiple network nodes, calculate service charges and generate subscriber billing information.

The system distributes billing tasks across several processing nodes to improve processing efficiency, scalability and reliability. It incorporates distributed coordination, transaction management, concurrency control and fault tolerance to ensure that billing operations can continue even when individual nodes experience failures.

The project demonstrates how distributed computing concepts can be applied to real-world telecommunications billing systems.

2. Description of each milestone

The project develops progressively across 12 weeks, with each milestone introducing new distributed computing concepts and building upon the previous implementation.

Milestone 1: Weeks 1 and 2 – Distributed Operating Systems and Processing

This milestone establishes the foundation of the distributed billing system. It introduces distributed computing concepts and creates the initial processing environment where different nodes can participate in billing operations.

Main activities:
- Introduction to distributed operating systems.
- Creation and configuration of distributed processing nodes.
- Understanding communication between nodes.
- Implementation of basic billing and usage-record processing.
- Establishment of the initial system structure.

Milestone 2: Weeks 3 and 4 – System Architecture and Distributed Algorithms

This milestone focuses on developing the system architecture and introducing algorithms that coordinate operations across multiple nodes.

Main activities:
- Development of the distributed system architecture.
- Implementation of node coordination and workload distribution.
- Introduction of the Bully Election Algorithm for coordinator selection.
- Implementation of failure detection and recovery experiments.
- Integration of an Apache HTTP Server frontend.
- Development of a Power BI dashboard for monitoring and visualization.

The repository specifically documents Apache HTTP Server and Power BI integration for these weeks. 


Milestone 3: Weeks 5 and 6 – Transactions and Concurrency Control

This milestone introduces mechanisms for maintaining data consistency and managing simultaneous billing operations across distributed nodes.

Week 5 – Distributed Transactions
- Implementation of Two-Phase Commit (2PC).
- Introduction of transaction coordination.
- Application of ACID properties.
- Implementation of transaction logging and recovery.
- Use of SQLite for persistent transaction records.
- Testing transaction failures and recovery.

Week 6 – Concurrency Control
- Implementation of concurrent transaction processing.
- Identification and management of race conditions.
- Application of mutex locks.
- Development of deadlock detection mechanisms.
- Use of wait-for graphs.
- Implementation of deadlock prevention and recovery.

The milestone also introduces Docker Compose for deployment and Apache Spark for analysing experimental results. 

Milestone 4: Weeks 7 and 8 – Fault Tolerance and Transparency

This milestone focuses on improving system reliability and making the distributed environment operate more smoothly despite failures.

Main activities:
- Implementation of fault detection mechanisms.
- Development of node recovery procedures.
- Maintaining service availability when a processing node fails.
- Handling communication interruptions.
- Introducing replication and recovery mechanisms where applicable.
- Improving access to distributed resources through transparency.
The objective is to ensure that users can continue accessing billing services without needing to understand the internal distribution of processing tasks.
