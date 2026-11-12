# Week 2 Experimental Methodology

## Hypothesis
Increasing the number of distributed rating nodes will increase available processing capacity and improve workload distribution compared with a single-rating-node baseline, while additional communication may introduce overhead.

## Experimental Design

### Independent Variable
Number of rating nodes: 1, 2, and 4.

### Dependent Variables
- Throughput
- Average latency
- Average jitter
- Application-level request loss
- CPU utilization
- Memory utilization
- Load distribution

### Controlled Variables
- Hardware
- Operating system
- Python version
- Software version
- Workload
- Random seed
- Network topology
- Request format
- Rating algorithm
- Requests per experiment

## Implementation
Node 1 generates workload. Node 2 performs billing and scheduling. Nodes 3–6 perform rating. TCP is used for communication and round-robin scheduling is used by Node 2.

## Measurement
Throughput = successful requests / total experiment time.

Latency = response time - request time.

Jitter = average absolute difference between consecutive latency values.

Request loss = failed requests / total requests × 100.

CPU and memory are measured using psutil.

## Analysis
Compare the 1-node baseline with 2-node and 4-node configurations. Analyze request distribution and identify bottlenecks at Node 2, rating nodes, communication and system resources.

## Conclusion
The conclusion must be based on measured experimental results. No performance improvement should be claimed without experimental evidence.
