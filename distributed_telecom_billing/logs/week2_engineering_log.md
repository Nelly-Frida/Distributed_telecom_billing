# Week 2 Failure-Driven Engineering Log

## What Changed?
Rating computation was distributed across multiple TCP-connected rating nodes. Round-robin scheduling and performance monitoring were introduced.

## What Failed?
The first implementation used logical Python objects rather than independent network processes.

## Why Did It Fail?
The nodes were executing inside one Python process, so communication did not represent actual network communication.

## How Was It Fixed?
Rating services were implemented as independent TCP servers.

## Alternative Considered
ThreadPoolExecutor was considered, but independent TCP processes were retained for meaningful distributed latency measurements.

## What Was Learned?
Adding nodes does not automatically improve performance; workload distribution and measured overhead must be evaluated experimentally.
