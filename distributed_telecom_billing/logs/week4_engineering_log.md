# Week 4 Failure-Driven Engineering Log

## What Changed?
Added Lamport logical clocks to every rating node and a Bully-algorithm
leader election. The elected node runs a Coordinator that tracks
heartbeats and writes coordination.csv.

## What Failed?
First election run: all four rating nodes declared themselves leader
simultaneously.

## Why Did It Fail?
Nodes start sequentially but within ~1 second. NODE-3 started first,
found no higher peers alive, and became leader. Same for NODE-4, etc.

## How Was It Fixed?
Added a 2-second startup delay in BullyElection.run() before the first
election. All higher-id peers are listening by then.

## Alternative Considered
Raft-style two-phase election. Rejected as over-engineering at this scale.

## What Was Learned?
Distributed algorithms assume a system model. Enforcing that assumption
at startup is usually simpler than modifying the algorithm.
