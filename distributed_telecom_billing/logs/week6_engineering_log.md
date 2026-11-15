# Week 6 Engineering Log — Concurrency and Deadlock

## What changed
Implemented a resource manager that tracks ownership and waiting requests, builds a wait-for graph, detects cycles, selects a deterministic victim, releases victim resources and allows the surviving transaction to continue. Added globally ordered multi-resource acquisition as a prevention strategy and a race-condition/mutex demonstration.

## What failed / risk observed
The original demonstration could time out before both wait edges were simultaneously visible. It also did not compare a prevention strategy against the detection/recovery approach or quantify lost updates.

## Why
The wait-for graph must be observed while both transactions are waiting. A single deadlock scenario does not demonstrate prevention, race conditions or the impact of contention.

## Fix
Synchronise initial resource ownership, poll until a cycle appears, abort the selected victim, and measure the survivor. Use sorted lock acquisition to prevent circular wait. Compare unsynchronised and mutex-protected counter updates and verify account-balance conservation.

## Alternative considered
Timeout-only recovery is simpler but cannot distinguish deadlock from a slow transaction. Global lock ordering prevents circular wait but can constrain scheduling flexibility.

## Lesson
Deadlock detection identifies a cycle after it forms; ordered locking prevents circular wait by enforcing a consistent resource acquisition order.

## Evidence to record
Run `python experiments/week6_concurrency.py`. Attach the CSV/JSON outputs, wait-for graph, selected victim, transaction throughput, wait times, mutex counter comparison and final balance check.
