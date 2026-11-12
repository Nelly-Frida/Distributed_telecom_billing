# Week 5 Engineering Log — Distributed Transactions

## What changed
Reworked the original in-memory 2PC participants into SQLite-backed durable participants. Added a persistent coordinator journal, idempotent participant commit handling, a write-ahead global COMMIT decision, presumed-abort recovery and retryable commit recovery.

## What failed / risk observed
The earlier implementation kept participant state in process memory and returned IN_DOUBT after a commit communication error without a durable decision record. Restarting a participant could therefore lose its prepared/committed state, and a coordinator restart had no reliable recovery source.

## Why
Transaction state and the coordinator's global decision were not persisted independently of process lifetime.

## Fix
Persist participant state and coordinator decisions in SQLite. Write COMMIT_DECIDED before sending any COMMIT message. On recovery, abort transactions without a durable commit decision and retry commit for transactions whose decision is COMMIT.

## Alternative considered
A replicated database or distributed consensus-backed transaction log would be more suitable for production, but SQLite is appropriate for this local teaching prototype and makes durability behavior demonstrable.

## Lesson
2PC needs a durable coordinator decision log; participant acknowledgements alone are insufficient for recovery.

## Evidence to record
Run `python experiments/week5_transactions.py`. Attach `results/week5_transactions.csv`, `results/week5_experiment_summary.json`, the ledger counts and the participant consistency checks. Record the actual machine-specific timings.
