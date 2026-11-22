# Week 10 Engineering Log — Naming and Distributed Process Management

> Observed while this version was built and validated (Linux sandbox, 1 vCPU). Keep what is true for you, correct the numbers with your own runs and
> **add your own failures**. Reference data: `results/reference_run_week9_10/`.

## What changed
A naming subsystem (durable record store with exact and longest-prefix resolution; four name-server deployments: central, root, account zone, service zone; five resolver strategies: static, broadcast,
central, DNS-style with TTL cache, prefix table; an admin that writes identical records into every deployment); a process runtime in the rating nodes and a process manager in NODE-2
(create, schedule with three policies, locate by name, coordinate with a barrier and reduce, terminate with cascade, recover from a checkpoint, migrate); the Week 10 experiment and Spark job.

## What failed (and why, and the fix) — found while building and testing

| # | What failed or surprised | Why | How it was fixed | Alternative considered |
|---|---|---|---|---|
| 1 | Found in code review before the first run: the broadcast resolver stored the name being looked up in shared state. | Two threads resolving at the same time would overwrite each other's name and get wrong answers. | The name is captured per lookup. The concurrent-clients measurement depends on it. | A lock around the lookup (would serialise all lookups and distort the measurement). |
| 2 | Found in review: the static and prefix resolvers scanned every record for each lookup. | The scan costs O(records), so the "names" scalability test would have measured the scan, not the strategy. | A length-indexed prefix match (cost depends on the number of distinct prefix lengths): lookup stays about 0.002 ms at 50,000 names. | A trie (more code, same result for fixed-length prefixes). |
| 3 | Found in review: finished tasks were never released from the node runtimes, and locating a finished process failed. | The manager removed tasks only on terminate. | Finished tasks are reaped by the monitor; locate answers for DONE or FAILED processes from the process table. After the full run: 0 tasks left on any node. | Keep tasks forever (leaks memory and slots). |
| 4 | The first recovery run reported 2 tasks on the crashed node but only 1 rescheduled. | The count included a task that was probably already finished when the crash was injected (1.0 s after the start). | Count only RUNNING tasks and inject the crash earlier (0.6 s): 2 of 2 rescheduled, 0.55 s after the crash, result exact. | Accept the first number (it hid a measurement error). |
| 5 | One of the two rescheduled tasks restarted from record 0, the other from record 213. | The manager checkpoints by polling every 0.25 s; the task that restarted at 0 had no checkpoint yet. | None needed for correctness (the result is exact either way); the poll interval trades checkpoint freshness against overhead. Documented. | Checkpoints pushed by the node after every N records. |
| 6 | Found in review of the staleness test: after resetting the placement the gateway still held the old prefix table. | The gateway caches the table; a reset changes it behind the gateway's back. | Every placement reset flushes the gateway cache (`LOGICAL_INVALIDATE`) in one helper used by all parts. | Restart NODE-2 between parts (slow). |

## Findings that are lessons, not bugs
- **Latency follows messages**: about 0.1 ms per message on loopback. central 0.097 ms (1 message), DNS cold 0.189 ms (2), broadcast 2.21 ms (4, plus waiting for the slowest reply); warm caches cost 0.001 to 0.014 ms.
- **Broadcast is the only strategy whose cost grows with the number of nodes** (0.66, 1.29, 1.97, 4.23, 7.35 ms for 4, 8, 16, 32, 64 endpoints); **a single central server saturates** at about 10,000 lookups/s while warm DNS and prefix caches do not.
- **The prefix table moves the cost, it does not remove it:** lookups stay at 0.002 ms, but loading the table takes 8.7 ms for 1,000 names and 1.1 s for 50,000.
- **Staleness is the price of caching:** static is wrong forever, DNS for one TTL (2.0 s in the test), a prefix table until a node says `NOT_OWNER`. With that reply both recover in 2 attempts.
- **Name-server outage:** only the caches (warm DNS, warm prefix) and the strategies with no name server (static, broadcast) keep resolving.
- **Least-loaded scheduling pays on skewed work**: makespan 1.78 s against 2.22 s (round robin) and 2.62 s (random).
- **Exactly-once needs a deterministic task and a checkpoint of both the position and the partial result**; with them a crash and a migration both give the exact result.

## What was learned
A name is an indirection, and every indirection has a consistency problem: who learns about a change, and when. The strategies differ mainly in the answer. Process management reuses
the same ideas as the data path: a logical name that survives movement, a failure detector to decide where to run, and idempotent work so that recovery can be safe.

## Evidence to record
`week10_summary.csv`, `week10_lookups.csv`, `week10_process_events.csv`, `week10_experiment_summary.json`, the Spark tables (`week10_resolution_metrics`, `week10_latency_by_messages`) and the output of the experiment.
