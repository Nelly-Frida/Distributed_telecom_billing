# Week 9 Engineering Log — Transparency and Reliability

> Observed while this version was built and validated (Linux sandbox, 1 vCPU, local launcher). Keep what is true for you, correct the numbers with your own runs
> and **add your own failures**. Reference data: `results/reference_run_week9_10/`.

## What changed
A transparency gateway in NODE-2 (logical names, replicated writes, failover, stale-location handling, repair, live migration and re-replication behind a freeze gate); account partitions
hosted by the rating nodes (append-only operation sets, idempotent, `NOT_OWNER` for partitions a node does not own); a placement table in the naming service; a transparent client and a deliberately
non-transparent baseline client; HTTP access (`POST /logical`); the Week 9 experiment (access, location, replication, migration, failure, overhead) and a Spark job that recomputes the numbers.

## What failed
The failures that matter in this milestone are the **baseline's**, and they are the evidence:

| Experiment | Transparent client | Direct (non-transparent) client |
|---|---|---|
| Partition moved under load | 0 errors in 2,376 + 486 + 6,072 requests (before, during, after) | 54 % errors during the move and **100 % after it**, because its placement snapshot is stale and nothing tells it |
| Replica node crashed (1.55 s outage) | 0 errors in 1,880 requests during the outage | 92.6 % errors in 3,731 requests during the outage |
| Replication factor 1 -> 2 -> 3 -> 2 | 0 errors, 0 lost writes, replicas converged | not applicable: the direct client cannot change the replication factor at all |

No request failed through the gateway in the reference run, and no acknowledged write was lost (checked against the operation sets on the replicas).

## Why, how it was fixed, alternatives (design decisions and a wording error)
| # | Issue | Why | Resolution | Alternative considered |
|---|---|---|---|---|
| 1 | The first wording of hypothesis H9-3 said read latency does not depend on the replication factor. | In the mixed read/write phases read latency rose from 5.1 ms (R = 1) to 9.0 ms (R = 3) because writes share the nodes; in the read-only overhead test it moved only from 5.8 to 6.2 ms. | The hypothesis now says "almost independent in a read-only load; it rises in a mixed load", and both measurements are reported. | Report only the read-only figure (selective). |
| 2 | How to make a retry safe when a gateway request times out after the node applied it. | A timeout does not tell the caller whether the operation happened. | Every operation carries an id; applying it twice changes nothing. Retry and repair are then trivially safe, and repair is a set union. | Version numbers with a sequencer in the gateway (a restart of NODE-2 would have to recover them from the replicas). |
| 3 | Consistency level of replicated writes. | Quorum or consensus replication (Raft) would give linearizability but costs a leader election and more messages per write. | Write-all-available with stale marking and union repair; the limitation (not linearizable, a write held by one disk only can be lost) is stated in the design. | Quorum reads and writes (R + W > N), or Raft. |
| 4 | Where the freeze should sit during migration. | Copying everything while frozen would make the pause grow with the partition size. | Pre-copy while traffic continues, then freeze only for the operations that arrived meanwhile: 12.9 ms for a 1,290-operation partition, 1.7 to 19.7 ms across the reconfigurations. | Freeze for the whole copy (simpler, longer pause). |

## Findings that are lessons
- **Transparency has a price and it is measurable:** about 1 to 2 ms for the extra hop (+41 % on a read, +20 % on a write at saturation), about 4 to 5 ms per extra replica on a write, almost nothing per replica on a read, 0.277 ms to resolve a name cold and 0.004 ms warm.
- **A stale location is a protocol problem, not a bug:** the fix is not a shorter cache time but a node that says `NOT_OWNER`, so the caller knows to look the name up again.
- **The migration freeze is the only visible pause** and it scales with the operations that arrive during the pre-copy, not with the partition size.

## What was learned
Transparency is a set of mechanisms with a cost, not one feature: location transparency needs naming, replication transparency needs a consistency model, migration transparency needs a switch protocol, failure transparency needs idempotent operations and repair. Measure the non-transparent alternative on the same load; its failures are the argument.

## Evidence to record
`week9_summary.csv`, `week9_requests.csv`, `week9_experiment_summary.json` (reconfiguration and verification details), the Spark tables (`week9_error_timeline` for the migration and crash stories, `week9_overhead`), the output of `week9_transparency.py` for the defence.
