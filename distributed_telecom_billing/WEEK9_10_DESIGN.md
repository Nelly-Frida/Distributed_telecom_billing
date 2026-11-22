# Weeks 9 and 10 — Design: transparency, naming and process management

Evolution: S1 prototype -> S3 Apache multi-tier -> S4 election and clocks -> S5 durable 2PC -> S6 concurrency and deadlock ->
S7 fault tolerance -> S8 system models -> **S9 transparency** -> **S10 naming and process management**.
Nothing was replaced. The Week 9 gateway sits in NODE-2 next to the Week 7 router; the Week 10 name servers are new processes; the
process runtime and the account store live inside the existing rating nodes, so they share the Week 7 fault injection and the Week 8 capacity model.

## 1. Architecture (new in Weeks 9 and 10 marked *)

```
 application --(logical name acct.C00042)--> NODE-2  * transparency gateway   * process manager
                                                |            |  resolve                |  schedule / locate / coordinate
                                                |            v                         v
                                                |   * NS-CENTRAL  NS-ROOT --referral--> NS-ACCT, NS-SVC    (name servers)
                                                v
                        NODE-3   NODE-4   NODE-5   NODE-6   (rating nodes)
                        * account partitions (SQLite, replicated)     * process runtime (tasks)
```

## 2. Week 9 — the five transparencies

The application uses a **logical resource** (`acct.C00042`) and never a physical one (node, replica, port, location, failure). The non-transparent
baseline (`DirectClient`) holds a snapshot of the placement and talks to the node itself, with no failover and no retry.

| Transparency | Mechanism | Evidence (reference run) |
|---|---|---|
| Access | one operation interface (`LOGICAL get/apply`) reachable over TCP and HTTP (`POST /logical`) | the same 60 operations give identical results over both paths (100 %); 1.24 ms over TCP, 1.92 ms over HTTP |
| Location | names are resolved by the naming service; no caller holds an address; the data physically sits where the name record says | 0 of 12 partitions differ from the name record; correct under all four gateway naming strategies |
| Replication | writes go to every available replica, reads to any up-to-date one; the replication factor changes at run time | R = 1 -> 2 -> 3 -> 2 under load with 0 errors, 0 lost writes, replicas converged |
| Migration | pre-copy, a short freeze to copy the last operations, atomic switch of the name, clean-up | 1,290 operations moved with a 12.9 ms freeze (148.7 ms in all), 0 errors, 0 lost writes; the direct client fails 100 % after the move |
| Failure | failed replica skipped and marked stale, repaired by set union after it returns, NOT_OWNER triggers a fresh lookup and a retry | node outage of 1.55 s: 0 errors in 1,880 requests; the direct client fails 92.6 % of 3,731; 881 operations repaired, 0 lost |

### 2.1 Account partitions and the consistency model
An account is the sum of an append-only **set of operations**, each with an id. Applying an operation twice changes nothing (idempotent), so a retry is always safe,
and two replicas are equal exactly when they hold the same set. Repair is therefore a **set union**, which cannot lose or duplicate anything.
Partitions are named by prefix (`acct.C000` covers `acct.C00000` to `acct.C00099`); the placement table (prefix -> ordered replica list, with an epoch) lives in the naming service.

Writes use **write-all-available**: a write succeeds when at least one replica applied it, replicas that missed it are marked stale, are not read from, and are repaired
when they return. Reads go to any non-stale replica, round robin. Consequences: after a write is acknowledged every non-stale replica has it (read-your-writes); a read that overlaps an in-flight write may see the old or the new value; the scheme is **not linearizable**.
Durability needs at least one replica disk to survive: an acknowledged write that only one replica holds is lost if that disk is destroyed before repair.

### 2.2 Migration and re-replication (`TransparentGateway.reconfigure`)
1. The new node joins (`OWN_ADD`) and receives the partition's operations while traffic continues (pre-copy).
2. The partition is frozen (new operations wait; in-flight ones drain). The operations that arrived meanwhile are copied.
3. The name record is switched atomically (epoch + 1) in the naming service and the gateway cache is flushed; the partition is unfrozen.
4. The old node drops the partition. A caller holding the old location gets `NOT_OWNER`, looks the name up again and retries.
If the switch fails, the new replica is dropped (roll back). Migration is the special case "replace one node"; growing or shrinking R uses the same steps.
The freeze is the only time operations wait: 1.7 to 19.7 ms in the reference run, growing with the number of operations copied in step 2.

### 2.3 Overhead (what transparency costs)
Measured at saturation with 8 clients (closed loop, no think time), reference run:

| | read mean | write mean | write throughput |
|---|---|---|---|
| direct physical access | 4.08 ms | 5.23 ms | 1,520 /s |
| gateway, R = 1 | 5.76 ms (+41 %) | 6.26 ms (+20 %) | 1,276 /s |
| gateway, R = 2 | 5.43 ms | 9.99 ms | 799 /s |
| gateway, R = 3 | 6.19 ms | 14.93 ms | 536 /s |

The extra hop costs about 1 to 2 ms; each extra replica adds roughly 4 to 5 ms to a write and almost nothing to a read. Name resolution costs 0.277 ms cold and 0.004 ms warm.
Against the 5 to 40 ms service times of Week 8 this is small; against a pure in-memory operation it is a large relative cost. State both in the report.

## 3. Week 10 — naming

`Name -> Name Resolution -> Resource Location`. Four name-server deployments run the same code: **NS-CENTRAL** (every record), **NS-ROOT** (delegations: `acct` -> NS-ACCT, `svc` and `proc` -> NS-SVC),
**NS-ACCT** and **NS-SVC** (zone servers). A record is `{name, kind, locations, epoch, ttl, is_prefix}` and resolution is exact match first, then longest prefix.
`NamingAdmin` writes the same records into every deployment so the strategies are compared on identical data.

| Strategy | Messages per lookup | State kept by the client | Goes stale after a move | Single point of failure |
|---|---|---|---|---|
| static map | 0 | the whole table | yes, forever | no |
| broadcast | N (one per node) | none | never | no |
| central server | 1 | none | never | yes |
| DNS-style (root -> zone, TTL cache) | 2 cold, 1 with the delegation cached, 0 warm | records and delegations with a TTL | until the TTL expires | partly (cache survives) |
| prefix table | 1 to load the table, then 0 | the prefix table | until a NOT_OWNER reply forces a refresh | partly (cache survives) |

Measured (reference run, 2,000 Zipf-distributed lookups over 100 partitions):

| Strategy | mean latency | messages per lookup | cache hit ratio | correct |
|---|---|---|---|---|
| static | 0.0008 ms | 0 | - | 100 % |
| prefix, warm | 0.0010 ms | 0 | 1.00 | 100 % |
| DNS, warm | 0.0139 ms | 0.05 | 0.95 | 100 % |
| central | 0.0969 ms | 1 | - | 100 % |
| DNS, cold | 0.1888 ms | 2 | - | 100 % |
| prefix, cold | 0.4227 ms | 1 (the whole table) | - | 100 % |
| broadcast (4 nodes) | 2.2115 ms | 4 | - | 100 % |

- **Latency follows messages**: about 0.1 ms per message on loopback, so central = 1 message, DNS cold = 2, broadcast = 4 plus the cost of waiting for the slowest answer.
- **Scalability in nodes**: broadcast grows linearly (0.66, 1.29, 1.97, 4.23, 7.35 ms for 4, 8, 16, 32, 64 endpoints, real sockets); central, DNS-warm and prefix-warm do not depend on the node count.
- **Scalability in clients**: one central server saturates at about 10,000 lookups/s whatever the thread count; DNS-warm reaches about 85,000 and prefix-warm about 600,000 (the clients are threads in one Python process, so the absolute caching figures are bounded by the interpreter).
- **Scalability in names**: lookup time is flat (about 0.002 ms for the prefix table at 50,000 names) but the **prefix table must be downloaded**: 8.7 ms for 1,000 names, 74 ms for 10,000, 1,108 ms for 50,000. That is the real price of the prefix-table strategy, and it is why very large name spaces use hierarchies.
- **Staleness after a move**: static never recovers (50 wrong lookups, redirect fails); DNS is wrong for one TTL (2.0 s, 20 wrong lookups); a prefix table is wrong until a node answers NOT_OWNER; central and broadcast are never wrong. With the NOT_OWNER redirect the DNS and prefix resolvers serve the request in 2 attempts (about 1 to 3 ms).
- **Name-server outage** (all four dropping connections): static, broadcast, warm DNS and warm prefix keep resolving (100 %); central, cold DNS and cold prefix resolve 0 %.

## 4. Week 10 — distributed process management

`Create -> Locate -> Schedule -> Coordinate -> Terminate`. A managed **process** is a billing task hosted in a node's runtime (`process_mgmt/node_runtime.py`): it rates a deterministic batch of
call records one by one and consumes the node's emulated service capacity, so tasks and ordinary requests compete for the same node. They are tasks inside the node processes, not operating-system processes:
the name, scheduling, location and life-cycle logic is the distributed part.

| Stage | Implementation |
|---|---|
| Create | name `proc.job-N`, record registered in the naming service (central + `NS-SVC`), table row persisted in SQLite (survives a NODE-2 restart) |
| Schedule | `round_robin`, `random`, `least_loaded` (least estimated remaining work), restricted to nodes the Week 7 detector calls UP; a refused start tries the next node |
| Locate | resolve the process **name**, verify with the node, fall back to a fresh lookup; finished processes are answered from the table |
| Coordinate | groups fan out over the nodes; `wait` is a barrier; `reduce` adds up charge, usage and records; parent/child relations |
| Terminate | cooperative stop on the node, cascade to children, name removed, runtime slot released (reaped); nothing is left behind |
| Recover | a monitor polls every process; if the node is unreachable or has forgotten it, the process is rescheduled elsewhere from its last **checkpoint** (offset + partial totals) |
| Migrate | stop with a checkpoint, switch the name, resume on the new node |

**Exactly-once results.** The batch is deterministic (`expected_batch(seed, start, count)`), so the result can be checked against ground truth. Records after the last checkpoint are re-done and records before it are not counted again, so the total is exact after a crash or a migration.

Measured (reference run):
- Scheduling on 24 skewed tasks: makespan 1.78 s for least_loaded against 2.22 s for round robin and 2.62 s for random; assigned-work imbalance (coefficient of variation) 0.08 against 0.43 and 0.41.
- Coordination: speed-up 1.97, 3.46, 3.45, 3.40 for 2, 4, 8, 16 tasks, limited by the 4 nodes; the barrier adds under 1 ms; creating 16 tasks takes 39 ms.
- Crash: both running tasks on the failed node were rescheduled 0.55 s after the crash; all 2,400 records processed, result exact.
- Migration of a running task at record 255: 10.5 ms from stop to switch (14.6 ms in all); located on the new node by name; result exact.
- Life cycle: 6 of 6 names registered and located, result exact; the cascade terminated 4 processes in 12 ms; 0 names and 0 tasks left behind.

## 5. Known limitations (state them in the report and the defence)

1. NODE-2 hosts the gateway and the process manager: it is a single point of failure (a restart recovers it; the process table and the placement survive, a request in flight is lost).
2. The name servers are single instances. The caches keep resolving through an outage, but nothing can be registered or moved while they are down; a migration needs the naming service.
3. Write-all-available is not linearizable, and a write acknowledged by one replica only is lost if that replica's disk is destroyed before repair (section 2.1).
4. There is no distributed lock. A second gateway would work for reads and writes (operation ids make writes idempotent), but concurrent migrations of the same partition are not supported.
5. Managed processes are tasks in node runtimes, deterministic and idempotent by design; arbitrary non-idempotent programs cannot be restarted from a checkpoint safely.
6. The broadcast scalability test uses a farm of synthetic endpoints on the same machine; the caching throughput figures are limited by one Python process.
7. Everything runs on one machine or one Docker host: no WAN latency, one clock, shared CPU. Absolute numbers depend on the machine; the shapes should not.
8. The Docker Compose files were validated structurally; the container workflow was not executed by the author of this package.
