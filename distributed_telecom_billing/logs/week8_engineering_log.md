# Week 8 Engineering Log — Distributed System Models

> Observed while this version was built and validated (Linux sandbox, 1 vCPU). Keep what is true for you, correct the numbers with
> your own runs and add your own failures. Reference data: `results/reference_run/`.

## What changed
Added an emulated service capacity to the rating nodes (`work_ms`, `NODE_SERVICE_SLOTS`, queueing delay and busy time per node, `NODE_STATS`); a
least-loaded allocation policy in the pool dispatcher; `experiments/week8_system_models.py`, which runs the same workload under six model
variants (host-based, server-based, pool with round robin, pool with least loaded, workstation, integrated) with three kinds of measurement
(saturation sweep, fixed offered load, fault test), a cost model and a weighted decision matrix; Spark tables for latency, scalability, service-class latency and load balance.

## What failed (and why, and the fix)

| # | What failed or surprised | Why | How it was fixed | Alternative considered |
|---|---|---|---|---|
| 1 | In the first fault test every model showed exactly 87.5 % success, including the processor pools that should mask a node failure. | The test failed "client 0" in all models; that was meant only for the workstation model (a user's own machine). 87.5 % = 7/8 clients. | Only the workstation model loses a client; the other models lose NODE-3. Pools now show 100 %. | Keep one fault for all models (hides the fact that the failing component differs per model). |
| 2 | The workstation model looked 100 % utilised and the cheapest per request. | A closed loop with no think time keeps every workstation busy by construction; real users are idle most of the time. | Utilisation, tail latency and cost are measured at a fixed offered load (8 clients x 10 req/s = 80 req/s for every model): workstation utilisation 20.6 %, integrated 13.2 %, host-based overloaded at 97.5 %. Saturation runs are kept for capacity and scalability only. | Add random think times (adds noise; a fixed pace is reproducible). |
| 3 | The workstation model won the weighted ranking under both weight profiles although it cannot provide a shared ledger. | The emulated rating step is stateless, so the numbers cannot see the need for one authoritative store and for the Week 5 transactions. | An explicit pass/fail requirement gate applied after the scores, labelled as a design judgement; the recommended model is chosen among the models that pass. | Hide the model from the comparison (would remove an instructive result). |
| 4 | Availability for the fault test could be understated by an empty bucket after the clients stop. | Same grid error as in Week 7 (log entry 5). | The grid ends at the planned run length. | |

## Findings that are lessons, not bugs
- **Capacity matches queueing theory.** Reference run, 8 clients: host-based 49.4 req/s (bound 51.3), server-based 100.9 (bound 100, set by the voice server), pool with least loaded 208.6 (bound 205), workstation 411.8 (bound 410). Round robin reached 187.2 because it ignores request size.
- **The busiest dedicated server limits a server-based design.** Throughput is `1 / max_i(p_i * S_i)`: voice is 50 % of requests at 20 ms, so about 100 req/s whatever the other servers do.
- **Least loaded versus round robin matters at saturation** (208.6 versus 187.2 req/s at 8 clients) **and not at moderate load** (p95 61.6 versus 62.4 ms at 80 req/s).
- **Reliability differs by model** when one component fails: host-based 0 % served during the fault, server-based 47.5 % (the voice share is lost), workstation 87.5 % (one user of eight), pool and integrated 100 %.
- **Integrated costs most** because it provisions both workstations and the pool; its benefit (light requests answered locally: sms 5.1 ms mean at 80 req/s against about 17 ms in the pools) did not outweigh the cost in this workload.

## Alternatives considered for the experiment itself
CPU-burning instead of sleeping (depends on the test machine's cores, so results would not be reproducible); queueing formulas only (no evidence from running code);
separate machines per node (closest to reality, not available to the group).

## What was learned
A model comparison is only as honest as its load definition and its requirements: saturation hides idle capacity, a stateless emulation hides the need for shared state, and
the ranking changes with the weights. Report the measured capacity against the analytical bound, say which criteria are judgement, and gate on hard requirements.

## Evidence to record
`results/week8_runs.csv`, `week8_reliability.csv`, `week8_decision_matrix.csv`, `week8_experiment_summary.json` (parameters, unit prices, weights), the Spark tables, and the capacity-versus-bound table from `WEEK7_8_DESIGN.md`.
