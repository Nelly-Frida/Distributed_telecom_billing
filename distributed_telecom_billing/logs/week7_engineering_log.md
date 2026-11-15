# Week 7 Engineering Log — Fault Tolerance and Recovery

> The failures below were **observed while this version was built and validated** (Linux sandbox, 1 vCPU, local launcher).
> Keep the ones that also happened to you, correct the numbers with your own runs, and **add the failures you observe on your machine**.
> Reference data: `results/reference_run/`.

## What changed
Added a fault-injection framework (crash, drop, omission, error, delay, Byzantine) controlled by messages on every node's port; a heartbeat failure detector
in NODE-2; a failure-aware dispatcher with failover, per-attempt timeouts and an optional 3-node majority vote; a replicated ledger (LEDGER-1 and LEDGER-2)
with resync and background anti-entropy; automatic re-drive of pending two-phase-commit transactions; rating-node failover for 2PC under a new transaction id;
a supervisor that restarts crashed processes; availability and MTTR measurement; repeated trials; a Spark job that recomputes availability from raw rows.

## What failed (and why, and the fix) — in the order it was found

| # | What failed or surprised | Why | How it was fixed | Alternative considered |
|---|---|---|---|---|
| 1 | After a NODE-2 restart, recovery treated transactions that new clients had already started as undecided and presumed-aborted them. | `recover_pending` selected every transaction without a decision, including ones still being executed by this process. | The coordinator tracks transactions it is executing; recovery skips them. Unit test `test_recovery_never_aborts_a_transaction_still_running_here`. | Wait a fixed grace period after startup (fragile: depends on timing). |
| 2 | Healing a restarted ledger replica took 7.9 s for 727 missed commits. | Resync copied transactions one by one, 3 round trips each. | Parallel healing with 8 workers (about 92 -> 130-150 transactions/s in these runs). The remaining cost is round trips plus a durable write per transaction on the replica. | Ship a snapshot or a bulk log (more code, would hide the per-transaction idempotence). |
| 3 | After a ledger crash the replicas did not converge: two committed transactions stayed missing on the restarted replica. | A transaction that was in flight when the replica came back committed on the other replica only, after the one-shot resync had taken its snapshot. | `ledger_dirty` flag plus a background anti-entropy loop that re-syncs while a replica is lagging and reachable. Both replicas then held the same 1,949 committed transactions. | Make the coordinator wait for both replicas (W = 2): removes the lag but loses availability when one replica is down. |
| 4 | Rating-node failover for 2PC never triggered (found by a unit test). | Failover required status `ABORT`, but a dead participant leaves the transaction `IN_DOUBT` (decision ABORT): the ABORT cannot be delivered to it yet. | Failover keys on the durable decision `ABORT` plus `failure_kind = unreachable`; the first attempt can never commit, so a retry under `<id>-R1` is safe. | Retry with the same id (rejected: the id is bound to the first node in the journal). |
| 5 | A fully masked crash showed 95.65 % availability instead of 100 %. | The availability grid included the empty bucket after the clients stopped, which counted as DOWN. Found because the Spark recomputation disagreed by 2.77 to 4.35 percentage points. | The grid ends at the planned run length in the experiment and in Spark; they now agree to 0.0 points on all 16 scenario/mode pairs. | Ignore empty trailing buckets (hides genuinely idle outages). |
| 6 | The resilient ledger scenario reported an inconsistency after a baseline run. | The baseline writes only to the primary ledger, so LEDGER-2 was 515 transactions behind; that lag leaked into the next scenario. | Controlled starting state: both replicas are brought into sync (and pending 2PC cleared) before each transactional scenario. | Restart the whole stack between scenarios (slow). |
| 7 | The "stuck transactions" metric was 69 and 12 for baseline runs. | It counted what an explicit recovery call resolved, not whether automatic recovery works. | The metric is now `pending_after_auto_recovery`: poll the pending count without calling recovery; it is 0 within 0.5 s in all scenarios. A periodic re-drive was added for transactions that ended IN_DOUBT after a recovery snapshot. | Keep the old metric (measures the wrong thing). |
| 8 | A single run was not representative: `tx_rating_crash` resilient showed 100 % availability in two single runs but 95.45 % in all three runs of a `--repeats 3` batch. | The outcome depends on which requests are in flight at the instant of the crash. | `--repeats N` with mean, min and max (`week7_summary_aggregate.csv`). Quote the mean and range. | Report the best run (not acceptable). |

| 9 | The Week 4 leader-failure test showed `leader_id: None` on two nodes for 20+ seconds and looked like a regression of the election. | The cause was the test setup: processes left over from an aborted earlier test were still running with a deleted working directory, answering on the same ports and failing to create their files (`No such file or directory: 'powerbi'`). The garbled, null-padded log file gave it away: old processes were writing into a truncated file. | The test harness kills leftovers and refuses to start if a port is busy; with a clean start all nodes converge on the new leader in every trial (3 of 3). Lesson: verify the system under test is the one you started (ports, PIDs, log file). | Trust the first result and change the election code (would have "fixed" a bug that did not exist). |
| 10 | While investigating 9, a real weakness was found in the election guard from Week 4. | A follower that still believed in the dead leader 6 refused the lower-id announcement from node 5 ("never downgrade"), so it only converged after its own detection timeout. | A lower-id announcement is now ignored only while the known leader answers a probe; if the leader is dead, the highest surviving node is accepted at once. Four unit tests cover it, and `ELECTION_DEBUG=1` prints the election messages for the defence. | Remove the guard (re-opens the stale-announcement race found in Week 4). |

## Findings that are lessons, not bugs
- **Request-level success hides outages.** Under an omission fault the baseline completed 99.2 % of the requests it managed to issue, but was UP only 45.5 % of the time, because blocked clients stop issuing requests. Availability per time bucket is the honest metric.
- **Crash-oriented failover cannot see a Byzantine node.** With the fault active, baseline and resilient both returned about 9 % wrong charges and looked healthy; only the 3-node majority vote removed them (0 % wrong), at about 3x the per-request latency (2.5 ms against 0.7 to 0.8 ms in the reference run).
- **A restarted node is back within about 1.4 s** (supervisor delay 1.0 s plus process start), shorter than the detector's DOWN threshold, so a plain crash is usually seen as SUSPECT and then RECOVERED (DOWN appeared once in the reference run, for the ledger). Omission, drop and error faults that last 4 s were declared DOWN about 1.6 s after injection.
- **The coordinator is a single point of failure.** A NODE-2 crash is recovered (journal, re-drive, exactly-once replay) but not masked: availability 86 % and MTTR about 1.5 s in the reference run.

## What was learned
Failure detection and failover hide a failure only when the failed component is stateless or replicated; stateful components need replication plus a repair path (resync, anti-entropy, journal recovery) and the repair path needs its own tests.
Define availability and MTTR before measuring, measure them in time buckets, repeat the run, and cross-check the measurement with an independent computation (here Spark).

## Evidence to record
`results/week7_summary_aggregate.csv`, `week7_timeline.csv` (availability over time), `week7_detector_events.csv`, `week7_supervisor_events.csv`, `week7_experiment_summary.json`
(consistency reports and recovery evidence), the output of `week7_live_demo.py` for both modes, and the Spark cross-check table.
