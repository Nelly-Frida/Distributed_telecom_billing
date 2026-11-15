"""Unit tests for Week 7 (fault tolerance) and Week 8 (system models).

Run from the project root:   python -m unittest discover -s tests -v
No network or running system is needed: remote calls are replaced by in-process fakes.
"""
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from common.emulated_work import ServiceCapacity
from coordination.bully_election import BullyElection
from common.fault_state import FaultState
from experiments.ft_metrics import availability, bucketize, mttr
from fault_tolerance import failure_detector as fd
from fault_tolerance.resilient_dispatcher import ResilientDispatcher, majority_charge
from transactions import two_phase_commit as tpc
from transactions.state_store import TransactionStateStore


class FaultStateTests(unittest.TestCase):
    def test_modes_map_to_actions_and_expire(self):
        f = FaultState("N")
        self.assertIsNone(f.intercept({}))
        f.control({"type": "FAULT_INJECT", "mode": "drop", "duration_s": 0.2})
        self.assertEqual(f.intercept({}), ("drop",))
        f.control({"type": "FAULT_INJECT", "mode": "omission", "hold_s": 3})
        self.assertEqual(f.intercept({}), ("omit", 3.0))
        f.control({"type": "FAULT_INJECT", "mode": "error", "message": "boom"})
        self.assertEqual(f.intercept({}), ("error", "boom"))
        f.control({"type": "FAULT_INJECT", "mode": "drop", "duration_s": 0.1})
        time.sleep(0.2)
        self.assertIsNone(f.intercept({}), "a timed fault must expire by itself")

    def test_control_messages_bypass_faults_and_clear_works(self):
        f = FaultState("N")
        f.control({"type": "FAULT_INJECT", "mode": "drop"})
        self.assertTrue(f.is_control({"type": "FAULT_CLEAR"}))
        self.assertTrue(f.control({"type": "FAULT_CLEAR"})["ok"])
        self.assertIsNone(f.intercept({}))

    def test_unknown_mode_is_rejected(self):
        self.assertFalse(FaultState("N").control({"type": "FAULT_INJECT", "mode": "gremlins"})["ok"])

    def test_byzantine_corrupts_charge_silently(self):
        f = FaultState("N")
        f.control({"type": "FAULT_INJECT", "mode": "byzantine", "factor": 2.0})
        reply = f.corrupt({"charge": 10.0, "node": "N"})
        self.assertEqual(reply["charge"], 20.0)
        self.assertNotIn("error", reply)


class FailureDetectorTests(unittest.TestCase):
    def test_suspect_then_down_then_recovered_with_callback(self):
        recovered = []
        det = fd.FailureDetector({"A": ("h", 1), "B": ("h", 2)}, miss_threshold=3,
                                 on_recovered=lambda n: recovered.append(n))
        det._apply_probe("A", False, "x")
        self.assertEqual(det.status("A"), fd.SUSPECT)
        det._apply_probe("A", False, "x")
        det._apply_probe("A", False, "x")
        self.assertEqual(det.status("A"), fd.DOWN)
        self.assertEqual(det.up_nodes(["A", "B"]), ["B", "A"], "healthy nodes are preferred")
        det._apply_probe("A", True)
        time.sleep(0.05)
        self.assertEqual(det.status("A"), fd.UP)
        self.assertEqual(recovered, ["A"])
        self.assertEqual([e["event"] for e in det.snapshot()["events"]], ["SUSPECTED", "DOWN", "RECOVERED"])

    def test_passive_report_marks_suspect_and_probe_restores(self):
        det = fd.FailureDetector({"A": ("h", 1)})
        det.report_failure("A", "request failed")
        self.assertEqual(det.status("A"), fd.SUSPECT)
        det._apply_probe("A", True)
        self.assertEqual(det.status("A"), fd.UP)


class ResilientDispatcherTests(unittest.TestCase):
    def make(self, behaviour, policy="round_robin"):
        det = fd.FailureDetector({n: ("h", i) for i, n in enumerate(["NODE-3", "NODE-4", "NODE-5", "NODE-6"])})
        disp = ResilientDispatcher(det, attempt_timeout=0.1, max_attempts=3, policy=policy)

        def call(name, request, timeout):
            return behaviour(name, request)
        disp._call = call
        return det, disp

    def test_failover_masks_a_dead_node_and_marks_it_suspect(self):
        def behaviour(name, req):
            if name == "NODE-3":
                raise ConnectionRefusedError("down")
            return {"charge": 5.0, "node": name}
        det, disp = self.make(behaviour)
        results = [disp.process_request({"request_id": i}) for i in range(8)]
        self.assertTrue(all("error" not in r for r in results))
        self.assertEqual(sum(r["failover_attempts"] for r in results), 1, "only the first hit needs a retry")
        self.assertEqual(det.status("NODE-3"), fd.SUSPECT)
        self.assertNotIn("NODE-3", {r["selected_node"] for r in results})

    def test_all_nodes_failing_returns_an_error(self):
        det, disp = self.make(lambda n, r: (_ for _ in ()).throw(TimeoutError("t")))
        self.assertIn("error", disp.process_request({"request_id": 1}))

    def test_least_loaded_prefers_idle_node(self):
        det, disp = self.make(lambda n, r: {"charge": 1.0, "node": n}, policy="least_loaded")
        disp.inflight.update({"NODE-3": 5, "NODE-4": 0, "NODE-5": 3, "NODE-6": 2})
        self.assertEqual(disp.order()[0], "NODE-4")

    def test_majority_vote_catches_a_byzantine_node(self):
        def behaviour(name, req):
            return {"charge": 12.5 if name == "NODE-4" else 10.0, "node": name}
        det, disp = self.make(behaviour)
        chosen = disp.order()[:3]                       # the nodes the vote will ask
        reply = disp.process_verified({"request_id": 1}, replicas=3)
        self.assertEqual(reply["charge"], 10.0)
        self.assertTrue(reply["verified"])
        self.assertEqual(len(reply["byzantine_suspects"]), 1 if "NODE-4" in chosen else 0)
        if "NODE-4" in chosen:
            self.assertEqual(det.status("NODE-4"), fd.SUSPECT, "the deviant node is demoted")

    def test_majority_charge_edge_cases(self):
        self.assertEqual(majority_charge([10.0, 10.0, 12.5]), (10.0, [0, 1]))
        self.assertEqual(majority_charge([1.0, 2.0, 3.0]), (None, []))
        self.assertEqual(majority_charge([]), (None, []))


class MetricsTests(unittest.TestCase):
    def rows(self, bad_from, bad_to, total=10.0):
        out, t = [], 0.0
        while t < total:
            ok = not (bad_from <= t < bad_to)
            out.append({"t_end_s": t, "ok": ok, "correct": ok})
            t += 0.05
        return out

    def test_availability_and_mttr_for_a_two_second_outage(self):
        buckets = bucketize(self.rows(3.0, 5.0), 10.0)
        self.assertAlmostEqual(availability(buckets), 0.8)
        self.assertEqual(mttr(buckets, 3.0), {"masked": False, "mttr_s": 2.0, "outage_s": 2.0})

    def test_masked_failure_has_zero_mttr_and_full_availability(self):
        buckets = bucketize(self.rows(99, 99), 10.0)
        self.assertEqual(availability(buckets), 1.0)
        self.assertTrue(mttr(buckets, 3.0)["masked"])

    def test_wrong_results_count_as_downtime(self):
        rows = [{"t_end_s": i * 0.05, "ok": True, "correct": False} for i in range(200)]
        self.assertEqual(availability(bucketize(rows, 10.0)), 0.0)


class ServiceCapacityTests(unittest.TestCase):
    def test_single_slot_serialises_requests_and_reports_queueing(self):
        cap = ServiceCapacity(slots=1)
        waits = []
        threads = [threading.Thread(target=lambda: waits.append(cap.execute(40))) for _ in range(3)]
        started = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertGreaterEqual(time.perf_counter() - started, 0.115)   # 3 x 40 ms, not 40 ms
        self.assertGreater(max(waits), 70.0)
        snap = cap.snapshot()
        self.assertEqual(snap["completed"], 3)
        self.assertGreater(snap["utilization"], 0.7)

    def test_two_slots_run_in_parallel(self):
        cap = ServiceCapacity(slots=2)
        started = time.perf_counter()
        threads = [threading.Thread(target=cap.execute, args=(60,)) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertLess(time.perf_counter() - started, 0.11)


class FakeCluster:
    """In-process stand-in for the rating node and two ledger replicas."""
    RATING = {("127.0.0.1", p) for p in (5003, 5004, 5005, 5006)}

    def __init__(self, tmp):
        self.ledger = {("127.0.0.1", 5007): TransactionStateStore(str(Path(tmp) / "l1.db")),
                       ("127.0.0.1", 5008): TransactionStateStore(str(Path(tmp) / "l2.db"))}
        self.down = set()
        self.rating_state = {}

    def send_json(self, host, port, msg, timeout=0):
        addr = (host, port)
        if addr in self.down:
            raise ConnectionRefusedError(f"{addr} down")
        kind, tx = msg["type"], msg.get("transaction_id")
        if addr in self.RATING:
            rates = {"voice": 0.5, "sms": 1.0, "data": 2.0}
            store = self.rating_state.setdefault(addr, {})
            if kind == "TX_PREPARE":
                store[tx] = {"state": "PREPARED", "charge": rates[msg["service"]] * msg["usage"]}
                return {"prepared": True, "charge": store[tx]["charge"]}
            if kind == "TX_COMMIT":
                store[tx]["state"] = "COMMITTED"
                return {"committed": True}
            if kind == "TX_ABORT":
                store.pop(tx, None)
                return {"aborted": True}
            if kind == "TX_STATUS":
                return {"state": store.get(tx, {}).get("state", "UNKNOWN"), "payload": store.get(tx)}
        led = self.ledger[addr]
        if kind == "TX_PREPARE":
            return led.prepare(tx, {k: msg[k] for k in ("customer_id", "charge", "service", "usage")})
        if kind == "TX_COMMIT":
            return led.commit(tx)
        if kind == "TX_ABORT":
            return led.abort(tx)
        if kind == "TX_STATUS":
            return led.status(tx)
        if kind == "LEDGER_LIST":
            return led.list_ids()
        raise AssertionError(kind)


class ReplicatedTransactionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cluster = FakeCluster(self.tmp.name)
        self.patches = [mock.patch.object(tpc, "send_json", self.cluster.send_json)]
        for p in self.patches:
            p.start()
        self.coord = tpc.TwoPhaseCommitCoordinator(csv_path=str(Path(self.tmp.name) / "tx.csv"),
                                                   journal_path=str(Path(self.tmp.name) / "j.db"))

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def req(self, txid):
        return {"transaction_id": txid, "request_id": 1, "customer_id": "C1", "service": "voice", "usage": 10}

    def test_commit_survives_one_ledger_replica_down_and_resync_heals_it(self):
        self.cluster.down.add(("127.0.0.1", 5008))
        result = self.coord.execute(self.req("T1"), rating_node="NODE-3", replicate=True)
        self.assertEqual(result["status"], "COMMIT")
        self.assertTrue(self.coord.ledger_dirty, "a missed write must flag the replica as lagging")
        self.assertFalse(self.coord.consistency_report()["consistent"])
        self.cluster.down.clear()
        healed = self.coord.resync_ledger()
        self.assertEqual(healed["healed"], 1)
        self.assertTrue(self.coord.consistency_report()["consistent"])
        self.assertFalse(self.coord.ledger_dirty)

    def test_baseline_mode_uses_only_the_primary_ledger(self):
        result = self.coord.execute(self.req("T2"), rating_node="NODE-3", replicate=False)
        self.assertEqual(result["status"], "COMMIT")
        self.assertEqual(self.cluster.ledger[("127.0.0.1", 5008)].list_ids()["committed"], [])

    def test_both_ledgers_down_never_commits(self):
        self.cluster.down.update({("127.0.0.1", 5007), ("127.0.0.1", 5008)})
        result = self.coord.execute(self.req("T3"), rating_node="NODE-3", replicate=True)
        self.assertNotEqual(result["status"], "COMMIT")

    def test_rating_node_failure_triggers_failover_under_a_new_transaction_id(self):
        self.cluster.down.add(("127.0.0.1", 5003))
        det = fd.FailureDetector({n: ("h", i) for i, n in enumerate(["NODE-3", "NODE-4", "NODE-5", "NODE-6"])})
        disp = ResilientDispatcher(det)
        result = self.coord.execute_resilient(self.req("T4"), disp, rating_node="NODE-3")
        self.assertEqual(result["status"], "COMMIT")
        self.assertEqual(result["failover_from"], "NODE-3")
        self.assertEqual(result["transaction_id"], "T4-R1")
        first = self.coord.status("T4")
        self.assertEqual(first["decision"], "ABORT", "the first attempt must never commit")
        self.assertEqual(first["status"], "IN_DOUBT", "abort could not reach the dead node yet")
        self.cluster.down.clear()                       # node comes back: recovery re-drives the abort
        self.coord.recover_pending()
        self.assertEqual(self.coord.status("T4")["status"], "ABORT")

    def test_recovery_never_aborts_a_transaction_still_running_here(self):
        self.coord.journal.create("LIVE", {"customer_id": "C1"}, "NODE-3")
        self.coord.journal.create("STALE", {"customer_id": "C1"}, "NODE-3")
        with self.coord._active_lock:
            self.coord._active.add("LIVE")
        recovered = {r["transaction_id"] for r in self.coord.recover_pending()["recovered"]}
        self.assertEqual(recovered, {"STALE"})
        self.assertEqual(self.coord.status("LIVE")["status"], "PREPARING")


class ElectionGuardTests(unittest.TestCase):
    """A lower-id COORDINATOR announcement must not override a LIVE higher-id leader,
    but must be accepted when that leader is dead (the highest surviving node wins)."""

    def make(self, my_id=3):
        return BullyElection("N", my_id, [], ("h", my_id), on_leader_change=lambda i, a: None, all_peers=[])

    def test_lower_id_announcement_ignored_while_known_leader_is_alive(self):
        e = self.make()
        e.receive_coordinator(6, ("h", 6))
        e._leader_alive = lambda: True
        e.receive_coordinator(5, ("h", 5))
        self.assertEqual(e.leader_id, 6)

    def test_lower_id_announcement_accepted_when_known_leader_is_dead(self):
        e = self.make()
        e.receive_coordinator(6, ("h", 6))
        e._leader_alive = lambda: False
        e.receive_coordinator(5, ("h", 5))
        self.assertEqual(e.leader_id, 5)

    def test_higher_id_announcement_is_always_accepted(self):
        e = self.make()
        e.receive_coordinator(4, ("h", 4))
        e._leader_alive = lambda: True
        e.receive_coordinator(6, ("h", 6))
        self.assertEqual(e.leader_id, 6)

    def test_node_that_is_leader_itself_ignores_lower_announcements(self):
        e = self.make(my_id=6)
        e.leader_id, e.leader_addr = 6, ("h", 6)
        e.receive_coordinator(5, ("h", 5))
        self.assertEqual(e.leader_id, 6)


if __name__ == "__main__":
    unittest.main()
