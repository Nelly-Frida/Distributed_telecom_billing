import tempfile
import unittest
from pathlib import Path
from transactions.state_store import TransactionStateStore
from concurrency.resource_manager import ResourceManager


class TransactionStateStoreTests(unittest.TestCase):
    def test_prepare_commit_are_idempotent_and_durable(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=str(Path(tmp)/"participant.db")
            store=TransactionStateStore(path)
            payload={"charge":20.0,"customer_id":"C001"}
            self.assertTrue(store.prepare("TX-1",payload)["prepared"])
            self.assertTrue(store.prepare("TX-1",payload)["prepared"])
            self.assertTrue(store.commit("TX-1")["committed"])
            self.assertTrue(store.commit("TX-1")["committed"])
            reopened=TransactionStateStore(path)
            self.assertEqual(reopened.status("TX-1")["state"],"COMMITTED")
            self.assertFalse(reopened.abort("TX-1")["aborted"])

    def test_rejects_transaction_id_reuse_with_different_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=TransactionStateStore(str(Path(tmp)/"participant.db"))
            store.prepare("TX-2",{"charge":10})
            with self.assertRaises(ValueError):
                store.prepare("TX-2",{"charge":11})


class ResourceManagerTests(unittest.TestCase):
    def test_ordered_lock_acquisition_releases_all_resources(self):
        rm=ResourceManager()
        self.assertTrue(rm.acquire_ordered("TX-A",["ACCOUNT-B","ACCOUNT-A"]))
        self.assertEqual(rm.stats()["locked_resources"],2)
        rm.finish("TX-A")
        self.assertEqual(rm.stats()["locked_resources"],0)

    def test_wait_for_graph_cycle_detection(self):
        self.assertTrue(ResourceManager.has_cycle({"TX-A":{"TX-B"},"TX-B":{"TX-A"}}))
        self.assertFalse(ResourceManager.has_cycle({"TX-A":{"TX-B"},"TX-B":set()}))


if __name__=="__main__":unittest.main()
