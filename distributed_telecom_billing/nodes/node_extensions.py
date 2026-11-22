"""Weeks 9-10 messages handled by every rating node, kept out of the node files:

    OWN_ADD / OWN_DROP / OWN_LIST        partition ownership (which account prefixes live here)
    ACCT_APPLY / ACCT_GET                the logical account operations
    ACCT_OP_IDS / ACCT_OPS_FETCH / ACCT_OPS_PUT   replica repair and migration copy
    WHO_HAS                              answer a broadcast name lookup
    PROC_START / PROC_STATUS / PROC_STOP / PROC_LIST / PROC_REAP   managed processes
"""
import os

from accounts.account_store import AccountStore
from process_mgmt.node_runtime import ProcessRuntime


class NodeExtensions:
    def __init__(self, node_id, capacity=None):
        self.node_id = node_id
        default = f"data/accounts_{node_id.lower().replace('-', '')}.db"
        self.accounts = AccountStore(os.getenv("ACCOUNT_DB_PATH", default))
        self.runtime = ProcessRuntime(node_id, capacity)

    def handle(self, r):
        t = r.get("type")
        if t is None:
            return None
        a = self.accounts
        if t == "WHO_HAS":
            name = r.get("name", "")
            if name == "svc.rating" or name.startswith("svc."):
                return {"has": True, "epoch": 1}
            if name.startswith("proc."):
                return {"has": self.runtime.has(name), "epoch": 1}
            return a.who_has(name)
        if t == "OWN_ADD":
            return a.own_add(r["prefix"], r.get("epoch", 1))
        if t == "OWN_DROP":
            return a.own_drop(r["prefix"])
        if t == "OWN_LIST":
            return {"owned": a.owned()}
        if t == "ACCT_APPLY":
            return a.apply(r["prefix"], r["customer"], r["op_id"], r.get("charge", 0.0), r.get("usage", 0.0))
        if t == "ACCT_GET":
            return a.get(r["prefix"], r["customer"])
        if t == "ACCT_OP_IDS":
            return {"op_ids": a.op_ids(r["prefix"])}
        if t == "ACCT_OPS_FETCH":
            return {"ops": a.ops_fetch(r["op_ids"])}
        if t == "ACCT_OPS_PUT":
            return a.ops_put(r["ops"])
        if t == "PROC_START":
            return self.runtime.start(r["proc_id"], r["spec"])
        if t == "PROC_STATUS":
            return self.runtime.status(r["proc_id"])
        if t == "PROC_STOP":
            return self.runtime.stop(r["proc_id"])
        if t == "PROC_LIST":
            return self.runtime.list()
        if t == "PROC_REAP":
            return self.runtime.reap(r["proc_id"])
        return None
