"""Two ways for an application to use the account service (Week 9).

TransparentClient   uses LOGICAL names through the gateway. It knows one address (the gateway)
                    and nothing about nodes, replicas, moves or failures.
DirectClient        the non-transparent baseline: the application holds the physical placement,
                    talks to the node itself, and has to cope with failures and moves on its own
                    (here it does not, which is exactly what the comparison measures).
"""
import uuid

from common.communication import send_json
from common.config import NODE2_PORT, NODE_HOST, RATING_NODES
from naming.resolvers import partition_of


class TransparentClient:
    def __init__(self, host=None, port=None, timeout=15.0):
        self.addr = (host or NODE_HOST, port or NODE2_PORT)
        self.timeout = timeout

    def _call(self, msg):
        return send_json(self.addr[0], self.addr[1], msg, timeout=self.timeout)

    def apply(self, customer, charge, usage=0.0, op_id=None):
        return self._call({"type": "LOGICAL", "name": f"acct.{customer}", "op": "apply",
                           "args": {"charge": charge, "usage": usage, "op_id": op_id or uuid.uuid4().hex}})

    def get(self, customer):
        return self._call({"type": "LOGICAL", "name": f"acct.{customer}", "op": "get"})


class DirectClient:
    """Placement is a snapshot taken at start-up: {prefix: [node, ...]}. Writes go to the first node only."""

    def __init__(self, placement, timeout=3.0):
        self.placement = {p: list(v) for p, v in placement.items()}
        self.timeout = timeout

    def _node(self, name):
        prefix = partition_of(name)
        node = self.placement[prefix][0]
        return prefix, RATING_NODES[node]

    def apply(self, customer, charge, usage=0.0, op_id=None):
        name = f"acct.{customer}"
        prefix, (host, port) = self._node(name)
        return send_json(host, port, {"type": "ACCT_APPLY", "prefix": prefix, "customer": customer,
                                      "op_id": op_id or uuid.uuid4().hex, "charge": charge, "usage": usage},
                         timeout=self.timeout)

    def get(self, customer):
        name = f"acct.{customer}"
        prefix, (host, port) = self._node(name)
        return send_json(host, port, {"type": "ACCT_GET", "prefix": prefix, "customer": customer}, timeout=self.timeout)
