from dataclasses import dataclass, asdict
from typing import Dict, Any


@dataclass
class UsageRecord:
    request_id: int
    customer_id: str
    service: str
    usage: float
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
