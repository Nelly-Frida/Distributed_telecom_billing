from dataclasses import dataclass, asdict
from typing import Dict, Any

<<<<<<< HEAD
=======

>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a
@dataclass
class UsageRecord:
    request_id: int
    customer_id: str
    service: str
    usage: float
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
