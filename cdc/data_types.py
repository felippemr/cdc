from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, Any


@dataclass(
    frozen=True,
    init=True,
    repr=True,
    eq=False,
    order=False,
    unsafe_hash=False,
)
class ConnectionSettings:
    database: str
    host: str
    port: str
    user: str
    slot_name: str

    def to_dict(self) -> Dict[str, str]:
        d = deepcopy(self.__dict__)
        del d['slot_name']
        return d


@dataclass(
    frozen=True,
    init=True,
    repr=True,
    eq=False,
    order=False,
    unsafe_hash=False,
)
class ChangeData:
    transaction_id: str
    change: Dict[str, Any]
    next_lsn: str
    timestamp: str
