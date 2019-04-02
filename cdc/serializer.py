from typing import Generator
from io import BytesIO

import rapidjson

from cdc.data_types import ChangeData

CHUNK_SIZE = 20


class Serializer:
    __slots__ = ['_json_buffer']

    def __init__(self):
        self._json_buffer = BytesIO()

    @staticmethod
    def _process_wal2json_change(change) -> Generator[ChangeData, None, None]:
        payload, lsn = change
        change_dictionary = rapidjson.loads(payload)

        changes = change_dictionary['change']
        timestamp = change_dictionary['timestamp']
        transaction_id = change_dictionary['xid']
        lsn = lsn

        for chunk in (changes[i:i+CHUNK_SIZE] for i in range(0, len(changes), CHUNK_SIZE)):
            yield ChangeData(
                transaction_id=transaction_id,
                change=chunk,
                timestamp=timestamp,
                lsn=lsn,
            )

    def __call__(self, change) -> Generator[ChangeData, None, None]:
        yield from self._process_wal2json_change(change)
