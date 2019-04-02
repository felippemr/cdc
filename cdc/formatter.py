from typing import Generator
from io import BytesIO
import json

import rapidjson

from cdc.data_types import ChangeData


class Formatter:

    def __init__(self):
        self._json_buffer = BytesIO()

    def _process_wal2json_change(self, change) -> Generator[ChangeData, None, None]:
        self._json_buffer.write(change)

        try:
            change_dictionary = rapidjson.loads(self._json_buffer.read())
        except (json.JSONDecodeError, ValueError) as e:
            print(e)
        else:
            self._json_buffer.close()
            for change in change_dictionary['change']:
                yield ChangeData(
                    transaction_id=change_dictionary['xid'],
                    change=change,
                    timestamp=change_dictionary['timestamp'],
                    next_lsn=change_dictionary['nextlsn'],
                )
            self._json_buffer = BytesIO()

    def __call__(self, change) -> Generator[ChangeData, None, None]:
        yield from self._process_wal2json_change(change)
