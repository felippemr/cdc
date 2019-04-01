import json

from cdc.data_types import ChangeData


class Formatter:
    def _process_wal2json_change(self, change):
        change_dictionary = json.loads(change)
        if change_dictionary:
            transaction_id = change_dictionary['xid']

        for change in change_dictionary['change']:
            yield ChangeData(transaction_id=transaction_id, change=change)

    def __call__(self, change):
        yield from self._process_wal2json_change(change)
