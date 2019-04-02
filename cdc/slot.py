from typing import Optional, Dict

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import psycopg2.errorcodes

from cdc.data_types import ConnectionSettings
from cdc.log import logger

psycopg2.extensions.register_type(
    psycopg2.extensions.UNICODE, None
)  # Typecasters for basic types.
psycopg2.extensions.register_type(
    psycopg2.extensions.UNICODEARRAY, None
)  # Typecasters to convert arrays of sql types into Python lists.


class SlotReader:

    def __init__(self, connection_settings: ConnectionSettings,):
        self._connection_settings = connection_settings

        self._repl_conn = None
        self._repl_cursor = None

        self.output_plugin = 'wal2json'
        self.cur_lag = 0

    def __enter__(self):
        self._repl_conn = self._get_connection(connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self._repl_cursor = self._repl_conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for closable in [self._repl_cursor, self._repl_conn]:
            try:
                closable.close()
            except Exception:
                pass

    def _get_connection(self, connection_factory=None, cursor_factory=None):
        return psycopg2.connect(
            connection_factory=connection_factory,
            cursor_factory=cursor_factory,
            **self._connection_settings.to_dict()
        )

    def create_slot(self):
        slot_name = self._connection_settings.slot_name
        logger.info(f'Creating slot {slot_name}')
        try:
            self._repl_cursor.create_replication_slot(
                slot_name,
                slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                output_plugin=self.output_plugin,
            )
        except psycopg2.ProgrammingError as p:
            # Will be raised if slot exists already.
            if p.pgcode != psycopg2.errorcodes.DUPLICATE_OBJECT:
                logger.error(p)
                raise
            else:
                logger.info(f'Slot {slot_name} is already present.')

    def delete_slot(self):
        slot_name = self._connection_settings.slot_name
        logger.info(f'Deleting slot {slot_name}')
        try:
            self._repl_cursor.drop_replication_slot(slot_name)
        except psycopg2.ProgrammingError as p:
            # Will be raised if slot does not exist.
            if p.pgcode != psycopg2.errorcodes.UNDEFINED_OBJECT:
                logger.error(p)
                raise
            else:
                logger.info(f'Slot {slot_name} was not found.')

    def process_replication_stream(self, consumer, reply_from_lsn: Optional[int] = None):
        slot_name = self._connection_settings.slot_name

        self._log_greetings(slot_name, reply_from_lsn)
        self._repl_cursor.start_replication(slot_name, options=self._replication_options)

        if reply_from_lsn:
            self._repl_cursor.send_feedback(apply_lsn=reply_from_lsn)

        self._repl_cursor.consume_stream(consumer)

    @staticmethod
    def _log_greetings(slot_name, reply_from_lsn):
        msg = f'Starting the consumption of slot "{slot_name}"'
        if reply_from_lsn:
            msg += f" from LSN {reply_from_lsn}"

        logger.info(msg)

    @property
    def _replication_options(self) -> Dict[str, int]:
        options = None
        if self.output_plugin == 'wal2json':
            options = {
                'include-xids': 1,
                'include-lsn': 1,
                'include-timestamp': 1,
                'write-in-chunks': 1,
                'include-schemas': 0,
                'include-types': 0,
                'include-typmod': 0,
            }

        return options
