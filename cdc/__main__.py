from typing import Optional

from cdc.consumer import Consumer
from cdc.data_types import ConnectionSettings
from cdc.formatter import Formatter
from cdc.log import logger
from cdc.slot import SlotReader
from cdc.stream import StreamWriter


def main(
    connection_settings: ConnectionSettings,
    stream_name: str,
    recreate_slot: bool,
    reply_from_lsn: Optional[int] = None,
):

    logger.info('Starting CDC!')
    writer = StreamWriter(stream_name)

    with SlotReader(connection_settings) as reader:
        if recreate_slot:
            reader.delete_slot()
            reader.create_slot()

        formatter = Formatter()
        consume = Consumer(formatter, writer)

        # Blocking. Responds to Control-C.
        reader.process_replication_stream(consume, reply_from_lsn)


if __name__ == '__main__':
    conn_settings = ConnectionSettings(
        database='eshares_dev',
        host='localhost',
        port='5432',
        user='rep',
        slot_name='carta_cdc_2',
    )
    main(
        connection_settings=conn_settings,
        stream_name='carta_cdc_1',
        recreate_slot=True,
        reply_from_lsn=None,
    )
