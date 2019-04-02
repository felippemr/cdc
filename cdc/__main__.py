from typing import Optional
from multiprocessing import Queue

from cdc.consumer import Consumer
from cdc.data_types import ConnectionSettings
from cdc.enqueuer import Enqueuer
from cdc.log import logger
from cdc.slot import SlotReader


def _main(
    connection_settings: ConnectionSettings,
    stream_name: str,
    recreate_slot: bool,
    reply_from_lsn: Optional[int] = None,
):
    job_queue = Queue()
    logger.info('Starting CDC!')

    consumer = Consumer.bootstrap(stream_name, job_queue)
    consumer.run()

    with SlotReader(connection_settings) as reader:
        if recreate_slot:
            reader.delete_slot()
            reader.create_slot()

        enqueuer = Enqueuer(job_queue)
        reader.process_replication_stream(enqueuer, reply_from_lsn)


if __name__ == '__main__':
    conn_settings = ConnectionSettings(
        database='eshares_dev',
        host='localhost',
        port='5432',
        user='rep',
        slot_name='carta_cdc_1',
    )
    _main(
        connection_settings=conn_settings,
        stream_name='carta_cdc_1',
        recreate_slot=False,
        reply_from_lsn=None,
    )
