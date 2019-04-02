from cdc.log import logger


class Enqueuer:
    def __init__(self, queue):
        self.queue = queue

    def __call__(self, change):
        self.queue.put((change.payload, change.data_start))
        change.cursor.send_feedback(flush_lsn=change.data_start)
        self._log_messages(change)

    @staticmethod
    def _log_messages(change):
        logger.info('Flushed LSN: {}'.format(change.data_start))
        logger.info('WAL end: {}'.format(change.wal_end))
