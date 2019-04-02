from cdc.log import logger


class Enqueuer:
    def __init__(self, queue):
        self.queue = queue

    def __call__(self, change):
        self.queue.put(change)
        change.cursor.send_feedback(write_lsn=change.data_start)
        self._log_messages(change)

    @staticmethod
    def _log_messages(change):
        logger.info('Wrote LSN: {}'.format(change.data_start))
        logger.info('WAL end: {}'.format(change.wal_end))
