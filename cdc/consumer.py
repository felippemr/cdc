from cdc.log import logger


class Consumer:
    def __init__(self, formatter, writer):
        self.formatter = formatter
        self.writer = writer

    def __call__(self, change):
        for formatted_message in self.formatter(change.payload):
            did_put = self.writer.put_message(formatted_message)

            if did_put:
                change.cursor.send_feedback(flush_lsn=change.data_start)
                self._log_messages(change)

    @staticmethod
    def _log_messages(change):
        logger.info('Flushed LSN: {}'.format(change.data_start))
        logger.info('WAL end: {}'.format(change.wal_end))
