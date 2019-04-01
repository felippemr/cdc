from cdc.log import logger


class StreamWriter(object):
    def __init__(self, stream_name, back_off_limit=60, send_window=13):
        self.stream_name = stream_name
        self.back_off_limit = back_off_limit
        self.last_send = 0

        self._sequence_number_for_ordering = '0'
        self._send_window = send_window

    @staticmethod
    def put_message(fmt_msg):
        logger.info(fmt_msg)
        return True
