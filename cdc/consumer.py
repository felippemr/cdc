from queue import Queue
from threading import Thread
from typing import ClassVar

from cdc.log import logger
from cdc.serializer import Serializer
from cdc.stream import StreamWriter


class Consumer:
    def __init__(
        self,
        stream_name: str,
        job_queue: Queue,
        serializer_class: ClassVar[Serializer],
        writer_class: ClassVar[StreamWriter],
        threading_class: ClassVar[Thread] = Thread
    ):
        self._stream_name = stream_name
        self._job_queue = job_queue
        self._serializer = serializer_class
        self._writer = writer_class
        self._threading_class = threading_class

    @classmethod
    def bootstrap(
        cls,
        stream_name: str,
        job_queue: Queue,
        serializer_class: ClassVar[Serializer] = Serializer,
        writer_class: ClassVar[StreamWriter] = StreamWriter,
        threading_class: ClassVar[Thread] = Thread
    ):
        return cls(
            stream_name, job_queue, serializer_class, writer_class, threading_class
        )

    def run(self):
        thread = self._threading_class(target=self._work, daemon=True)
        thread.start()

    def _work(self):
        while True:
            serializer = self._serializer()
            writer = self._writer(self._stream_name)

            change_data = self._job_queue.get()

            self._consume_changes(change_data, serializer, writer)
            self._flush_lsn(change_data)

    @staticmethod
    def _consume_changes(item, serializer, writer):
        for change in serializer(item):
            writer.put_message(change)

    @staticmethod
    def _flush_lsn(item):
        logger.info(f'Flushed LSN: {item.data_start}')
        item.cursor.send_feedback(apply_lsn=item.data_start)

