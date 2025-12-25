"""Collects logs from multiprocessing workers and filters out canceled workers."""

import copy
import io
import logging
import multiprocessing
import multiprocessing.connection
import multiprocessing.reduction
import socket
import struct
from typing import Any


def init_in_worker(logging_level: int, server_conn: multiprocessing.connection.Connection) -> None:
    root = logging.getLogger()
    root.setLevel(logging_level)
    root.handlers.clear()
    root.addHandler(_MPConnSendingHandler(server_conn))


def maybe_handle_message_from_worker(message: Any) -> bool:
    if isinstance(message, logging.LogRecord):
        _emit_log_from_worker(message)
        return True
    return False


def _emit_log_from_worker(record: logging.LogRecord) -> None:
    logger = logging.getLogger(record.name)
    logger.handle(record)


class _MPConnSendingHandler(logging.Handler):
    """Sends all logs into the multiprocessing connection."""

    def __init__(self, server_conn: multiprocessing.connection.Connection):
        super().__init__()
        self._server_conn = server_conn

    def emit(self, record: logging.LogRecord) -> None:
        prepared = self._prepare_record(record)
        buf = io.BytesIO()
        buf.write(b'\0\0\0\0\1')
        multiprocessing.reduction.ForkingPickler(buf).dump(prepared)
        view = buf.getbuffer()
        struct.Struct('i').pack_into(view, 0, len(view) - 5)
        sock = socket.socket(fileno=self._server_conn.fileno(), family=socket.AF_UNIX, type=socket.SOCK_STREAM)
        while view:
            nbytes = sock.send(view)
            view = view[nbytes:]
        sock.detach()

    def _prepare_record(self, record: logging.LogRecord) -> logging.LogRecord:
        """Formats the message, removes unpickleable fields and those not necessary for formatting."""
        formatted = self.format(record)
        record = copy.copy(record)
        record.message = formatted
        record.msg = formatted
        record.args = None
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        return record
