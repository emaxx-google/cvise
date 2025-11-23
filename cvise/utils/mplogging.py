"""Collects logs from multiprocessing workers and filters out canceled workers."""

import copy
import logging
import multiprocessing
import multiprocessing.connection
from typing import Any

from cvise.utils import sigmonitor


def init_in_worker(logging_level: int, server_conn: multiprocessing.connection.Connection) -> None:
    root = logging.getLogger()
    root.setLevel(logging_level)
    root.handlers.clear()
    root.addHandler(_MPConnSendingHandler(server_conn))


def maybe_handle_message_from_worker(message: Any, is_active_worker: bool) -> bool:
    if isinstance(message, logging.LogRecord):
        if is_active_worker:
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
        with sigmonitor.scoped_mode(sigmonitor.Mode.RAISE_EXCEPTION_ON_DEMAND):
            self._server_conn.send(prepared)

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
