"""Collects logs from multiprocessing workers and filters out canceled workers."""

import copy
import logging
import multiprocessing
import multiprocessing.connection
from collections.abc import Callable
from dataclasses import dataclass

from cvise.utils import sigmonitor


class MPLogger:
    """Collects and processes logs from multiprocessing workers in the main process.

    Main features:
    * Discards logs from canceled jobs (to hide spurious errors from the user).
    * Supports logging to file (otherwise workers would try to write to the same file simultaneously).

    Each worker should use worker_process_initializer().
    """

    def worker_process_initializer(self) -> Callable:
        return _WorkerProcessInitializer(logging_level=logging.getLogger().getEffectiveLevel())

    def on_log_from_worker(self, record: logging.LogRecord) -> None:
        logger = logging.getLogger(record.name)
        logger.handle(record)


@dataclass
class _WorkerProcessInitializer:
    """The function-like object returned by worker_process_initializer()."""

    logging_level: int

    def __call__(self, server_conn: multiprocessing.connection.Connection):
        root = logging.getLogger()
        root.setLevel(self.logging_level)
        root.handlers.clear()
        root.addHandler(_MPConnSendingHandler(server_conn))


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
