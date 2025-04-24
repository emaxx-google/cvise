import datetime
import logging
import os
import tempfile
from contextlib import contextmanager


def is_readable_file(filename):
    try:
        with open(filename) as f:
            f.read()
        return True
    except UnicodeDecodeError:
        return False


# TODO: use tempfile.NamedTemporaryFile(delete_on_close=False) since Python 3.12 is the oldest supported release
@contextmanager
def CloseableTemporaryFile(mode='w+b', dir=None):
    f = tempfile.NamedTemporaryFile(mode=mode, delete=False, dir=dir)
    try:
        yield f
    finally:
        # For Windows systems, be sure we always close the file before we remove it!
        if not f.closed:
            f.close()
        os.remove(f.name)


class DeltaTimeFormatter(logging.Formatter):
    def __init__(self, start_time, fmt):
        super().__init__(fmt)
        self.start_time = start_time

    def format(self, record):  # noqa: A003
        relative_created = record.created - self.start_time
        record.delta = str(datetime.timedelta(seconds=int(relative_created)))
        # pad with one more zero
        if record.delta[1] == ':':
            record.delta = '0' + record.delta
        return super().format(record)
