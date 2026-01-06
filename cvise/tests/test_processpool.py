import concurrent.futures
import multiprocessing
import queue
import time
from typing import Iterable

import pytest

from cvise.utils import sigmonitor
from cvise.utils.processpool import ProcessPool


_WORKERS = 8
_DEFAULT_TIMEOUT = 100


@pytest.fixture
def pool() -> Iterable[ProcessPool]:
    pool = ProcessPool(max_active_workers=_WORKERS)
    pool.__enter__()
    yield pool
    if not pool._shutdown:  # some tests shut down the pool themselves
        pool.__exit__(None, None, None)


def test_two(pool: ProcessPool):
    future1 = pool.schedule(_return_arg, (1,), timeout=_DEFAULT_TIMEOUT)
    future2 = pool.schedule(_return_arg, (2,), timeout=_DEFAULT_TIMEOUT)

    assert future1.result() == 1
    assert future2.result() == 2


def test_many_at_once(pool: ProcessPool):
    N = 1000
    futures = [pool.schedule(_return_arg, (i,), timeout=_DEFAULT_TIMEOUT) for i in range(N)]

    for i in range(N):
        assert futures[i].result() == i


def test_many_as_chunks(pool: ProcessPool):
    CHUNKS = 10
    N = 10
    for _ in range(CHUNKS):
        futures = [pool.schedule(_return_arg, (i,), timeout=_DEFAULT_TIMEOUT) for i in range(N)]
        for i in range(N):
            assert futures[i].result() == i


def test_abort(pool: ProcessPool):
    start_time = time.monotonic()
    future1 = pool.schedule(_sleep, (0.1,), timeout=_DEFAULT_TIMEOUT)
    future2 = pool.schedule(_sleep, (_DEFAULT_TIMEOUT,), timeout=_DEFAULT_TIMEOUT)
    pool.abort(future2)
    assert future1.result() == 0.1
    pool.__exit__(None, None, None)

    assert future2.cancelled()
    assert time.monotonic() - start_time < _DEFAULT_TIMEOUT


def test_run_after_abort(pool: ProcessPool):
    N = 10
    SLEEP = 0.1
    start_time = time.monotonic()
    future_to_abort = pool.schedule(_sleep, (_DEFAULT_TIMEOUT,), timeout=_DEFAULT_TIMEOUT)
    futures_to_wait = [pool.schedule(_sleep, (SLEEP,), timeout=_DEFAULT_TIMEOUT) for i in range(N)]
    pool.abort(future_to_abort)

    for i in range(N):
        assert futures_to_wait[i].result() == SLEEP
    pool.__exit__(None, None, None)
    assert future_to_abort.cancelled()
    assert time.monotonic() - start_time < _DEFAULT_TIMEOUT


def test_heavy_arg(pool: ProcessPool):
    SZ = 1000 * 1000
    arg = '0' * SZ
    future = pool.schedule(_return_arg, (arg,), timeout=_DEFAULT_TIMEOUT)

    assert future.result() == arg


def test_concurrency(pool: ProcessPool):
    N = 1000
    mgr = multiprocessing.Manager()
    event_queue: queue.Queue[int] = mgr.Queue()
    futures = [
        pool.schedule(
            _sleep_and_push_queue_events,
            (
                0.01,
                event_queue,
            ),
            timeout=_DEFAULT_TIMEOUT,
        )
        for i in range(N)
    ]
    concurrent.futures.wait(futures)

    balance = 0
    for _ in range(N * 2):
        event = event_queue.get()
        balance += event
        assert 0 <= balance <= _WORKERS


def _return_arg(arg):
    return arg


def _sleep(duration: float) -> float:
    try:
        # Don't use time.sleep() since it'd ignore signals.
        sigmonitor.get_future().result(timeout=duration)
    except TimeoutError:
        pass
    return duration


def _sleep_and_push_queue_events(duration: float, event_queue: queue.Queue[int]) -> None:
    event_queue.put(+1)
    try:
        # Don't use time.sleep() since it'd ignore signals.
        sigmonitor.get_future().result(timeout=duration)
    except TimeoutError:
        pass
    finally:
        event_queue.put(-1)
