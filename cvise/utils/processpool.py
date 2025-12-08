from __future__ import annotations

import collections
import concurrent.futures
import contextlib
import heapq
import logging
import multiprocessing
import multiprocessing.connection
import multiprocessing.reduction
import os
import select
import selectors
import socket
import sys
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

from cvise.utils import mplogging, sigmonitor


@dataclass(slots=True)
class _Task:
    f: Callable
    args: Sequence[Any]
    future: Future
    timeout: float


@dataclass(slots=True)
class _ScheduledAbort:
    worker_pid: int
    task_future: Future


@dataclass(slots=True)
class _SharedState:
    event_read_socket: socket.socket
    event_write_socket: socket.socket
    lock: threading.Lock
    pre_shutdown: bool
    shutdown: bool
    task_queue: collections.deque[_Task]
    scheduled_aborts: list[_ScheduledAbort]

    def close(self) -> None:
        self.event_read_socket.close()
        self.event_write_socket.close()

    def set_pre_shutdown(self) -> None:
        with self.lock:
            self.pre_shutdown = True
        self._notify()

    def set_shutdown(self) -> None:
        with self.lock:
            assert self.pre_shutdown
            self.shutdown = True
        self._notify()

    def enqueue_task(self, task: _Task) -> bool:
        with self.lock:
            if self.pre_shutdown:
                return False
            self.task_queue.append(task)
            should_notify = self._should_notify()
        if should_notify:
            self._notify()
        return True

    def enqueue_task_abort(self, abort: _ScheduledAbort) -> None:
        with self.lock:
            self.scheduled_aborts.append(abort)
            should_notify = self._should_notify()
        if should_notify:
            self._notify()

    def _should_notify(self) -> bool:
        return len(self.task_queue) + len(self.scheduled_aborts) == 1

    def _notify(self) -> None:
        self.event_write_socket.send(b'\0')


@dataclass(slots=True)
class _PickledTask:
    data: memoryview
    future: Future
    timeout: float


@dataclass(slots=True)
class _WorkerInfo:
    process: multiprocessing.Process | None
    connection: multiprocessing.connection.Connection | None
    active_task_future: Future | None
    stopping: bool = False


class ProcessPoolError(Exception):
    pass


class ProcessPool:
    def __init__(self, max_worker_count):
        self._max_worker_count = max_worker_count

        event_read_socket, event_write_socket = socket.socketpair()
        event_read_socket.setblocking(False)
        event_write_socket.setblocking(False)

        self._shared_state = _SharedState(
            event_read_socket=event_read_socket,
            event_write_socket=event_write_socket,
            lock=threading.Lock(),
            pre_shutdown=False,
            shutdown=False,
            task_queue=collections.deque(),
            scheduled_aborts=[],
        )
        self._pool_thread = threading.Thread(target=self._pool_thread_main)

    def __enter__(self):
        self._pool_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

        # Terminate all workers in a blocking fashion.
        self._shared_state.set_shutdown()
        self._pool_thread.join(60)
        if self._pool_thread.is_alive():
            logging.warning('Failed to stop process pool thread')
            assert 0
        self._shared_state.close()

    def stop(self):
        """Initiates the termination of already running tasks; pending tasks are canceled."""
        self._shared_state.set_pre_shutdown()

    def schedule(self, f: Callable, args: Sequence[Any], timeout: float):
        future = Future()
        task = _Task(f=f, args=args, future=future, timeout=timeout)
        if not self._shared_state.enqueue_task(task):
            # we're in (pre)shutdown
            future.cancel()
        return future

    def _pool_thread_main(self):
        try:
            self._pool_thread_main2()
        except:
            logging.exception('_pool_thread_main')

    def _pool_thread_main2(self):
        workers: dict[int, _WorkerInfo] = {}
        pickled_task_queue: collections.deque[_PickledTask] = collections.deque()
        free_worker_pids: collections.deque[int] = collections.deque()
        stopping_workers: int = 0
        timeouts_heap: list[tuple[float, Future]] = []
        event_selector = selectors.DefaultSelector()
        event_selector.register(self._shared_state.event_read_socket, selectors.EVENT_READ)
        while True:
            now = time.monotonic()
            while timeouts_heap:
                timeout, future = timeouts_heap[0]
                if timeout > now and not future.done():
                    break
                heapq.heappop(timeouts_heap)
                if not future.done():
                    with contextlib.suppress(concurrent.futures.InvalidStateError):
                        future.set_exception(TimeoutError('Job timed out'))

            worker: _WorkerInfo | None = None
            workers_to_start: int = 0
            workers_to_stop: list[_WorkerInfo] = []
            tasks_to_send: collections.deque[_Task | _PickledTask] = collections.deque()
            has_tasks_to_serialize = False
            with self._shared_state.lock:
                assert len(free_worker_pids) <= len(workers)
                assert 0 <= len(workers) - stopping_workers <= self._max_worker_count
                assert 0 <= len(workers) - len(free_worker_pids) <= self._max_worker_count

                if self._shared_state.pre_shutdown:
                    break

                for abort in self._shared_state.scheduled_aborts:
                    if abort.worker_pid not in workers:
                        continue  # already died
                    worker = workers[abort.worker_pid]
                    if worker.active_task_future != abort.task_future:
                        continue  # a new task started already
                    workers_to_stop.append(worker)
                self._shared_state.scheduled_aborts = []

                workers_to_start = self._max_worker_count - len(workers) + stopping_workers
                assert 0 <= workers_to_start <= self._max_worker_count

                available_workers = min(
                    len(free_worker_pids) + workers_to_start,
                    self._max_worker_count - (len(workers) - len(free_worker_pids)),
                )
                for queue in (pickled_task_queue, self._shared_state.task_queue):
                    while available_workers and queue:
                        task = queue.popleft()
                        if task.future.done():
                            continue
                        tasks_to_send.append(task)
                        available_workers -= 1

                has_tasks_to_serialize = len(self._shared_state.task_queue) > 0

            for worker in workers_to_stop:
                assert worker
                assert worker.process is not None
                assert worker.process.pid is not None
                worker.stopping = True
                # print(f'cancel: sigterm to pid={worker.process.pid}', file=sys.stderr)
                worker.process.terminate()
                stopping_workers += 1
                assert worker.connection is not None
                # observe when the process will become join'able
                event_selector.register(worker.process.sentinel, selectors.EVENT_READ, data=worker.process.pid)

            while tasks_to_send and free_worker_pids:
                task = tasks_to_send.popleft()
                worker_pid = free_worker_pids.popleft()
                self._send_task(task, workers[worker_pid], timeouts_heap)

            for _ in range(workers_to_start):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(
                    target=self._worker_process_main,
                    args=(logging.getLogger().getEffectiveLevel(), child_conn),
                )
                proc.start()
                child_conn.close()
                assert proc.pid is not None
                # print(f'started worker pid={proc.pid}', file=sys.stderr)
                workers[proc.pid] = _WorkerInfo(process=proc, connection=parent_conn, active_task_future=None)
                event_selector.register(parent_conn, selectors.EVENT_READ, data=proc.pid)
                if tasks_to_send:
                    task = tasks_to_send.popleft()
                    self._send_task(task, workers[proc.pid], timeouts_heap)
                else:
                    free_worker_pids.append(proc.pid)

            while True:
                poll_timeout = 0 if has_tasks_to_serialize else timeouts_heap[0][0] - now if timeouts_heap else None
                events = event_selector.select(timeout=poll_timeout)
                for selector_key, _event_mask in events:
                    fileobj = selector_key.fileobj
                    if fileobj == self._shared_state.event_read_socket:
                        # Just eat the notifications - the next iteration of the outer loop will pick up the changes (a
                        # new task, or a shutdown signal, etc.).
                        self._shared_state.event_read_socket.recv(100)
                    elif isinstance(fileobj, multiprocessing.connection.Connection):
                        pid: int = selector_key.data
                        worker = workers[pid]
                        assert worker.connection is not None
                        try:
                            raw_message = worker.connection.recv_bytes()
                        except (EOFError, OSError):
                            if not worker.stopping and worker.active_task_future:
                                with contextlib.suppress(
                                    concurrent.futures.InvalidStateError
                                ):  # it might've been canceled
                                    worker.active_task_future.set_exception(ProcessPoolError(f'Worker {pid} died'))
                                worker.active_task_future = None
                            event_selector.unregister(worker.connection)
                            worker.connection.close()
                            worker.connection = None
                            # print(f'closing conn to worker={pid}', file=sys.stderr)
                            if worker.process is None:
                                workers.pop(pid)
                                stopping_workers -= 1
                        else:
                            if not worker.stopping and (
                                worker.active_task_future is None or not worker.active_task_future.done()
                            ):
                                message = multiprocessing.reduction.ForkingPickler.loads(raw_message)
                                if self._handle_message_from_worker(worker, message):
                                    if (
                                        pickled_task_queue
                                        and not pickled_task_queue[0].future.done()
                                        and len(workers) - len(free_worker_pids) < self._max_worker_count
                                    ):
                                        task = pickled_task_queue.popleft()
                                        self._send_task(task, worker, timeouts_heap)
                                    else:
                                        free_worker_pids.append(pid)
                    else:
                        pid: int = selector_key.data
                        worker = workers[pid]
                        assert worker.process is not None
                        event_selector.unregister(worker.process.sentinel)
                        worker.process.join()
                        worker.process = None
                        if worker.connection is None:
                            workers.pop(pid)
                            stopping_workers -= 1

                if events or not has_tasks_to_serialize:
                    break
                with self._shared_state.lock:
                    task = self._shared_state.task_queue.popleft()
                    has_tasks_to_serialize = len(self._shared_state.task_queue) > 0
                data = multiprocessing.reduction.ForkingPickler.dumps((task.f, task.args))
                pickled_task_queue.append(_PickledTask(data=data, future=task.future, timeout=task.timeout))

        # In pre-shutdown, we can terminate all pending tasks and workers except the currently-connecting ones (which
        # are needed to avoid deadlocking the accept() call in the connection listener).
        event_selector.close()
        with self._shared_state.lock:
            scheduled_tasks = self._shared_state.task_queue
        for task in scheduled_tasks:
            task.future.cancel()
        for task in pickled_task_queue:
            task.future.cancel()
        for worker in workers.values():
            if not worker.connection or worker.stopping:
                continue
            if worker.active_task_future:
                worker.active_task_future.cancel()
            # print(f'preshutdown: sigterm to pid={worker.process.pid}', file=sys.stderr)
            worker.process.terminate()
            worker.stopping = True
        # Pump all messages from busy workers to prevent them from blocking if the connection buffer gets full.
        for worker in workers.values():
            if not worker.connection or not worker.active_task_future:
                continue
            # print(f'preshutdown pumping conns pid={worker.process.pid}', file=sys.stderr)
            while True:
                try:
                    message = worker.connection.recv_bytes()
                except (EOFError, OSError):
                    break
        # print(f'preshutdown pumped all conns', file=sys.stderr)

        # Wait for the signal of the full shutdown (which comes after the connection listener has stopped).
        while True:
            with self._shared_state.lock:
                if self._shared_state.shutdown:
                    break
            # print(f'waiting for shutdown flag; not_handshaked={[pi for pi, w in workers.items() if w.connection is None]} running={[pi for pi in workers if psutil.pid_exists(pi)]}', file=sys.stderr)
            select.select([self._shared_state.event_read_socket], [], [])
            self._shared_state.event_read_socket.recv(100)
        # print(f'preshutdown done', file=sys.stderr)

        # Terminate the remaining workers and reap all children.
        for worker in workers.values():
            if not worker.stopping:
                assert worker.process is not None
                # print(f'shutdown sigterm to pid={worker.process.pid}', file=sys.stderr)
                worker.process.terminate()
        for worker in workers.values():
            if worker.process is not None:
                # print(f'shutdown joining pid={worker.process.pid}', file=sys.stderr)
                worker.process.join()
        # print(f'shutdown done', file=sys.stderr)

    def _send_task(self, task: _Task | _PickledTask, worker: _WorkerInfo, timeouts_heap) -> None:
        assert worker.process is not None
        assert worker.process.pid is not None
        task.future.add_done_callback(lambda future: self._on_future_resolved(worker.process.pid, future))
        assert worker.connection is not None
        data = (
            multiprocessing.reduction.ForkingPickler.dumps((task.f, task.args))
            if isinstance(task, _Task)
            else task.data
        )
        worker.connection.send_bytes(data)
        assert worker.active_task_future is None
        worker.active_task_future = task.future
        heapq.heappush(timeouts_heap, (time.monotonic() + task.timeout, task.future))

    def _handle_message_from_worker(self, worker: _WorkerInfo, message: Any) -> bool:
        if mplogging.maybe_handle_message_from_worker(message):
            return False
        if worker.active_task_future is None:
            return False
        with contextlib.suppress(concurrent.futures.InvalidStateError):  # it might've been canceled
            if isinstance(message, Exception):
                worker.active_task_future.set_exception(message)
            else:
                worker.active_task_future.set_result(message)
        worker.active_task_future = None
        return True

    def _on_future_resolved(self, worker_pid: int, future: Future):
        if not future.cancelled() and not isinstance(future.exception(timeout=0), TimeoutError):
            return
        self._shared_state.enqueue_task_abort(_ScheduledAbort(worker_pid=worker_pid, task_future=future))

    @staticmethod
    def _worker_process_main(logging_level: int, server_conn: multiprocessing.connection.Connection):
        try:
            ProcessPool._worker_process_main2(logging_level, server_conn)
        except Exception as e:
            print(f'{os.getpid()} worker process exception: {e}', file=sys.stderr)

    @staticmethod
    def _worker_process_main2(logging_level: int, server_conn: multiprocessing.connection.Connection):
        # print(f'worker[{os.getpid()}]: started', file=sys.stderr)
        sigmonitor.init(sigmonitor.Mode.RAISE_EXCEPTION_ON_DEMAND, handle_sigint=False)
        try:
            mplogging.init_in_worker(logging_level=logging_level, server_conn=server_conn)
            with selectors.DefaultSelector() as event_selector:
                event_selector.register(server_conn, selectors.EVENT_READ)
                event_selector.register(sigmonitor.get_wakeup_fd(), selectors.EVENT_READ)
                # Handle incoming tasks in an infinite loop (until stopped by a signal).
                while True:
                    events = event_selector.select()
                    can_recv = False
                    for selector_key, _event_mask in events:
                        if selector_key.fileobj != sigmonitor.get_wakeup_fd():
                            can_recv = True
                    sigmonitor.maybe_retrigger_action()
                    if can_recv:
                        f, args = server_conn.recv()
                        try:
                            result = f(*args)
                        except Exception as e:
                            result = e
                        sigmonitor.maybe_retrigger_action()
                        pickled = multiprocessing.reduction.ForkingPickler.dumps(result)
                        server_conn.send_bytes(pickled)
        except (EOFError, OSError):
            # print(f'worker[{os.getpid()}]: exit on {sys.exc_info()}', file=sys.stderr)
            return
        except (KeyboardInterrupt, SystemExit):
            # print(f'worker[{os.getpid()}]: exit on {sys.exc_info()}', file=sys.stderr)
            os._exit(1)
        except Exception as e:
            # print(f'worker[{os.getpid()}]: died on {sys.exc_info()}', file=sys.stderr)
            raise
