from __future__ import annotations

import concurrent.futures
import contextlib
import heapq
import io
import logging
import multiprocessing
import multiprocessing.connection
import multiprocessing.reduction
import os
import pickle
import selectors
import socket
import struct
import sys
import threading
import time
from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

from cvise.utils import mplogging, sigmonitor


class ProcessPoolError(Exception):
    pass


class ProcessPool:
    """Multiprocessing task pool with active task cancellation support.

    The worker pool is orchestrated by an event loop on a background thread. Implementation attempts to be highly
    parallelizable, using i/o multiplexing, task pre-pickling, worker precreation (concurrently to aborted worker
    shutdown), signals with file descriptor based handlers, and fine-tuned operation ordering (focused on workloads that
    take place in C-Vise).
    """

    def __init__(self, max_active_workers: int):
        event_read_socket, event_write_socket = socket.socketpair()
        event_read_socket.setblocking(False)
        event_write_socket.setblocking(False)

        self._shared_state = _SharedState(
            event_read_socket=event_read_socket,
            event_write_socket=event_write_socket,
            lock=threading.Lock(),
            shutdown=False,
            task_queue=deque(),
            scheduled_aborts=[],
        )
        self._pool_runner = _PoolRunner(max_active_workers, self._shared_state)
        self._pool_runner_thread = threading.Thread(target=self._pool_thread_main)

    def __enter__(self):
        self._pool_runner_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
        self._pool_runner_thread.join(60)
        if self._pool_runner_thread.is_alive():
            logging.warning('Failed to stop process pool thread')
            assert 0
        self._shared_state.close()

    def stop(self) -> None:
        """Initiates cancellation of pending tasks and termination of already running ones."""
        self._shared_state.notify_shutdown()

    def schedule(self, f: Callable, args: Sequence[Any], timeout: float) -> Future:
        future = Future()
        task = _Task(f=f, args=args, future=future, timeout=timeout)
        if not self._shared_state.enqueue_task(task):
            future.cancel()  # we're in shutdown
        return future

    def _pool_thread_main(self) -> None:
        self._pool_runner.run()
        self._pool_runner.shut_down()


@dataclass(slots=True)
class _Task:
    f: Callable
    args: Sequence[Any]
    future: Future
    timeout: float


@dataclass(slots=True)
class _PickledTask:
    packet: bytes
    future: Future
    timeout: float


@dataclass(slots=True)
class _ScheduledAbort:
    worker_pid: int
    task_future: Future


@dataclass(slots=True)
class _SharedState:
    """State shared between the pool runner thread and other threads.

    Most importantly, this is used to deliver newly scheduled tasks and task cancellations.
    """

    event_read_socket: socket.socket
    event_write_socket: socket.socket
    lock: threading.Lock
    shutdown: bool
    task_queue: deque[_Task]
    scheduled_aborts: list[_ScheduledAbort]

    def close(self) -> None:
        self.event_read_socket.close()
        self.event_write_socket.close()

    def take_work_batch(self, max_tasks: int) -> _WorkBatch | None:
        with self.lock:
            if self.shutdown:
                return None
            tasks = []
            while self.task_queue and len(tasks) < max_tasks:
                task = self.task_queue.popleft()
                if not task.future.done():
                    tasks.append(task)
            has_more_tasks = bool(self.task_queue)
            aborts = self.scheduled_aborts
            self.scheduled_aborts = []
        return _WorkBatch(tasks=tasks, aborts=aborts, has_more_tasks=has_more_tasks)

    def take_one_task(self) -> tuple[_Task, bool]:
        with self.lock:
            task = self.task_queue.popleft()
            has_more_tasks = bool(self.task_queue)
        return task, has_more_tasks

    def take_all_tasks(self) -> list[_Task]:
        with self.lock:
            tasks = list(self.task_queue)
            self.task_queue.clear()
        return tasks

    def notify_shutdown(self) -> None:
        with self.lock:
            self.shutdown = True
        self._notify()

    def enqueue_task(self, task: _Task) -> bool:
        with self.lock:
            if self.shutdown:
                return False
            self.task_queue.append(task)
            should_notify = self._should_notify()
        if should_notify:
            self._notify()
        return True

    def enqueue_abort(self, abort: _ScheduledAbort) -> None:
        with self.lock:
            if self.shutdown:
                return  # all pending & active tasks are canceled anyway during shutdown
            self.scheduled_aborts.append(abort)
            should_notify = self._should_notify()
        if should_notify:
            self._notify()

    def _should_notify(self) -> bool:
        return len(self.task_queue) + len(self.scheduled_aborts) == 1

    def _notify(self) -> None:
        self.event_write_socket.send(b'\0')


@dataclass(slots=True)
class _WorkBatch:
    tasks: list[_Task]
    aborts: list[_ScheduledAbort]
    has_more_tasks: bool


@dataclass(slots=True)
class _TimeoutsHeapNode:
    when: float
    task_future: Future

    def __lt__(self, other: _TimeoutsHeapNode) -> bool:
        return self.when < other.when


@dataclass(slots=True)
class _Worker:
    pid: int
    process: multiprocessing.Process | None
    connection: multiprocessing.connection.Connection | None
    sock: socket.socket | None
    active_task_future: Future | None
    stopping: bool = False


class _PoolRunner:
    """Implements the process pool event loop; is expected to be used on a background thread."""

    __slots__ = (
        '_event_selector',
        '_free_worker_pids',
        '_max_active_workers',
        '_pickled_task_queue',
        '_read_buf',
        '_shared_state',
        '_stopping_workers',
        '_timeouts_heap',
        '_workers',
    )

    def __init__(self, max_worker_count: int, shared_state: _SharedState):
        self._max_active_workers = max_worker_count
        self._shared_state = shared_state
        self._workers: dict[int, _Worker] = {}
        self._pickled_task_queue: deque[_PickledTask] = deque()
        self._free_worker_pids: deque[int] = deque()
        self._stopping_workers: int = 0
        self._timeouts_heap: list[_TimeoutsHeapNode] = []
        self._event_selector = selectors.DefaultSelector()
        self._event_selector.register(self._shared_state.event_read_socket, selectors.EVENT_READ)
        self._read_buf = bytearray(1000000)

    def run(self) -> None:
        while self._do_step():
            pass

    def _do_step(self) -> bool:
        # print(f'step', file=sys.stderr)
        assert len(self._free_worker_pids) <= len(self._workers)
        assert 0 <= len(self._workers) - self._stopping_workers <= self._max_active_workers
        assert 0 <= len(self._workers) - len(self._free_worker_pids) <= self._max_active_workers
        assert sum(1 for w in self._workers.values() if w.active_task_future is not None) <= self._max_active_workers

        # 1. If there are timed-out tasks, resolve their futures and schedule worker aborts.
        self._mark_timed_out_tasks()

        # 2. Load work to be done (tasks, aborts, shutdown flag, etc.) from the state shared across threads.
        max_new_tasks = self._max_active_workers - len(self._workers) + len(self._free_worker_pids)
        assert 0 <= max_new_tasks <= self._max_active_workers
        tasks_to_send: deque[_Task | _PickledTask] = self._take_pickled_tasks(max_tasks=max_new_tasks)
        batch = self._shared_state.take_work_batch(max_tasks=max_new_tasks - len(tasks_to_send))
        if batch is None:
            return False  # shutdown
        tasks_to_send.extend(batch.tasks)
        assert len(tasks_to_send) <= max_new_tasks

        # 3. Initiate stopping of workers for canceled/timed-out tasks. Note we'll spawn new workers below immediately,
        # however for the purpose of starting new tasks we'll treat the "in the process of stopping" workers as busy -
        # to avoid exceeding the concurrency limit even in this transition period.
        for abort in batch.aborts:
            self._trigger_worker_stop(abort.worker_pid, abort.task_future)

        # 4. Send new tasks to free workers.
        self._send_tasks_to_free_workers(tasks_to_send)

        # 5. Start fresh workers (on pool startup or after aborts); if possible, immediately send them tasks.
        workers_to_start = self._max_active_workers - len(self._workers) + self._stopping_workers
        assert 0 <= workers_to_start <= self._max_active_workers
        for _ in range(workers_to_start):
            self._start_worker()
        self._send_tasks_to_free_workers(tasks_to_send)
        assert not tasks_to_send

        # 6. Wait for asynchronous events (messages from workers, process terminations, signals). When applicable, spend
        # the waiting time on pickling previously scheduled tasks.
        has_tasks_to_pickle = batch.has_more_tasks
        while has_tasks_to_pickle:
            if self._pump_file_descriptors(wait=False):
                break
            has_tasks_to_pickle = self._pickle_one_task()
        else:
            self._pump_file_descriptors(wait=True)
        return True

    def shut_down(self) -> None:
        # print(f'poolrunner.close: event_selector.close()', file=sys.stderr)
        self._event_selector.close()
        # Cancel pending tasks.
        for task in self._pickled_task_queue:
            task.future.cancel()
        self._pickled_task_queue.clear()
        for task in self._shared_state.take_all_tasks():
            task.future.cancel()
        # Abort running tasks and terminate all workers.
        for worker in self._workers.values():
            if not worker.stopping:
                assert worker.process is not None
                worker.process.terminate()
            if worker.connection:
                while True:
                    try:
                        worker.connection.recv_bytes()
                    except (EOFError, OSError):
                        break
                worker.sock.detach()
                worker.connection.close()
            if worker.active_task_future:
                worker.active_task_future.cancel()
        for worker in self._workers.values():
            if worker.process is not None:
                worker.process.join()
        self._workers.clear()

    def _pump_file_descriptors(self, wait: bool) -> bool:
        if not wait:
            poll_timeout = 0
        elif self._timeouts_heap:
            earliest_timeout = self._timeouts_heap[0].when
            now = time.monotonic()
            poll_timeout = max(0, earliest_timeout - now)
        else:
            poll_timeout = None
        events = self._event_selector.select(timeout=poll_timeout)
        for selector_key, _event_mask in events:
            fileobj = selector_key.fileobj
            if fileobj == self._shared_state.event_read_socket:
                # Just drain the notification(s) - the next iteration of the run loop will pick up the changes.
                self._shared_state.event_read_socket.recv(1000)
            elif isinstance(fileobj, multiprocessing.connection.Connection):
                pid: int = selector_key.data
                self._on_worker_conn_ready(self._workers[pid])
            else:  # must be the process sentinel
                pid: int = selector_key.data
                self._on_worker_proc_joinable(self._workers[pid])
        return bool(events)

    def _start_worker(self) -> None:
        parent_conn, child_conn = multiprocessing.Pipe()
        proc = multiprocessing.Process(
            target=_worker_process_main,
            args=(logging.getLogger().getEffectiveLevel(), child_conn),
        )
        proc.start()
        child_conn.close()
        assert proc.pid is not None
        assert proc.pid not in self._workers
        sock = socket.socket(fileno=parent_conn.fileno(), family=socket.AF_UNIX, type=socket.SOCK_STREAM)
        self._workers[proc.pid] = _Worker(
            pid=proc.pid, process=proc, connection=parent_conn, sock=sock, active_task_future=None
        )
        self._event_selector.register(parent_conn, selectors.EVENT_READ, data=proc.pid)
        self._free_worker_pids.append(proc.pid)

    def _on_worker_conn_ready(self, worker: _Worker) -> None:
        assert worker.connection is not None
        # print(f'reading from pid={worker.pid}', file=sys.stderr)
        try:
            nbytes = worker.sock.recv_into(self._read_buf)
        except OSError:
            self._handle_worker_conn_eof(worker)
            return
        # print(f'read from pid={worker.pid} len={nbytes}', file=sys.stderr)
        view = memoryview(self._read_buf[:nbytes])
        if not view:
            self._handle_worker_conn_eof(worker)
            return
        handled_task_completion = False
        while view:
            assert len(view) >= 4
            sz: int = view[:4].cast('i')[0]
            assert sz >= 0, f'buf={bytes(view[:4])} sz={sz}'
            raw_message = view[4 : 4 + sz]
            # print(f'handling msg from pid={worker.pid} len={sz}', file=sys.stderr)
            if self._handle_message_from_worker(worker, raw_message) and not handled_task_completion:
                handled_task_completion = True
                # If the worker has gotten free, try sending it a task immediately - if we have one without the need in
                # taking a mutex. We could've just let the event loop do it, but the latency is lower this way.
                if not self._maybe_send_pickled_task(worker):
                    self._free_worker_pids.append(worker.pid)
            view = view[4 + sz :]

    def _send_tasks_to_free_workers(self, tasks: deque[_Task | _PickledTask]) -> None:
        while tasks and self._free_worker_pids:
            task = tasks.popleft()
            worker_pid = self._free_worker_pids.popleft()
            self._send_task(task, self._workers[worker_pid])

    def _send_task(self, task: _Task | _PickledTask, worker: _Worker) -> None:
        assert worker.process is not None
        task.future.add_done_callback(lambda future: self._on_future_resolved(worker.pid, future))
        assert worker.connection is not None
        match task:
            case _Task():
                packet = _create_packet((task.f, task.args))
            case _PickledTask():
                packet = memoryview(task.packet)
        # print(f'sending task to pid={worker.pid} len={len(packet)}', file=sys.stderr)
        while packet:
            nbytes = worker.sock.send(packet)
            packet = packet[nbytes:]
        assert worker.active_task_future is None
        worker.active_task_future = task.future
        timeout_when = time.monotonic() + task.timeout
        heapq.heappush(self._timeouts_heap, _TimeoutsHeapNode(when=timeout_when, task_future=task.future))
        # print(f'sent task to pid={worker.pid}', file=sys.stderr)

    def _maybe_send_pickled_task(self, worker: _Worker) -> bool:
        if not self._pickled_task_queue:
            return False  # nothing to send
        if len(self._workers) - len(self._free_worker_pids) >= self._max_active_workers:
            return False  # would exceed allowed concurrency
        task = self._pickled_task_queue.popleft()
        self._send_task(task, worker)
        return True

    def _trigger_worker_stop(self, worker_pid: int, task_future: Future) -> None:
        if worker_pid not in self._workers:
            return  # already died
        worker = self._workers[worker_pid]
        if worker.active_task_future != task_future:
            return  # a new task started already
        assert worker.process is not None
        worker.stopping = True
        worker.active_task_future = None
        worker.process.terminate()
        self._stopping_workers += 1
        assert worker.connection is not None
        # observe when the process will become join'able
        self._event_selector.register(worker.process.sentinel, selectors.EVENT_READ, data=worker.pid)

    def _take_pickled_tasks(self, max_tasks: int) -> deque[_PickledTask]:
        taken = deque()
        while len(taken) < max_tasks and self._pickled_task_queue:
            task = self._pickled_task_queue.popleft()
            if not task.future.done():
                taken.append(task)
        return taken

    def _handle_message_from_worker(self, worker: _Worker, raw_message: memoryview) -> bool:
        if worker.stopping:
            return False  # worker shutdown produces spurious errors; we simply wait till EOF
        if worker.active_task_future is not None and worker.active_task_future.done():
            return False  # similar to above, but the future cancellation didn't propagate as the worker shutdown yet
        message = pickle.loads(raw_message)
        if mplogging.maybe_handle_message_from_worker(message):
            return False
        return self._handle_task_result(worker, message)

    def _handle_task_result(self, worker: _Worker, result_message: Any) -> bool:
        if worker.active_task_future is None:
            return False
        if isinstance(result_message, Exception):
            _assign_future_exception(worker.active_task_future, result_message)
        else:
            _assign_future_result(worker.active_task_future, result_message)
        worker.active_task_future = None
        return True

    def _pickle_one_task(self) -> bool:
        task, has_more_tasks = self._shared_state.take_one_task()
        packet = bytes(_create_packet((task.f, task.args)))
        self._pickled_task_queue.append(_PickledTask(packet=packet, future=task.future, timeout=task.timeout))
        return has_more_tasks

    def _mark_timed_out_tasks(self) -> None:
        now = time.monotonic()
        while self._timeouts_heap:
            node = self._timeouts_heap[0]
            if node.when > now and not node.task_future.done():
                break
            heapq.heappop(self._timeouts_heap)
            if not node.task_future.done():
                _assign_future_exception(node.task_future, TimeoutError('Job timed out'))

    def _on_future_resolved(self, worker_pid: int, future: Future) -> None:
        if _future_aborted(future):
            self._shared_state.enqueue_abort(_ScheduledAbort(worker_pid=worker_pid, task_future=future))

    def _on_worker_proc_joinable(self, worker: _Worker) -> None:
        assert worker.process is not None
        self._event_selector.unregister(worker.process.sentinel)
        worker.process.join()
        # print(f'joined pid={worker.pid}', file=sys.stderr)
        worker.process = None
        if worker.connection is None:
            self._workers.pop(worker.pid)
            self._stopping_workers -= 1
            assert self._stopping_workers >= 0

    def _handle_worker_conn_eof(self, worker: _Worker) -> None:
        assert worker.connection is not None
        if worker.active_task_future is not None:
            if not worker.active_task_future.done():
                _assign_future_exception(worker.active_task_future, ProcessPoolError(f'Worker {worker.pid} died'))
            worker.active_task_future = None
        self._event_selector.unregister(worker.connection)
        worker.sock.detach()
        worker.sock = None
        worker.connection.close()
        worker.connection = None
        # print(f'closed conn to worker={worker.pid}', file=sys.stderr)
        if worker.process is None:
            self._workers.pop(worker.pid)
            self._stopping_workers -= 1


def _worker_process_main(logging_level: int, server_conn: multiprocessing.connection.Connection) -> None:
    # print(f'worker[{os.getpid()}]: started', file=sys.stderr)
    sigmonitor.init(sigmonitor.Mode.QUICK_EXIT, sigint=False, sigchld=True)
    mplogging.init_in_worker(logging_level=logging_level, server_conn=server_conn)
    wakeup_fd = sigmonitor.get_wakeup_fd()
    read_buf = bytearray(1000000)
    sock = socket.socket(fileno=server_conn.fileno(), family=socket.AF_UNIX, type=socket.SOCK_STREAM)
    with sigmonitor.scoped_mode(sigmonitor.Mode.RAISE_EXCEPTION_ON_DEMAND):
        try:
            with selectors.DefaultSelector() as event_selector:
                event_selector.register(server_conn, selectors.EVENT_READ)
                event_selector.register(wakeup_fd, selectors.EVENT_READ)
                # Handle incoming tasks in an infinite loop (until stopped by a signal).
                while True:
                    events = event_selector.select()
                    can_recv = False
                    for selector_key, _event_mask in events:
                        if selector_key.fileobj == wakeup_fd:
                            try:
                                os.read(wakeup_fd, 1024)
                            except OSError:
                                pass
                            sigmonitor.maybe_retrigger_action()
                        else:
                            can_recv = True
                    if can_recv:
                        try:
                            nbytes = sock.recv_into(read_buf)
                        except OSError:
                            break
                        view = memoryview(read_buf[:nbytes])
                        if not view:
                            break
                        sz: int = view[:4].cast('i')[0]
                        # print(f'worker[{os.getpid()}]: received sz={sz}', file=sys.stderr)
                        raw_message = view[4 : 4 + sz]
                        assert sz == len(view) - 4, f'sz={sz} len={len(view)} pid={os.getpid()}'
                        f, args = pickle.loads(raw_message)
                        # print(f'worker[{os.getpid()}]: task begin', file=sys.stderr)
                        try:
                            result = f(*args)
                        except Exception as e:
                            result = e
                        # print(f'worker[{os.getpid()}]: task end', file=sys.stderr)
                        sigmonitor.maybe_retrigger_action()
                        # print(f'worker[{os.getpid()}]: task reply begin', file=sys.stderr)
                        packet = _create_packet(result)
                        while packet:
                            nbytes = sock.send(packet)
                            packet = packet[nbytes:]
                        packet.release()
                        # print(f'worker[{os.getpid()}]: task reply end', file=sys.stderr)
        finally:
            # print(f'worker[{os.getpid()}]: dying', file=sys.stderr)
            sock.detach()
            server_conn.close()


_pickler_bytes_io: io.BytesIO | None = None
_pickler: multiprocessing.reduction.ForkingPickler | None = None
_packer = struct.Struct('i')


def _create_packet(value: Any) -> memoryview:
    global _pickler
    global _pickler_bytes_io
    if _pickler is None:
        _pickler_bytes_io = io.BytesIO()
        _pickler = multiprocessing.reduction.ForkingPickler(_pickler_bytes_io)
    else:
        _pickler_bytes_io.seek(0)
        _pickler_bytes_io.truncate(0)
        _pickler.clear_memo()
    _pickler_bytes_io.write(b'\0\0\0\0')  # length
    _pickler.dump(value)
    view = _pickler_bytes_io.getbuffer()
    _packer.pack_into(view, 0, len(view) - 4)
    return view


def _future_aborted(future: Future) -> bool:
    if future.cancelled():
        return True
    exc = future.exception(timeout=0)
    return isinstance(exc, TimeoutError)


def _assign_future_result(future: Future, result: Any) -> None:
    if future.done():
        return
    with contextlib.suppress(concurrent.futures.InvalidStateError):  # cover against concurrent changes
        future.set_result(result)


def _assign_future_exception(future: Future, exc: BaseException) -> None:
    if future.done():
        return
    with contextlib.suppress(concurrent.futures.InvalidStateError):  # cover against concurrent changes
        future.set_exception(exc)
