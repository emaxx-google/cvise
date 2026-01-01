from __future__ import annotations

import concurrent.futures
import contextlib
import enum
import heapq
import io
import logging
import multiprocessing
import multiprocessing.reduction
import pickle
import selectors
import socket
import struct
import threading
import time
from collections import deque
from collections.abc import Callable, Collection, Sequence
from concurrent.futures import Future
from copy import copy
from dataclasses import dataclass
from typing import Any

from cvise.utils import sigmonitor


_DEFAULT_RECV_BUF_SIZE = 131072  # chosen to exceed most of C-Vise task size
_TIMEOUTS_HEAP_REPACK_FACTOR = 10  # to prevent excessively big heap size
_pickler_bytes_io: io.BytesIO | None = None
_pickler: multiprocessing.reduction.ForkingPickler | None = None
_header_struct = struct.Struct('bi')
_HEADER_SIZE = _header_struct.size
_HEADER_STUB = b'\0' * _HEADER_SIZE


@enum.unique
class _MarshalledType(enum.IntEnum):
    TASK = enum.auto()
    TASK_RESULT = enum.auto()
    TASK_ERROR = enum.auto()
    LOG = enum.auto()


class ProcessPoolError(Exception):
    pass


class ProcessPool:
    """Multiprocessing task pool with active task cancellation support.

    The worker pool is orchestrated by an event loop on a background thread. Implementation attempts to be highly
    parallelizable, using i/o multiplexing, task pre-pickling, worker precreation (concurrently to aborted worker
    shutdown), signals with file descriptor based handlers, and fine-tuned operation ordering (focused on workloads that
    take place in C-Vise).
    """

    def __init__(self, max_active_workers: int, recv_buf_size: int = _DEFAULT_RECV_BUF_SIZE):
        self._exit_stack = contextlib.ExitStack()

        self._shutdown = False

        self._shutdown_notifier = _Notifier()
        self._exit_stack.enter_context(self._shutdown_notifier)

        self._task_queue = _TaskQueue()
        self._exit_stack.enter_context(self._task_queue)

        self._staged_worker_inbox = _StagedWorkerInbox()
        self._exit_stack.enter_context(self._staged_worker_inbox)

        self._abort_inbox = _AbortInbox()
        self._exit_stack.enter_context(self._abort_inbox)

        self._worker_creator = _WorkerCreator(recv_buf_size, self._staged_worker_inbox)

        self._pool_runner = _PoolRunner(
            max_active_workers=max_active_workers,
            recv_buf_size=recv_buf_size,
            shutdown_notifier=self._shutdown_notifier,
            task_queue=self._task_queue,
            staged_worker_inbox=self._staged_worker_inbox,
            abort_inbox=self._abort_inbox,
            worker_creator=self._worker_creator,
        )
        self._pool_runner_thread = threading.Thread(target=self._pool_thread_main, name='PoolThread', daemon=True)

    def __enter__(self) -> ProcessPool:
        self._pool_runner_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

        self._worker_creator.join_thread()
        self._pool_runner_thread.join(60)  # semi-arbitrary timeout to prevent possibility of deadlocks
        if self._pool_runner_thread.is_alive():
            logging.warning('Failed to stop process pool thread')
        self._exit_stack.__exit__(exc_type, exc_val, exc_tb)

        # Clear leftovers after the threads has been shut down.

        for task in self._task_queue.tasks:
            task.future.cancel()

        staged_workers = self._staged_worker_inbox.take_all()
        for staged in staged_workers:
            staged.proc.terminate()
        for staged in staged_workers:
            staged.proc.join()

    def stop(self) -> None:
        """Initiates cancellation of pending tasks and termination of already running ones."""
        self._shutdown = True
        self._shutdown_notifier.notify()
        self._worker_creator.notify_shutdown()

    def schedule(self, f: Callable, args: Sequence[Any], timeout: float) -> Future:
        future = Future()
        if self._shutdown:
            future.cancel()
        else:
            task = _Task(f=f, args=args, future=future, timeout=timeout)
            self._task_queue.enqueue(task)
        return future

    def abort(self, future: Future) -> None:
        self._abort_inbox.add(future)

    def abort_all(self, futures: list[Future]) -> None:
        self._abort_inbox.add_all(futures)

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
class _MarshalledTask:
    wire_bytes: bytes
    future: Future
    timeout: float


class _Notifier:
    """Implements basic notification pattern that's usable with select()."""

    __slots__ = ('read_sock', '_write_sock')

    def __init__(self):
        self.read_sock: socket.socket
        self._write_sock: socket.socket

    def __enter__(self) -> _Notifier:
        self.read_sock, self._write_sock = socket.socketpair()
        self.read_sock.setblocking(False)
        self._write_sock.setblocking(False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.read_sock.close()
        self._write_sock.close()

    def notify(self) -> None:
        try:
            self._write_sock.send(b'\0')
        except BlockingIOError:
            pass  # no need in excessive notifications

    def eat_notifications(self, buf: bytearray) -> None:
        self.read_sock.recv_into(buf)


class _TaskQueue(_Notifier):
    """Container for storing incoming tasks and notifying the pool runner about them.

    Note that the pool runner reads directly from the "tasks" property, relying on it data structure being thread-safe.
    """

    __slots__ = ('tasks',)

    def __init__(self):
        super().__init__()
        self.tasks: deque[_Task] = deque()

    def enqueue(self, task: _Task) -> None:
        self.tasks.append(task)
        self.notify()


@dataclass(slots=True)
class _StagedWorker:
    """A newly created worker, as returned by _WorkerCreator."""

    pid: int
    proc: multiprocessing.Process
    sock: socket.socket


class _StagedWorkerInbox(_Notifier):
    """Container for delivering staged (newly created) workers to the pool runner."""

    __slots__ = ('_lock', '_workers')

    def __init__(self):
        self._lock = threading.Lock()
        self._workers: list[_StagedWorker] = []

    def add(self, worker: _StagedWorker) -> None:
        with self._lock:
            should_notify = not self._workers  # no need in notifying more than once
            self._workers.append(worker)
        if should_notify:
            self.notify()

    def take_all(self) -> list[_StagedWorker]:
        with self._lock:
            ret = self._workers
            self._workers = []
        return ret


class _AbortInbox(_Notifier):
    """Container for delivering to-be-aborted task futures to the pool runner."""

    __slots__ = ('_lock', '_aborts')

    def __init__(self):
        self._lock = threading.Lock()
        self._aborts: list[Future] = []

    def add(self, abort: Future) -> None:
        with self._lock:
            should_notify = not self._aborts  # no need in notifying more than once
            self._aborts.append(abort)
        if should_notify:
            self.notify()

    def add_all(self, aborts: Collection[Future]) -> None:
        if not aborts:
            return
        with self._lock:
            should_notify = not self._aborts  # no need in notifying more than once
            self._aborts.extend(aborts)
        if should_notify:
            self.notify()

    def take_all(self) -> list[Future]:
        with self._lock:
            ret = self._aborts
            self._aborts = []
            return ret


@dataclass(slots=True)
class _TimeoutsHeapNode:
    when: float
    task_future: Future

    def __lt__(self, other: _TimeoutsHeapNode) -> bool:
        return self.when < other.when


@dataclass(slots=True)
class _Worker:
    pid: int
    proc: multiprocessing.Process | None
    sock: socket.socket | None
    task_future: Future | None = None
    stopping: bool = False
    partial_recv: bytearray | None = None


class _WorkerCreator:
    """Creates multiprocessing.Process instances on a background thread.

    The purpose is to avoid blocking operations (the handshake during the process creation) on the pool runner thread.
    """

    def __init__(self, recv_buf_size: int, staged_worker_inbox: _StagedWorkerInbox):
        self._recv_buf_size = recv_buf_size
        self._staged_worker_inbox = staged_worker_inbox
        self._cond = threading.Condition()
        self._scheduled_count = 0
        self._shutdown = False
        self._thread = threading.Thread(target=self._worker_creator_thread_main, name='WorkerCreator', daemon=True)
        self._thread.start()

    def schedule_creation(self, cnt: int) -> None:
        if not cnt:
            return
        with self._cond:
            self._scheduled_count += cnt
            self._cond.notify()

    def notify_shutdown(self) -> None:
        with self._cond:
            self._shutdown = True
            self._cond.notify()

    def join_thread(self) -> None:
        self._thread.join(60)  # semi-arbitrary timeout to prevent possibility of deadlocks
        if self._thread.is_alive():
            logging.warning('Failed to stop WorkerCreator thread')

    def _worker_creator_thread_main(self) -> None:
        while True:
            cnt = 0
            with self._cond:
                if self._shutdown:
                    return
                if not self._scheduled_count:
                    self._cond.wait()
                    continue
                cnt = self._scheduled_count
                self._scheduled_count = 0

            for _ in range(cnt):
                self._staged_worker_inbox.add(_create_worker(self._recv_buf_size))


class _PoolRunner:
    """Implements the pool's event loop; is expected to be used on a background thread."""

    __slots__ = (
        '_abort_inbox',
        '_busy_workers',
        '_selector',
        '_task_future_to_worker_pid',
        '_idle_worker_pids',
        '_marshalled_task_queue',
        '_max_active_workers',
        '_recv_buf',
        '_recv_buf_size',
        '_shutdown_notifier',
        '_shutdown',
        '_staged_worker_inbox',
        '_stopping_workers',
        '_task_queue',
        '_timeouts_heap',
        '_worker_creator',
        '_workers',
    )

    def __init__(
        self,
        max_active_workers: int,
        recv_buf_size: int,
        shutdown_notifier: _Notifier,
        task_queue: _TaskQueue,
        staged_worker_inbox: _StagedWorkerInbox,
        abort_inbox: _AbortInbox,
        worker_creator: _WorkerCreator,
    ):
        self._max_active_workers = max_active_workers
        self._recv_buf_size = recv_buf_size
        self._shutdown_notifier = shutdown_notifier
        self._task_queue = task_queue
        self._staged_worker_inbox = staged_worker_inbox
        self._abort_inbox = abort_inbox
        self._worker_creator = worker_creator
        self._workers: dict[int, _Worker] = {}
        self._marshalled_task_queue: deque[_MarshalledTask] = deque()
        self._busy_workers: int = 0
        self._idle_worker_pids: deque[int] = deque()
        self._stopping_workers: int = 0
        self._timeouts_heap: list[_TimeoutsHeapNode] = []
        self._selector = selectors.DefaultSelector()
        self._selector.register(
            self._shutdown_notifier.read_sock, selectors.EVENT_READ, data=(self._handle_shutdown_fd, ())
        )
        self._selector.register(self._task_queue.read_sock, selectors.EVENT_READ, data=(self._handle_task_queue_fd, ()))
        self._selector.register(
            self._staged_worker_inbox.read_sock, selectors.EVENT_READ, data=(self._handle_staged_worker_inbox_fd, ())
        )
        self._selector.register(
            self._abort_inbox.read_sock, selectors.EVENT_READ, data=(self._handle_abort_inbox_fd, ())
        )
        self._recv_buf = bytearray(self._recv_buf_size)
        self._shutdown = False
        self._task_future_to_worker_pid: dict[Future, int] = {}

    def run(self) -> None:
        self._worker_creator.schedule_creation(self._max_active_workers)
        while not self._shutdown:
            self._do_step()

    def _do_step(self) -> None:
        assert len(self._idle_worker_pids) <= len(self._workers)
        assert self._busy_workers == len(self._workers) - len(self._idle_worker_pids), (
            f'workers={len(self._workers)} idle={len(self._idle_worker_pids)} busy={self._busy_workers} stopping={self._stopping_workers} max={self._max_active_workers}'
        )
        assert 0 <= len(self._workers) - self._stopping_workers, (
            f'workers={len(self._workers)} idle={len(self._idle_worker_pids)} stopping={self._stopping_workers} max={self._max_active_workers}'
        )
        assert 0 <= len(self._workers) - len(self._idle_worker_pids) <= self._max_active_workers, (
            f'workers={len(self._workers)} idle={len(self._idle_worker_pids)} max={self._max_active_workers}'
        )
        assert sum(1 for w in self._workers.values() if w.task_future is not None) <= self._max_active_workers

        self._mark_timed_out_tasks()
        while self._task_queue.tasks:
            self._select_and_process_fds(wait=False)
            self._maybe_send_or_marshal_single_task()
        self._select_and_process_fds(wait=True)

    def shut_down(self) -> None:
        self._selector.close()
        # Cancel pending tasks.
        for task in self._marshalled_task_queue:
            task.future.cancel()
        self._marshalled_task_queue.clear()
        while self._task_queue.tasks:
            task = self._task_queue.tasks.popleft()
            task.future.cancel()
        # Abort running tasks and terminate all workers.
        for worker in self._workers.values():
            if not worker.stopping:
                assert worker.proc is not None
                worker.proc.terminate()
            if worker.sock:
                worker.sock.setblocking(True)
                while True:
                    try:
                        received = worker.sock.recv_into(self._recv_buf)
                    except (EOFError, OSError):
                        break
                    if not received:
                        break
                worker.sock.close()
            if worker.task_future:
                worker.task_future.cancel()
        for worker in self._workers.values():
            if worker.proc is not None:
                worker.proc.join()
        self._workers.clear()

    def _select_and_process_fds(self, wait: bool) -> None:
        if not wait:
            poll_timeout = 0
        elif self._timeouts_heap:
            earliest_timeout = self._timeouts_heap[0].when
            now = time.monotonic()
            poll_timeout = max(0, earliest_timeout - now)
        else:
            poll_timeout = None

        events = self._selector.select(timeout=poll_timeout)
        for selector_key, _event_mask in events:
            handler: Callable
            args: tuple
            handler, args = selector_key.data
            handler(*args)

    def _handle_shutdown_fd(self) -> None:
        self._shutdown_notifier.eat_notifications(self._recv_buf)
        self._shutdown = True

    def _handle_task_queue_fd(self) -> None:
        self._task_queue.eat_notifications(self._recv_buf)
        if not self._marshalled_task_queue:
            while self._task_queue.tasks and self._busy_workers < self._max_active_workers and self._idle_worker_pids:
                task = self._task_queue.tasks.popleft()
                if task.future.cancelled():
                    continue
                worker_pid = self._idle_worker_pids.popleft()
                self._send_task(task, self._workers[worker_pid])

    def _handle_staged_worker_inbox_fd(self) -> None:
        self._staged_worker_inbox.eat_notifications(self._recv_buf)

        for staged in self._staged_worker_inbox.take_all():
            assert staged.pid not in self._workers

            worker = _Worker(pid=staged.pid, proc=staged.proc, sock=staged.sock)
            self._workers[staged.pid] = worker
            self._selector.register(
                staged.sock, selectors.EVENT_READ, data=(self._on_readable_worker_sock, (staged.pid,))
            )
            self._send_task_or_mark_free(worker)

    def _handle_abort_inbox_fd(self) -> None:
        self._abort_inbox.eat_notifications(self._recv_buf)
        to_create = 0
        for task_future in self._abort_inbox.take_all():
            worker_pid = self._task_future_to_worker_pid.pop(task_future, None)
            if worker_pid is None:
                # the task hasn't started or has already finished
                continue
            self._trigger_worker_stop(worker_pid)
            to_create += 1
        self._worker_creator.schedule_creation(to_create)

    def _on_readable_worker_sock(self, pid: int) -> None:
        worker = self._workers[pid]
        assert worker.sock is not None

        try:
            nbytes = worker.sock.recv_into(self._recv_buf)
            chunk = memoryview(self._recv_buf)[:nbytes]
            if worker.partial_recv is None:
                view = chunk
            else:
                worker.partial_recv.extend(chunk)
                view = memoryview(worker.partial_recv)
        except (BlockingIOError, TimeoutError):
            raise  # we expect the file descriptor to be in non-blocking mode
        except OSError:
            self._handle_worker_sock_eof(worker)
            return
        if not chunk:
            self._handle_worker_sock_eof(worker)
            return

        while view:
            if not (parsed := _try_unmarshal(view)):
                break  # the message is still incomplete - remember the read prefix and move on
            tp, raw_message, view = parsed

            if worker.stopping:
                continue  # during worker shutdown, ignore replies and (likely spurious error) logs; we wait for EOF
            match tp:
                case _MarshalledType.TASK_RESULT.value:
                    self._on_worker_task_reply_msg(worker, raw_message, succeeded=True)
                case _MarshalledType.TASK_ERROR.value:
                    self._on_worker_task_reply_msg(worker, raw_message, succeeded=False)
                case _MarshalledType.LOG.value:
                    self._on_worker_log_msg(worker, raw_message)
                case _:
                    raise ValueError(f'Unexpected message type {tp} received from worker')

        if not view:
            worker.partial_recv = None
        elif worker.partial_recv is None:
            worker.partial_recv = bytearray(view)

    def _on_worker_task_reply_msg(self, worker: _Worker, raw_message: memoryview, succeeded: bool) -> None:
        # Update the worker state.
        future = worker.task_future
        assert future is not None
        tracked_pid = self._task_future_to_worker_pid.pop(future)
        assert tracked_pid == worker.pid
        worker.task_future = None
        self._busy_workers -= 1
        assert self._busy_workers >= 0

        # Try sending a new task quickly.
        self._send_task_or_mark_free(worker)

        # Resolve the task future.
        result = pickle.loads(raw_message)
        with contextlib.suppress(concurrent.futures.InvalidStateError):  # it could've been canceled or time out
            if succeeded:
                future.set_result(result)
            else:
                future.set_exception(result)

    def _on_worker_log_msg(self, worker: _Worker, raw_message: memoryview) -> None:
        # Ignore logs if the future is already switched to "done" - these are likely spurious errors because the main
        # process started deleting task's files.
        if worker.task_future is not None and worker.task_future.done():
            return
        message = pickle.loads(raw_message)
        record: logging.LogRecord = message
        logger = logging.getLogger(record.name)
        logger.handle(record)

    def _on_worker_proc_fd_joinable(self, worker_pid: int) -> None:
        worker = self._workers[worker_pid]
        assert worker.proc is not None
        self._selector.unregister(worker.proc.sentinel)
        worker.proc.join()
        worker.proc = None
        if worker.sock is None:
            self._workers.pop(worker.pid)
            self._stopping_workers -= 1
            self._busy_workers -= 1
            assert self._stopping_workers >= 0
            self._maybe_send_task_to_free_worker()

    def _send_task(self, task: _Task | _MarshalledTask, worker: _Worker) -> None:
        assert worker.proc is not None
        assert worker.sock is not None
        match task:
            case _Task():
                wire_bytes = _marshal(_MarshalledType.TASK.value, (task.f, task.args))
            case _MarshalledTask():
                wire_bytes = memoryview(task.wire_bytes)
        worker.sock.sendall(wire_bytes)
        assert worker.task_future is None
        worker.task_future = task.future
        self._busy_workers += 1
        timeout_when = time.monotonic() + task.timeout
        heapq.heappush(self._timeouts_heap, _TimeoutsHeapNode(when=timeout_when, task_future=task.future))
        self._task_future_to_worker_pid[task.future] = worker.pid

    def _send_task_or_mark_free(self, worker: _Worker) -> None:
        if self._busy_workers < self._max_active_workers:
            if self._marshalled_task_queue:
                self._send_task(self._marshalled_task_queue.popleft(), worker)
                return
            if self._task_queue.tasks:
                self._send_task(self._task_queue.tasks.popleft(), worker)
                return
        self._idle_worker_pids.append(worker.pid)

    def _maybe_send_task_to_free_worker(self) -> None:
        if self._idle_worker_pids and (self._task_queue.tasks or self._marshalled_task_queue):
            worker_pid = self._idle_worker_pids.popleft()
            self._send_task(
                (self._marshalled_task_queue or self._task_queue.tasks).popleft(), self._workers[worker_pid]
            )

    def _trigger_worker_stop(self, worker_pid: int) -> None:
        worker = self._workers[worker_pid]
        assert worker.proc is not None
        worker.stopping = True
        worker.task_future = None
        worker.proc.terminate()
        self._stopping_workers += 1
        assert worker.sock is not None
        # observe when the process will become join'able
        self._selector.register(
            worker.proc.sentinel, selectors.EVENT_READ, data=(self._on_worker_proc_fd_joinable, (worker_pid,))
        )

    def _maybe_send_or_marshal_single_task(self) -> None:
        if not self._task_queue.tasks:
            return
        task = self._task_queue.tasks.popleft()
        if self._busy_workers < self._max_active_workers and self._idle_worker_pids:
            assert not self._marshalled_task_queue
            if not task.future.cancelled():
                pid = self._idle_worker_pids.popleft()
                self._send_task(task, self._workers[pid])
        else:
            packet = bytes(_marshal(_MarshalledType.TASK.value, (task.f, task.args)))
            self._marshalled_task_queue.append(
                _MarshalledTask(wire_bytes=packet, future=task.future, timeout=task.timeout)
            )

    def _mark_timed_out_tasks(self) -> None:
        if len(self._timeouts_heap) > self._max_active_workers * _TIMEOUTS_HEAP_REPACK_FACTOR:
            # repack the heap if it has too many completed tasks
            self._timeouts_heap = [n for n in self._timeouts_heap if not n.task_future.done()]
            heapq.heapify(self._timeouts_heap)
            assert len(self._timeouts_heap) <= self._max_active_workers

        now = time.monotonic()
        while self._timeouts_heap:
            node = self._timeouts_heap[0]
            done = node.task_future.done()
            if node.when > now and not done:
                break
            heapq.heappop(self._timeouts_heap)
            if not done:
                _assign_future_exception(node.task_future, TimeoutError('Job timed out'))
                self._abort_inbox.add(node.task_future)

    def _handle_worker_sock_eof(self, worker: _Worker) -> None:
        assert worker.sock is not None
        if worker.task_future is not None:
            if not worker.task_future.done():
                _assign_future_exception(worker.task_future, ProcessPoolError(f'Worker {worker.pid} died'))
            tracked_pid = self._task_future_to_worker_pid.pop(worker.task_future)
            assert tracked_pid == worker.pid
            worker.task_future = None
        self._selector.unregister(worker.sock)

        worker.sock.close()
        worker.sock = None

        if worker.proc is None:
            self._workers.pop(worker.pid)
            self._stopping_workers -= 1
            self._busy_workers -= 1
            self._maybe_send_task_to_free_worker()


class _WorkerRunner:
    """Implements the event loop in the worker process (receive-execute-reply)."""

    __slots__ = ('_logging_level', '_recv_buf', '_recv_buf_size', '_server_sock', '_shutdown')

    def __init__(self, recv_buf_size: int, logging_level: int, server_sock: socket.socket):
        assert server_sock.getblocking()
        self._recv_buf_size = recv_buf_size
        self._logging_level = logging_level
        self._server_sock = server_sock
        self._shutdown: bool = False
        self._recv_buf: bytearray  # initialized later, in the child process

    def _init_in_child_process(self) -> None:
        sigmonitor.init(sigint=False, sigchld=True)

        root = logging.getLogger()
        root.setLevel(self._logging_level)
        root.handlers.clear()
        root.addHandler(_LogSender(self._server_sock))

        self._recv_buf = bytearray(self._recv_buf_size)

    def run(self) -> None:
        self._init_in_child_process()
        with self._server_sock:
            with selectors.DefaultSelector() as selector:
                selector.register(self._server_sock, selectors.EVENT_READ, data=(self._handle_server_sock_fd, ()))
                for sock in (sigmonitor.get_wakeup_sock(), sigmonitor.get_sigintterm_sock()):
                    selector.register(sock, selectors.EVENT_READ, data=(sigmonitor.handle_readable_wakeup_fd, (sock,)))
                # Receive and execute incoming tasks - until stopped by a signal or the socket closure.
                while not self._shutdown:
                    sigmonitor.maybe_raise_exc()
                    events = selector.select()
                    for selector_key, _event_mask in events:
                        f: Callable
                        args: tuple
                        f, args = selector_key.data
                        f(*args)

    def _handle_server_sock_fd(self) -> None:
        # Receive a task.
        if not (msg := self._recv()):
            self._shutdown = True
            return
        f, args = msg

        # Execute.
        try:
            result = f(*args)
            tp = _MarshalledType.TASK_RESULT.value
        except Exception as exc:
            result = exc
            tp = _MarshalledType.TASK_ERROR.value
        sigmonitor.maybe_raise_exc()

        # Reply.
        result_msg = _marshal(tp, result)
        try:
            self._server_sock.sendall(result_msg)
        except OSError:
            self._shutdown = True

    def _recv(self) -> tuple[Callable, tuple] | None:
        ntotal = 0
        while True:
            sigmonitor.maybe_raise_exc()

            if ntotal == len(self._recv_buf):  # double the buffer on exhaustion
                self._recv_buf.extend(b'\0' * ntotal)

            try:
                nchunk = self._server_sock.recv_into(memoryview(self._recv_buf)[ntotal:])
            except OSError:
                return None
            if not nchunk:
                return None
            ntotal += nchunk

            if parsed := _try_unmarshal(memoryview(self._recv_buf)[:ntotal]):
                marshalling_type, payload, tail = parsed
                assert marshalling_type == _MarshalledType.TASK.value
                assert not tail
                f, args = pickle.loads(payload)
                return f, args


def _create_worker(recv_buf_size: int) -> _StagedWorker:
    # Create sockets. The server's end is nonblocking to allow efficient multiplexing. The worker's end is OK to be
    # blocking, since it only handles one task at a time.
    sock, child_sock = socket.socketpair()
    sock.setblocking(False)
    child_sock.setblocking(True)

    worker_runner = _WorkerRunner(
        recv_buf_size=recv_buf_size,
        logging_level=logging.getLogger().getEffectiveLevel(),
        server_sock=child_sock,
    )
    proc = multiprocessing.Process(target=worker_runner.run)
    proc.start()
    child_sock.close()
    assert proc.pid is not None
    return _StagedWorker(pid=proc.pid, proc=proc, sock=sock)


def _marshal(marshalling_type: int, value: Any) -> memoryview:
    """Creates wire bytes, encoding the pickled value as well as its length and type."""
    global _pickler
    global _pickler_bytes_io
    if _pickler is None:
        _pickler_bytes_io = io.BytesIO()
        _pickler = multiprocessing.reduction.ForkingPickler(_pickler_bytes_io)
    else:
        _pickler_bytes_io.seek(0)
        _pickler_bytes_io.truncate(0)
        _pickler.clear_memo()
    _pickler_bytes_io.write(_HEADER_STUB)
    _pickler.dump(value)
    view = _pickler_bytes_io.getbuffer()
    npayload = len(view) - _HEADER_SIZE
    _header_struct.pack_into(view, 0, marshalling_type, npayload)
    return view


def _try_unmarshal(blob: memoryview) -> tuple[int, memoryview, memoryview] | None:
    """Decodes the wire bytes into the type and the pickled bytes; also returns the remaining tail."""
    n = len(blob)
    if n < _HEADER_SIZE:
        return None
    tp, npayload = _header_struct.unpack_from(blob[:_HEADER_SIZE])
    ntotal = _HEADER_SIZE + npayload
    if n < ntotal:
        return None
    return (tp, blob[_HEADER_SIZE:ntotal], blob[ntotal:])


def _assign_future_exception(future: Future, exc: BaseException) -> None:
    with contextlib.suppress(concurrent.futures.InvalidStateError):  # cover against concurrent changes
        future.set_exception(exc)


class _LogSender(logging.Handler):
    """Sends all logs from a worker to the server process via a socket.

    This is used instead of letting the workers write to stderr, in order to avoid spurious errors printed by canceled
    workers.
    """

    def __init__(self, server_sock: socket.socket):
        super().__init__()
        self._server_sock = server_sock

    def emit(self, record: logging.LogRecord) -> None:
        prepared = self._prepare_record(record)
        view = _marshal(_MarshalledType.LOG.value, prepared)
        try:
            self._server_sock.sendall(view)
        except OSError:
            # most likely it's due to the main process closing the connection and about to terminate us
            pass

    def _prepare_record(self, record: logging.LogRecord) -> logging.LogRecord:
        """Formats the message, removes unpickleable fields and those not necessary for formatting."""
        formatted = self.format(record)
        record = copy(record)
        record.message = formatted
        record.msg = formatted
        record.args = None
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        return record
