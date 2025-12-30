from __future__ import annotations

import concurrent.futures
import contextlib
import heapq
import io
import logging
import multiprocessing
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
from collections.abc import Callable, Collection, Sequence
from concurrent.futures import Future
from copy import copy
from datetime import datetime
from typing import Any

from cvise.utils import sigmonitor


_DEFAULT_RECV_BUF_SIZE = 65536


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
        self._pool_runner_thread.join(60)
        if self._pool_runner_thread.is_alive():
            logging.warning('Failed to stop process pool thread')
        self._exit_stack.__exit__(exc_type, exc_val, exc_tb)

        # Clear leftovers:

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


class _Task:
    __slots__ = ('f', 'args', 'future', 'timeout')

    def __init__(self, f: Callable, args: Sequence[Any], future: Future, timeout: float):
        self.f = f
        self.args = args
        self.future = future
        self.timeout = timeout


class _MarshalledTask:
    __slots__ = ('wire_bytes', 'future', 'timeout')

    def __init__(self, wire_bytes: bytes, future: Future, timeout: float):
        self.wire_bytes = wire_bytes
        self.future = future
        self.timeout = timeout


class _Notifier:
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
    __slots__ = ('tasks',)

    def __init__(self):
        super().__init__()
        self.tasks: deque[_Task] = deque()

    def enqueue(self, task: _Task) -> None:
        self.tasks.append(task)
        self.notify()


class _StagedWorker:
    """A newly created worker, as returned by _WorkerCreator."""

    __slots__ = ('pid', 'proc', 'sock')

    def __init__(self, pid: int, proc: multiprocessing.Process, sock: socket.socket):
        self.pid = pid
        self.proc = proc
        self.sock = sock


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


class _TimeoutsHeapNode:
    __slots__ = ('when', 'task_future')

    def __init__(self, when: float, task_future: Future):
        self.when = when
        self.task_future = task_future

    def __lt__(self, other: _TimeoutsHeapNode) -> bool:
        return self.when < other.when


class _Worker:
    __slots__ = ('pid', 'proc', 'sock', 'task_future', 'stopping', 'partial_recv')

    def __init__(self, pid: int, proc: multiprocessing.Process, sock: socket.socket):
        self.pid = pid
        self.proc: multiprocessing.Process | None = proc
        self.sock: socket.socket | None = sock
        self.task_future: Future | None = None
        self.stopping: bool = False
        self.partial_recv: bytearray | None = None


class _WorkerCreator:
    def __init__(self, recv_buf_size: int, staged_worker_inbox: _StagedWorkerInbox):
        self._recv_buf_size = recv_buf_size
        self._staged_worker_inbox = staged_worker_inbox
        self._cond = threading.Condition()
        self._scheduled_count = 0
        self._shutdown = False
        self._thread = threading.Thread(target=self._worker_creator_thread_main, name='WorkerCreator', daemon=True)
        self._thread.start()

    def schedule_creation(self, cnt: int) -> None:
        with self._cond:
            self._scheduled_count += cnt
            self._cond.notify()

    def notify_shutdown(self) -> None:
        with self._cond:
            self._shutdown = True
            self._cond.notify()

    def join_thread(self) -> None:
        self._thread.join(60)
        if self._thread.is_alive():
            logging.warning('Failed to stop WorkerCreator thread')

    def _worker_creator_thread_main(self):
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
                sock, child_sock = socket.socketpair()
                sock.setblocking(False)
                child_sock.setblocking(True)
                proc = multiprocessing.Process(
                    target=_WorkerRunner(self._recv_buf_size, logging.getLogger().getEffectiveLevel(), child_sock).run
                )
                proc.start()
                child_sock.close()
                assert proc.pid is not None
                self._staged_worker_inbox.add(_StagedWorker(pid=proc.pid, proc=proc, sock=sock))


class _PoolRunner:
    """Implements the process pool event loop; is expected to be used on a background thread."""

    __slots__ = (
        '_abort_inbox',
        '_busy_workers',
        '_event_selector',
        '_future_to_worker_pid',
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
        self._event_selector = selectors.DefaultSelector()
        self._event_selector.register(
            self._shutdown_notifier.read_sock, selectors.EVENT_READ, data=(self._handle_shutdown_fd, ())
        )
        self._event_selector.register(
            self._task_queue.read_sock, selectors.EVENT_READ, data=(self._handle_task_queue_fd, ())
        )
        self._event_selector.register(
            self._staged_worker_inbox.read_sock, selectors.EVENT_READ, data=(self._handle_staged_worker_inbox_fd, ())
        )
        self._event_selector.register(
            self._abort_inbox.read_sock, selectors.EVENT_READ, data=(self._handle_abort_inbox_fd, ())
        )
        self._recv_buf = bytearray(self._recv_buf_size)
        self._shutdown = False
        self._future_to_worker_pid: dict[Future, int] = {}

    def run(self) -> None:
        self._worker_creator.schedule_creation(self._max_active_workers)
        while not self._shutdown:
            self._do_step()

    def _do_step(self) -> None:
        # print(f'[{datetime.now()}] step', file=sys.stderr)
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
        # assert sum(1 for w in self._workers.values() if w.task_future is not None) <= self._max_active_workers

        self._mark_timed_out_tasks()
        while self._task_queue.tasks:
            self._select_and_process_fds(wait=False)
            self._maybe_send_or_marshal_single_task()
        self._select_and_process_fds(wait=True)

    def shut_down(self) -> None:
        # print(f'[{datetime.now()}] poolrunner.shut_down: event_selector.close()', file=sys.stderr)
        self._event_selector.close()
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

        # print(f'[{datetime.now()}] select_and_process_fds: timeout={poll_timeout} wait={wait}', file=sys.stderr)
        events = self._event_selector.select(timeout=poll_timeout)
        # print(f'[{datetime.now()}] select_and_process_fds: events={len(events)}', file=sys.stderr)
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
            self._event_selector.register(
                staged.sock, selectors.EVENT_READ, data=(self._on_readable_worker_sock, (staged.pid,))
            )
            self._send_task_or_mark_free(worker)

    def _handle_abort_inbox_fd(self) -> None:
        self._abort_inbox.eat_notifications(self._recv_buf)

        # print(f'[{datetime.now()}] event: aborts={len(aborts)} tasks={len(self._task_queue.tasks)} pickled_tasks={len(self._marshalled_task_queue)}', file=sys.stderr)

        workers_to_create = 0
        for task_future in self._abort_inbox.take_all():
            worker_pid = self._future_to_worker_pid.pop(task_future, None)
            if worker_pid is None:
                # the task hasn't started or already finished
                continue
            self._trigger_worker_stop(worker_pid)
            workers_to_create += 1
        if workers_to_create:
            self._worker_creator.schedule_creation(workers_to_create)

    def _on_readable_worker_sock(self, pid: int) -> None:
        worker = self._workers[pid]
        assert worker.sock is not None
        # print(f'[{datetime.now()}] on_readable_worker_conn: pid={pid}', file=sys.stderr)

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
            self._handle_worker_conn_eof(worker)
            return
        # print(f'read from pid={worker.pid} len={bytes_received} partial_recv={None if worker.partial_recv is None else len(worker.partial_recv)}', file=sys.stderr)
        if not chunk:
            self._handle_worker_conn_eof(worker)
            return

        begin = 0
        end = len(view)
        while begin < end:
            if end - begin < 5:
                break
            sz: int = view[begin : begin + 4].cast('i')[0]
            assert sz >= 0
            if begin + 5 + sz > end:
                break
            tp = view[begin + 4]
            raw_message = view[begin + 5 : begin + 5 + sz]
            begin += 5 + sz
            assert begin <= end
            # print(f'handling msg from pid={worker.pid} packet_size={4+sz} payload_size={sz}', file=sys.stderr)

            if worker.stopping:
                continue  # worker shutdown produces spurious messages; we simply wait till EOF

            if tp == 0:  # task result
                future_to_resolve = worker.task_future
                assert future_to_resolve is not None
                tracked_pid = self._future_to_worker_pid.pop(future_to_resolve)
                assert tracked_pid == pid
                worker.task_future = None
                self._busy_workers -= 1
                assert self._busy_workers >= 0
                self._send_task_or_mark_free(worker)  # eagerly send a new task
                message = pickle.loads(raw_message)
                if isinstance(message, Exception):
                    _assign_future_exception(future_to_resolve, message)
                else:
                    _assign_future_result(future_to_resolve, message)
            elif tp == 1:  # log
                message = pickle.loads(raw_message)
                # Ignore logs if the future is already switched to "done" - these are likely spurious errors because the main process started deleting task's files.
                if worker.task_future is None or not worker.task_future.done():
                    record: logging.LogRecord = message
                    logger = logging.getLogger(record.name)
                    logger.handle(record)
            else:
                assert False, f'Unknown message type {tp}'

        if begin == end:
            worker.partial_recv = None
        elif worker.partial_recv is None:
            worker.partial_recv = bytearray(view[begin:end])

    def _on_worker_proc_fd_joinable(self, worker_pid: int) -> None:
        worker = self._workers[worker_pid]
        assert worker.proc is not None
        self._event_selector.unregister(worker.proc.sentinel)
        worker.proc.join()
        # print(f'joined pid={worker.pid}', file=sys.stderr)
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
                wire_bytes = _marshal_task((task.f, task.args))
            case _MarshalledTask():
                wire_bytes = memoryview(task.wire_bytes)
        # print(f'sending task to pid={worker.pid} len={len(packet)}', file=sys.stderr)
        worker.sock.sendall(wire_bytes)
        assert worker.task_future is None
        worker.task_future = task.future
        self._busy_workers += 1
        timeout_when = time.monotonic() + task.timeout
        heapq.heappush(self._timeouts_heap, _TimeoutsHeapNode(when=timeout_when, task_future=task.future))
        self._future_to_worker_pid[task.future] = worker.pid
        # print(f'[{datetime.now()}] {"send_task" if isinstance(task, _Task) else "send_pickled_task"} task to pid={worker.pid}', file=sys.stderr)

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
        # print(f'[{datetime.now()}] trigger_worker_stop: pid={worker.pid}', file=sys.stderr)
        assert worker.proc is not None
        worker.stopping = True
        worker.task_future = None
        worker.proc.terminate()
        self._stopping_workers += 1
        assert worker.sock is not None
        # observe when the process will become join'able
        self._event_selector.register(
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
            packet = bytes(_marshal_task((task.f, task.args)))
            self._marshalled_task_queue.append(
                _MarshalledTask(wire_bytes=packet, future=task.future, timeout=task.timeout)
            )
            # print(f'[{datetime.now()}] pickle_one_task: tasks={len(self._task_queue.tasks)} pickled_tasks={len(self._marshalled_task_queue)}', file=sys.stderr)

    def _mark_timed_out_tasks(self) -> None:
        now = time.monotonic()
        while self._timeouts_heap:
            node = self._timeouts_heap[0]
            if node.when > now and not node.task_future.done():
                break
            heapq.heappop(self._timeouts_heap)
            if not node.task_future.done():
                _assign_future_exception(node.task_future, TimeoutError('Job timed out'))
                self._abort_inbox.add(node.task_future)

    def _handle_worker_conn_eof(self, worker: _Worker) -> None:
        assert worker.sock is not None
        if worker.task_future is not None:
            if not worker.task_future.done():
                _assign_future_exception(worker.task_future, ProcessPoolError(f'Worker {worker.pid} died'))
            tracked_pid = self._future_to_worker_pid.pop(worker.task_future)
            assert tracked_pid == worker.pid
            worker.task_future = None
        self._event_selector.unregister(worker.sock)

        worker.sock.close()
        worker.sock = None

        # print(f'[{datetime.now()}] worker_conn_eof: pid={worker.pid}', file=sys.stderr)
        if worker.proc is None:
            self._workers.pop(worker.pid)
            self._stopping_workers -= 1
            self._busy_workers -= 1
            self._maybe_send_task_to_free_worker()


class _WorkerRunner:
    __slots__ = ('_logging_level', '_recv_buf', '_recv_buf_size', '_shutdown', '_sock_to_server')

    def __init__(self, recv_buf_size: int, logging_level: int, sock_to_server: socket.socket):
        self._recv_buf_size = recv_buf_size
        self._logging_level = logging_level
        self._sock_to_server = sock_to_server
        self._shutdown: bool = False
        self._recv_buf: bytearray  # to be initialized in the child process

    def _init_in_child_process(self) -> None:
        sigmonitor.init(sigmonitor.Mode.RAISE_EXCEPTION_ON_DEMAND, sigint=False, sigchld=True)

        root = logging.getLogger()
        root.setLevel(self._logging_level)
        root.handlers.clear()
        root.addHandler(_LogSender(self._sock_to_server))

        self._recv_buf = bytearray(self._recv_buf_size)

    def run(self) -> None:
        self._init_in_child_process()
        # print(f'worker[{os.getpid()}]: started', file=sys.stderr)
        # with sigmonitor.scoped_mode(sigmonitor.Mode.RAISE_EXCEPTION_ON_DEMAND):
        with self._sock_to_server:
            with selectors.DefaultSelector() as event_selector:
                event_selector.register(
                    self._sock_to_server, selectors.EVENT_READ, data=(self._handle_sock_to_server_fd, ())
                )
                wakeup_fd = sigmonitor.get_wakeup_fd()
                event_selector.register(
                    wakeup_fd, selectors.EVENT_READ, data=(self._handle_sigmonitor_wakeup_fd, (wakeup_fd,))
                )
                # Handle incoming tasks in an infinite loop (until stopped by a signal or the socket closure).
                while not self._shutdown:
                    events = event_selector.select()
                    for selector_key, _event_mask in events:
                        handler: Callable
                        args: tuple
                        handler, args = selector_key.data
                        handler(*args)

    def _handle_sigmonitor_wakeup_fd(self, sigmonitor_wakeup_fd: int) -> None:
        sigmonitor.eat_wakeup_fd_notifications()
        sigmonitor.maybe_retrigger_action()

    def _handle_sock_to_server_fd(self) -> None:
        sz: int | None = None
        total = 0
        while True:
            if total == len(self._recv_buf):
                self._recv_buf.extend(b'\0' * len(self._recv_buf))
            view = memoryview(self._recv_buf)
            try:
                nbytes = self._sock_to_server.recv_into(view[total:])
            except (BlockingIOError, TimeoutError):
                raise  # we expect the file descriptor to be in non-blocking mode
            except OSError:
                self._shutdown = True
                return
            if not nbytes:
                self._shutdown = True
                return
            total += nbytes
            if sz is None:
                assert total >= 4
                sz = view[:4].cast('i')[0]
            if total >= 5 + sz:
                break
            view.release()
        assert total == 5 + sz
        assert view[4] == 0  # tp
        # print(f'worker[{os.getpid()}]: received sz={sz}', file=sys.stderr)
        raw_message = view[5:total]
        view.release()
        f, args = pickle.loads(raw_message)
        raw_message.release()
        # print(f'worker[{os.getpid()}]: task begin', file=sys.stderr)
        try:
            result = f(*args)
        except Exception as e:
            result = e
        # print(f'worker[{os.getpid()}]: task end', file=sys.stderr)
        sigmonitor.maybe_retrigger_action()
        wire_bytes = _marshal_task(result)
        # print(f'worker[{os.getpid()}]: task reply size={len(packet)}', file=sys.stderr)
        try:
            self._sock_to_server.sendall(wire_bytes)
        except BlockingIOError:
            raise  # we expect the file descriptor to be in non-blocking mode
        except OSError:
            self._shutdown = True
        wire_bytes.release()


_pickler_bytes_io: io.BytesIO | None = None
_pickler: multiprocessing.reduction.ForkingPickler | None = None
_packer = struct.Struct('i')


def _marshal_task(value: Any) -> memoryview:
    global _pickler
    global _pickler_bytes_io
    if _pickler is None:
        _pickler_bytes_io = io.BytesIO()
        _pickler = multiprocessing.reduction.ForkingPickler(_pickler_bytes_io)
    else:
        _pickler_bytes_io.seek(0)
        _pickler_bytes_io.truncate(0)
        _pickler.clear_memo()
    _pickler_bytes_io.write(b'\0\0\0\0\0')  # length, tp
    _pickler.dump(value)
    view = _pickler_bytes_io.getbuffer()
    _packer.pack_into(view, 0, len(view) - 5)
    return view


def _assign_future_result(future: Future, result: Any) -> None:
    if future.done():
        return
    with contextlib.suppress(concurrent.futures.InvalidStateError):  # cover against concurrent changes
        future.set_result(result)
        # print(f'set_result', file=sys.stderr)


def _assign_future_exception(future: Future, exc: BaseException) -> None:
    if future.done():
        return
    with contextlib.suppress(concurrent.futures.InvalidStateError):  # cover against concurrent changes
        future.set_exception(exc)
        # print(f'set_exception', file=sys.stderr)


class _LogSender(logging.Handler):
    """Sends all logs into the multiprocessing connection."""

    def __init__(self, sock_to_server: socket.socket):
        super().__init__()
        self._sock_to_server = sock_to_server

    def emit(self, record: logging.LogRecord) -> None:
        prepared = self._prepare_record(record)
        buf = io.BytesIO()
        buf.write(b'\0\0\0\0\1')
        multiprocessing.reduction.ForkingPickler(buf).dump(prepared)
        view = buf.getbuffer()
        struct.Struct('i').pack_into(view, 0, len(view) - 5)
        try:
            self._sock_to_server.sendall(view)
        except BlockingIOError:
            raise  # we expect the file descriptor to be in non-blocking mode
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
