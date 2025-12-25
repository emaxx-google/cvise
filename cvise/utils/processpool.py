from __future__ import annotations

import concurrent.futures
import contextlib
import heapq
import io
import logging
import msgspec
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
import traceback
import time
from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime
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

        new_workers_event_read_socket, new_workers_event_write_socket = socket.socketpair()
        new_workers_event_read_socket.setblocking(False)
        new_workers_event_write_socket.setblocking(False)

        self._shared_state = _SharedState(
            event_read_socket=event_read_socket,
            event_write_socket=event_write_socket,
            lock=threading.Lock(),
            shutdown=False,
            task_queue=deque(),
            scheduled_aborts=[],
            new_workers=[],
            new_workers_event_read_socket=new_workers_event_read_socket,
            new_workers_event_write_socket=new_workers_event_write_socket,
        )
        self._worker_creator = _WorkerCreator(self._shared_state)
        self._pool_runner = _PoolRunner(max_active_workers, self._shared_state, worker_creator=self._worker_creator)
        self._pool_runner_thread = threading.Thread(target=self._pool_thread_main, name='PoolThread')

    def __enter__(self):
        self._pool_runner_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
        self._worker_creator.join()
        self._pool_runner_thread.join(60)
        if self._pool_runner_thread.is_alive():
            logging.warning('Failed to stop process pool thread')
            assert 0
        self._shared_state.close()

    def stop(self) -> None:
        """Initiates cancellation of pending tasks and termination of already running ones."""
        self._shared_state.notify_shutdown()
        self._worker_creator.notify_shutdown()

    def schedule(self, f: Callable, args: Sequence[Any], timeout: float) -> Future:
        future = Future()
        task = _Task(f=f, args=args, future=future, timeout=timeout)
        if not self._shared_state.enqueue_task(task):
            future.cancel()  # we're in shutdown
        return future

    def _pool_thread_main(self) -> None:
        self._pool_runner.run()
        self._pool_runner.shut_down()


class _Task(msgspec.Struct):
    f: Callable
    args: Sequence[Any]
    future: Future
    timeout: float


class _PickledTask(msgspec.Struct):
    packet: bytes
    future: Future
    timeout: float


class _ScheduledAbort(msgspec.Struct):
    worker_pid: int
    task_future: Future


class _SharedState(msgspec.Struct):
    """State shared between the pool runner thread and other threads.

    Most importantly, this is used to deliver newly scheduled tasks and task cancellations.
    """

    event_read_socket: socket.socket
    event_write_socket: socket.socket
    lock: threading.Lock
    shutdown: bool
    task_queue: deque[_Task]
    scheduled_aborts: list[_ScheduledAbort]
    new_workers: list[tuple[multiprocessing.Process, multiprocessing.connection.Connection]]
    new_workers_event_read_socket: socket.socket
    new_workers_event_write_socket: socket.socket

    def close(self) -> None:
        self.event_read_socket.close()
        self.event_write_socket.close()
        self.new_workers_event_read_socket.close()
        self.new_workers_event_write_socket.close()

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

    def enqueue_new_worker(
        self,
        proc: multiprocessing.Process,
        conn: multiprocessing.connection.Connection,
    ) -> None:
        with self.lock:
            if self.shutdown:
                return  # all pending & active tasks are canceled anyway during shutdown
            self.new_workers.append((proc, conn))
            should_notify = len(self.new_workers) == 1
        if should_notify:
            self.new_workers_event_write_socket.send(b'\0')

    def _should_notify(self) -> bool:
        return len(self.task_queue) + len(self.scheduled_aborts) == 1

    def _notify(self) -> None:
        self.event_write_socket.send(b'\0')


class _TimeoutsHeapNode(msgspec.Struct):
    when: float
    task_future: Future

    def __lt__(self, other: _TimeoutsHeapNode) -> bool:
        return self.when < other.when


class _Worker(msgspec.Struct):
    pid: int
    process: multiprocessing.Process | None
    conn: multiprocessing.connection.Connection | None
    sock: socket.socket | None
    active_task_future: Future | None
    stopping: bool = False


class _WorkerCreator:
    def __init__(self, shared_state: _SharedState):
        self._shared_state = shared_state
        self._cond = threading.Condition()
        self._scheduled_count = 0
        self._shutdown = False
        self._thread = threading.Thread(target=self._worker_creator_thread_main, name='WorkerCreator', daemon=True)
        self._thread.start()

    def schedule_creation(self):
        with self._cond:
            self._scheduled_count += 1
            self._cond.notify()

    def notify_shutdown(self):
        with self._cond:
            self._shutdown = True
            self._cond.notify()

    def join(self):
        self._thread.join(60)
        if self._thread.is_alive():
            logging.warning('Failed to stop WorkerCreator thread')

    def _worker_creator_thread_main(self):
        while True:
            with self._cond:
                if self._shutdown:
                    return
                if not self._scheduled_count:
                    self._cond.wait()
                    continue
                self._scheduled_count -= 1

            conn, child_conn = multiprocessing.Pipe()
            proc = multiprocessing.Process(
                target=_worker_process_main,
                args=(logging.getLogger().getEffectiveLevel(), child_conn),
            )
            proc.start()
            child_conn.close()
            assert proc.pid is not None
            self._shared_state.enqueue_new_worker(proc, conn)


class _PoolRunner:
    """Implements the process pool event loop; is expected to be used on a background thread."""

    __slots__ = (
        '_event_selector',
        '_free_worker_pids',
        '_max_active_workers',
        '_task_queue',
        '_pickled_task_queue',
        '_read_buf',
        '_shared_state',
        '_shutdown',
        '_stopping_workers',
        '_timeouts_heap',
        '_worker_creator',
        '_workers',
    )

    def __init__(self, max_worker_count: int, shared_state: _SharedState, worker_creator: _WorkerCreator):
        self._max_active_workers = max_worker_count
        self._shared_state = shared_state
        self._worker_creator = worker_creator
        self._workers: dict[int, _Worker] = {}
        self._task_queue: deque[_Task] = deque()
        self._pickled_task_queue: deque[_PickledTask] = deque()
        self._free_worker_pids: deque[int] = deque()
        self._stopping_workers: int = 0
        self._timeouts_heap: list[_TimeoutsHeapNode] = []
        self._event_selector = selectors.DefaultSelector()
        self._event_selector.register(
            self._shared_state.event_read_socket, selectors.EVENT_READ, data=(self._on_readable_event_read_socket, None)
        )
        self._read_buf = bytearray(1000000)
        self._event_selector.register(
            self._shared_state.new_workers_event_read_socket,
            selectors.EVENT_READ,
            data=(self._on_readable_new_workers_event_read_socket, None),
        )
        self._shutdown = False

    def run(self) -> None:
        for _ in range(self._max_active_workers):
            self._worker_creator.schedule_creation()
        while not self._shutdown:
            self._do_step()

    def _do_step(self) -> None:
        # print(f'[{datetime.now()}] step', file=sys.stderr)
        assert len(self._free_worker_pids) <= len(self._workers)
        assert 0 <= len(self._workers) - self._stopping_workers, (
            f'workers={len(self._workers)} free={len(self._free_worker_pids)} stopping={self._stopping_workers} max={self._max_active_workers}'
        )
        assert 0 <= len(self._workers) - len(self._free_worker_pids) <= self._max_active_workers, (
            f'workers={len(self._workers)} free={len(self._free_worker_pids)} max={self._max_active_workers}'
        )
        # assert sum(1 for w in self._workers.values() if w.active_task_future is not None) <= self._max_active_workers

        self._mark_timed_out_tasks()
        if self._task_queue:
            while True:
                self._pump_file_descriptors(wait=False)
                if self._task_queue:
                    self._pickle_one_task()
                else:
                    break
        self._pump_file_descriptors(wait=True)

    def shut_down(self) -> None:
        # print(f'[{datetime.now()}] poolrunner.shut_down: event_selector.close()', file=sys.stderr)
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
            if worker.conn:
                while True:
                    try:
                        worker.conn.recv_bytes()
                    except (EOFError, OSError):
                        break
                worker.sock.detach()
                worker.conn.close()
            if worker.active_task_future:
                worker.active_task_future.cancel()
        for worker in self._workers.values():
            if worker.process is not None:
                worker.process.join()
        self._workers.clear()

    def _pump_file_descriptors(self, wait: bool) -> None:
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
            handler, arg = selector_key.data
            handler(arg)

    def _on_readable_event_read_socket(self, _unused) -> None:
        self._shared_state.event_read_socket.recv(10)  # drain the notification(s)

        with self._shared_state.lock:
            self._shutdown = self._shared_state.shutdown
            added_tasks = bool(self._shared_state.task_queue)
            if added_tasks:
                self._task_queue.extend(self._shared_state.task_queue)
                self._shared_state.task_queue.clear()
            aborts = self._shared_state.scheduled_aborts
            self._shared_state.scheduled_aborts = []

        # print(f'[{datetime.now()}] event: added_tasks={added_tasks} aborts={len(aborts)} tasks={len(self._task_queue)} pickled_tasks={len(self._pickled_task_queue)}', file=sys.stderr)

        if added_tasks and not self._pickled_task_queue:
            cnt = min(
                len(self._free_worker_pids),
                self._max_active_workers - len(self._workers) + len(self._free_worker_pids),
                len(self._task_queue),
            )
            for _ in range(cnt):
                worker_pid = self._free_worker_pids.popleft()
                task = self._task_queue.popleft()
                self._send_task(task, self._workers[worker_pid])

        for abort in aborts:
            self._trigger_worker_stop(abort.worker_pid, abort.task_future)
            self._worker_creator.schedule_creation()

    def _on_readable_new_workers_event_read_socket(self, _unused) -> None:
        self._shared_state.new_workers_event_read_socket.recv(10)  # drain the notification(s)

        with self._shared_state.lock:
            new_workers = self._shared_state.new_workers
            self._shared_state.new_workers = []

        for proc, conn in new_workers:
            assert proc.pid is not None
            assert proc.pid not in self._workers

            os.set_blocking(conn.fileno(), False)
            sock = socket.socket(fileno=conn.fileno(), family=socket.AF_UNIX, type=socket.SOCK_STREAM)
            sock.setblocking(False)
            sock.settimeout(0)

            worker = _Worker(
                pid=proc.pid,
                process=proc,
                conn=conn,
                sock=sock,
                active_task_future=None,
            )
            self._workers[proc.pid] = worker
            self._event_selector.register(conn, selectors.EVENT_READ, data=(self._on_readable_worker_conn, proc.pid))
            self._send_task_or_mark_free(worker)

    def _on_readable_worker_conn(self, pid: int) -> None:
        worker = self._workers[pid]
        assert worker.conn is not None
        view = memoryview(self._read_buf)
        # print(f'[{datetime.now()}] on_readable_worker_conn: pid={pid}', file=sys.stderr)
        begin = 0
        end = 0
        while end == 0 or begin < end:
            try:
                nbytes = worker.sock.recv_into(view[end:])
            except (BlockingIOError, TimeoutError):
                raise
            except OSError:
                self._handle_worker_conn_eof(worker)
                return
            # print(f'read from pid={worker.pid} len={nbytes} begin={begin} end={end}', file=sys.stderr)
            if not nbytes:
                self._handle_worker_conn_eof(worker)
                return
            end += nbytes
            if end - begin < 5:
                continue
            sz: int = view[begin : begin + 4].cast('i')[0]
            assert sz >= 0
            if begin + 5 + sz > end:
                continue
            tp = view[begin + 4]
            raw_message = view[begin + 5 : begin + 5 + sz]
            begin += 5 + sz
            assert begin <= end
            # print(f'handling msg from pid={worker.pid} packet_size={4+sz} payload_size={sz}', file=sys.stderr)

            if worker.stopping:
                continue  # worker shutdown produces spurious messages; we simply wait till EOF

            if tp == 0:  # task result
                original_future = worker.active_task_future
                assert original_future is not None
                worker.active_task_future = None
                self._send_task_or_mark_free(worker)  # eagerly send a new task
                message = pickle.loads(raw_message)
                if isinstance(message, Exception):
                    _assign_future_exception(original_future, message)
                else:
                    _assign_future_result(original_future, message)
            elif tp == 1:  # log
                message = pickle.loads(raw_message)
                assert mplogging.maybe_handle_message_from_worker(message)
            else:
                assert False, f'Unknown message type {tp}'

    def _on_worker_proc_fd_joinable(self, worker_pid: int) -> None:
        worker = self._workers[worker_pid]
        assert worker.process is not None
        self._event_selector.unregister(worker.process.sentinel)
        worker.process.join()
        # print(f'joined pid={worker.pid}', file=sys.stderr)
        worker.process = None
        if worker.conn is None:
            self._workers.pop(worker.pid)
            self._stopping_workers -= 1
            assert self._stopping_workers >= 0
            self._maybe_send_task_to_free_worker()

    def _send_task(self, task: _Task | _PickledTask, worker: _Worker) -> None:
        assert worker.process is not None
        task.future.add_done_callback(lambda future: self._on_future_resolved(worker.pid, future))
        assert worker.conn is not None
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
        # print(f'[{datetime.now()}] {"send_task" if isinstance(task, _Task) else "send_pickled_task"} task to pid={worker.pid}', file=sys.stderr)

    def _send_task_or_mark_free(self, worker: _Worker) -> None:
        if len(self._workers) - len(self._free_worker_pids) <= self._max_active_workers:
            if self._pickled_task_queue:
                self._send_task(self._pickled_task_queue.popleft(), worker)
                return
            if self._task_queue:
                self._send_task(self._task_queue.popleft(), worker)
                return
        self._free_worker_pids.append(worker.pid)

    def _maybe_send_task_to_free_worker(self) -> None:
        if self._free_worker_pids and (self._task_queue or self._pickled_task_queue):
            worker_pid = self._free_worker_pids.popleft()
            self._send_task((self._pickled_task_queue or self._task_queue).popleft(), self._workers[worker_pid])

    def _trigger_worker_stop(self, worker_pid: int, task_future: Future) -> None:
        if worker_pid not in self._workers:
            return  # already died
        worker = self._workers[worker_pid]
        if worker.active_task_future != task_future:
            return  # a new task started already
        # print(f'[{datetime.now()}] trigger_worker_stop: pid={worker.pid}', file=sys.stderr)
        assert worker.process is not None
        worker.stopping = True
        worker.active_task_future = None
        worker.process.terminate()
        self._stopping_workers += 1
        assert worker.conn is not None
        # observe when the process will become join'able
        self._event_selector.register(
            worker.process.sentinel, selectors.EVENT_READ, data=(self._on_worker_proc_fd_joinable, worker_pid)
        )

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
        # print(f'[{datetime.now()}] handle_task_result: pid={worker.pid}', file=sys.stderr)
        if isinstance(result_message, Exception):
            _assign_future_exception(worker.active_task_future, result_message)
        else:
            _assign_future_result(worker.active_task_future, result_message)
        worker.active_task_future = None
        return True

    def _pickle_one_task(self) -> None:
        task = self._task_queue.popleft()
        packet = bytes(_create_packet((task.f, task.args)))
        self._pickled_task_queue.append(_PickledTask(packet=packet, future=task.future, timeout=task.timeout))
        # print(f'[{datetime.now()}] pickle_one_task: tasks={len(self._task_queue)} pickled_tasks={len(self._pickled_task_queue)}', file=sys.stderr)

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

    def _handle_worker_conn_eof(self, worker: _Worker) -> None:
        assert worker.conn is not None
        if worker.active_task_future is not None:
            if not worker.active_task_future.done():
                _assign_future_exception(worker.active_task_future, ProcessPoolError(f'Worker {worker.pid} died'))
            worker.active_task_future = None
        self._event_selector.unregister(worker.conn)

        worker.sock.detach()
        worker.sock = None
        worker.conn.close()
        worker.conn = None

        # print(f'[{datetime.now()}] worker_conn_eof: pid={worker.pid}', file=sys.stderr)
        if worker.process is None:
            self._workers.pop(worker.pid)
            self._stopping_workers -= 1
            self._maybe_send_task_to_free_worker()


def _worker_process_main(
    logging_level: int,
    conn: multiprocessing.connection.Connection,
) -> None:
    # print(f'worker[{os.getpid()}]: started', file=sys.stderr)
    sigmonitor.init(sigmonitor.Mode.QUICK_EXIT, sigint=False, sigchld=True)
    mplogging.init_in_worker(logging_level=logging_level, server_conn=conn)
    wakeup_fd = sigmonitor.get_wakeup_fd()
    read_buf = bytearray(1000000)
    task_sock = socket.socket(fileno=conn.fileno(), family=socket.AF_UNIX, type=socket.SOCK_STREAM)
    with sigmonitor.scoped_mode(sigmonitor.Mode.RAISE_EXCEPTION_ON_DEMAND):
        try:
            with selectors.DefaultSelector() as event_selector:
                event_selector.register(conn, selectors.EVENT_READ)
                event_selector.register(wakeup_fd, selectors.EVENT_READ)
                # Handle incoming tasks in an infinite loop (until stopped by a signal).
                while True:
                    events = event_selector.select()
                    can_recv = False
                    for selector_key, _event_mask in events:
                        if selector_key.fileobj == wakeup_fd:
                            try:
                                os.read(wakeup_fd, 1024)
                            except (BlockingIOError, TimeoutError):
                                raise
                            except OSError:
                                pass
                            sigmonitor.maybe_retrigger_action()
                        else:
                            can_recv = True
                    if can_recv:
                        sz: int | None = None
                        view = memoryview(read_buf)
                        total = 0
                        while True:
                            try:
                                nbytes = task_sock.recv_into(view[total:])
                            except (BlockingIOError, TimeoutError):
                                raise
                            except OSError:
                                return
                            if not nbytes:
                                return
                            total += nbytes
                            if sz is None:
                                assert total >= 4
                                sz = view[:4].cast('i')[0]
                                if total >= 5 + sz:
                                    break
                        assert total == 5 + sz
                        assert view[4] == 0  # tp
                        # print(f'worker[{os.getpid()}]: received sz={sz}', file=sys.stderr)
                        raw_message = view[5:total]
                        f, args = pickle.loads(raw_message)
                        # print(f'worker[{os.getpid()}]: task begin', file=sys.stderr)
                        try:
                            result = f(*args)
                        except Exception as e:
                            traceback.print_exception(e, file=sys.stderr)
                            result = e
                        # print(f'worker[{os.getpid()}]: task end', file=sys.stderr)
                        sigmonitor.maybe_retrigger_action()
                        packet = _create_packet(result)
                        # print(f'worker[{os.getpid()}]: task reply size={len(packet)}', file=sys.stderr)
                        while packet:
                            try:
                                nbytes = task_sock.send(packet)
                            except (BlockingIOError, TimeoutError):
                                raise
                            except OSError:
                                break
                            packet = packet[nbytes:]
                        packet.release()
        finally:
            # print(f'worker[{os.getpid()}]: dying', file=sys.stderr)
            task_sock.detach()
            conn.close()


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
    _pickler_bytes_io.write(b'\0\0\0\0\0')  # length, tp
    _pickler.dump(value)
    view = _pickler_bytes_io.getbuffer()
    _packer.pack_into(view, 0, len(view) - 5)
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
        # print(f'set_result', file=sys.stderr)


def _assign_future_exception(future: Future, exc: BaseException) -> None:
    if future.done():
        return
    with contextlib.suppress(concurrent.futures.InvalidStateError):  # cover against concurrent changes
        future.set_exception(exc)
        # print(f'set_exception', file=sys.stderr)
