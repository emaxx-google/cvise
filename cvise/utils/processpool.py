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
from datetime import datetime
from typing import Any

from cvise.utils import mplogging, sigmonitor


_RECV_BUF_SIZE = 65536


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

        self._worker_creator = _WorkerCreator(self._staged_worker_inbox)

        self._pool_runner = _PoolRunner(
            max_active_workers=max_active_workers,
            shutdown_notifier=self._shutdown_notifier,
            task_queue=self._task_queue,
            staged_worker_inbox=self._staged_worker_inbox,
            abort_inbox=self._abort_inbox,
            worker_creator=self._worker_creator,
        )
        self._pool_runner_thread = threading.Thread(target=self._pool_thread_main, name='PoolThread', daemon=True)

    def __enter__(self):
        self._pool_runner_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
        self._worker_creator.join()
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


class _PickledTask:
    __slots__ = ('packet', 'future', 'timeout')

    def __init__(self, packet: bytes, future: Future, timeout: float):
        self.packet = packet
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


class _TaskQueue(_Notifier):
    __slots__ = ('tasks',)

    def __init__(self):
        super().__init__()
        self.tasks: deque[_Task] = deque()

    def enqueue(self, task: _Task) -> None:
        self.tasks.append(task)
        self.notify()


class _StagedWorker:
    __slots__ = ('pid', 'proc', 'sock')

    def __init__(self, pid: int, proc: multiprocessing.Process, sock: socket.socket):
        self.pid = pid
        self.proc = proc
        self.sock = sock


class _StagedWorkerInbox(_Notifier):
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
    __slots__ = ('pid', 'proc', 'sock', 'task', 'stopping', 'partial_recv')

    def __init__(self, pid: int, proc: multiprocessing.Process, sock: socket.socket):
        self.pid = pid
        self.proc: multiprocessing.Process | None = proc
        self.sock: socket.socket | None = sock
        self.task: Future | None = None
        self.stopping: bool = False
        self.partial_recv: bytearray | None = None


class _WorkerCreator:
    def __init__(self, staged_worker_inbox: _StagedWorkerInbox):
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

    def join(self):
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
                    target=_worker_process_main,
                    args=(logging.getLogger().getEffectiveLevel(), child_sock),
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
        '_free_worker_pids',
        '_max_active_workers',
        '_pickled_task_queue',
        '_read_buf',
        '_shutdown',
        '_shutdown_notifier',
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
        shutdown_notifier: _Notifier,
        task_queue: _TaskQueue,
        staged_worker_inbox: _StagedWorkerInbox,
        abort_inbox: _AbortInbox,
        worker_creator: _WorkerCreator,
    ):
        self._max_active_workers = max_active_workers
        self._shutdown_notifier = shutdown_notifier
        self._task_queue = task_queue
        self._staged_worker_inbox = staged_worker_inbox
        self._abort_inbox = abort_inbox
        self._worker_creator = worker_creator
        self._workers: dict[int, _Worker] = {}
        self._pickled_task_queue: deque[_PickledTask] = deque()
        self._busy_workers: int = 0
        self._free_worker_pids: deque[int] = deque()
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
        self._read_buf = bytearray(_RECV_BUF_SIZE)
        self._shutdown = False
        self._future_to_worker_pid: dict[Future, int] = {}

    def run(self) -> None:
        self._worker_creator.schedule_creation(self._max_active_workers)
        while not self._shutdown:
            self._do_step()

    def _do_step(self) -> None:
        # print(f'[{datetime.now()}] step', file=sys.stderr)
        assert len(self._free_worker_pids) <= len(self._workers)
        assert self._busy_workers == len(self._workers) - len(self._free_worker_pids), (
            f'workers={len(self._workers)} free={len(self._free_worker_pids)} busy={self._busy_workers} stopping={self._stopping_workers} max={self._max_active_workers}'
        )
        assert 0 <= len(self._workers) - self._stopping_workers, (
            f'workers={len(self._workers)} free={len(self._free_worker_pids)} stopping={self._stopping_workers} max={self._max_active_workers}'
        )
        assert 0 <= len(self._workers) - len(self._free_worker_pids) <= self._max_active_workers, (
            f'workers={len(self._workers)} free={len(self._free_worker_pids)} max={self._max_active_workers}'
        )
        # assert sum(1 for w in self._workers.values() if w.task is not None) <= self._max_active_workers

        self._mark_timed_out_tasks()
        if self._task_queue.tasks:
            while True:
                self._select_and_process_fds(wait=False)
                if self._task_queue.tasks:
                    if self._busy_workers < self._max_active_workers and self._free_worker_pids:
                        assert not self._pickled_task_queue
                        task = self._task_queue.tasks.popleft()
                        if not task.future.cancelled():
                            worker_pid = self._free_worker_pids.popleft()
                            self._send_task(task, self._workers[worker_pid])
                    else:
                        self._pickle_one_task()
                else:
                    break
        self._select_and_process_fds(wait=True)

    def shut_down(self) -> None:
        # print(f'[{datetime.now()}] poolrunner.shut_down: event_selector.close()', file=sys.stderr)
        self._event_selector.close()
        # Cancel pending tasks.
        for task in self._pickled_task_queue:
            task.future.cancel()
        self._pickled_task_queue.clear()
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
                        received = worker.sock.recv(_RECV_BUF_SIZE)
                    except (EOFError, OSError):
                        break
                    if not received:
                        break
                worker.sock.close()
            if worker.task:
                worker.task.cancel()
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
        self._shutdown_notifier.read_sock.recv_into(self._read_buf)  # drain the notification(s)
        self._shutdown = True

    def _handle_task_queue_fd(self) -> None:
        self._task_queue.read_sock.recv_into(self._read_buf)  # drain the notification(s)

        if not self._pickled_task_queue:
            while self._task_queue.tasks and self._busy_workers < self._max_active_workers and self._free_worker_pids:
                task = self._task_queue.tasks.popleft()
                if task.future.cancelled():
                    continue
                worker_pid = self._free_worker_pids.popleft()
                self._send_task(task, self._workers[worker_pid])

    def _handle_staged_worker_inbox_fd(self) -> None:
        self._staged_worker_inbox.read_sock.recv_into(self._read_buf)  # drain the notification(s)

        for staged in self._staged_worker_inbox.take_all():
            assert staged.pid not in self._workers

            worker = _Worker(pid=staged.pid, proc=staged.proc, sock=staged.sock)
            self._workers[staged.pid] = worker
            self._event_selector.register(
                staged.sock, selectors.EVENT_READ, data=(self._on_readable_worker_sock, (staged.pid,))
            )
            self._send_task_or_mark_free(worker)

    def _handle_abort_inbox_fd(self) -> None:
        self._abort_inbox.read_sock.recv_into(self._read_buf)  # drain the notification(s)

        # print(f'[{datetime.now()}] event: aborts={len(aborts)} tasks={len(self._task_queue.tasks)} pickled_tasks={len(self._pickled_task_queue)}', file=sys.stderr)

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
            if worker.partial_recv is None:
                view = memoryview(self._read_buf)
                bytes_received = worker.sock.recv_into(view)
                view = view[:bytes_received]
            else:
                new_buf = worker.sock.recv(_RECV_BUF_SIZE)
                bytes_received = len(new_buf)
                worker.partial_recv.extend(new_buf)
                view = memoryview(worker.partial_recv)
        except (BlockingIOError, TimeoutError):
            raise
        except OSError:
            self._handle_worker_conn_eof(worker)
            return
        # print(f'read from pid={worker.pid} len={bytes_received} partial_recv={None if worker.partial_recv is None else len(worker.partial_recv)}', file=sys.stderr)
        if not bytes_received:
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
                original_future = worker.task
                assert original_future is not None
                tracked_pid = self._future_to_worker_pid.pop(original_future)
                assert tracked_pid == pid
                worker.task = None
                self._busy_workers -= 1
                assert self._busy_workers >= 0
                self._send_task_or_mark_free(worker)  # eagerly send a new task
                message = pickle.loads(raw_message)
                if isinstance(message, Exception):
                    _assign_future_exception(original_future, message)
                else:
                    _assign_future_result(original_future, message)
            elif tp == 1:  # log
                message = pickle.loads(raw_message)
                # Ignore logs if the future is already switched to "done" - these are likely spurious errors because the main process started deleting task's files.
                if worker.task is None or not worker.task.done():
                    mplogging.maybe_handle_message_from_worker(message)
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

    def _send_task(self, task: _Task | _PickledTask, worker: _Worker) -> None:
        assert worker.proc is not None
        assert worker.sock is not None
        match task:
            case _Task():
                packet = _create_packet((task.f, task.args))
            case _PickledTask():
                packet = memoryview(task.packet)
        # print(f'sending task to pid={worker.pid} len={len(packet)}', file=sys.stderr)
        while packet:
            nbytes = worker.sock.send(packet)
            packet = packet[nbytes:]
        assert worker.task is None
        worker.task = task.future
        self._busy_workers += 1
        timeout_when = time.monotonic() + task.timeout
        heapq.heappush(self._timeouts_heap, _TimeoutsHeapNode(when=timeout_when, task_future=task.future))
        self._future_to_worker_pid[task.future] = worker.pid
        # print(f'[{datetime.now()}] {"send_task" if isinstance(task, _Task) else "send_pickled_task"} task to pid={worker.pid}', file=sys.stderr)

    def _send_task_or_mark_free(self, worker: _Worker) -> None:
        if self._busy_workers < self._max_active_workers:
            if self._pickled_task_queue:
                self._send_task(self._pickled_task_queue.popleft(), worker)
                return
            if self._task_queue.tasks:
                self._send_task(self._task_queue.tasks.popleft(), worker)
                return
        self._free_worker_pids.append(worker.pid)

    def _maybe_send_task_to_free_worker(self) -> None:
        if self._free_worker_pids and (self._task_queue.tasks or self._pickled_task_queue):
            worker_pid = self._free_worker_pids.popleft()
            self._send_task((self._pickled_task_queue or self._task_queue.tasks).popleft(), self._workers[worker_pid])

    def _trigger_worker_stop(self, worker_pid: int) -> None:
        worker = self._workers[worker_pid]
        # print(f'[{datetime.now()}] trigger_worker_stop: pid={worker.pid}', file=sys.stderr)
        assert worker.proc is not None
        worker.stopping = True
        worker.task = None
        worker.proc.terminate()
        self._stopping_workers += 1
        assert worker.sock is not None
        # observe when the process will become join'able
        self._event_selector.register(
            worker.proc.sentinel, selectors.EVENT_READ, data=(self._on_worker_proc_fd_joinable, (worker_pid,))
        )

    def _pickle_one_task(self) -> None:
        task = self._task_queue.tasks.popleft()
        packet = bytes(_create_packet((task.f, task.args)))
        self._pickled_task_queue.append(_PickledTask(packet=packet, future=task.future, timeout=task.timeout))
        # print(f'[{datetime.now()}] pickle_one_task: tasks={len(self._task_queue.tasks)} pickled_tasks={len(self._pickled_task_queue)}', file=sys.stderr)

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
        if worker.task is not None:
            if not worker.task.done():
                _assign_future_exception(worker.task, ProcessPoolError(f'Worker {worker.pid} died'))
            tracked_pid = self._future_to_worker_pid.pop(worker.task)
            assert tracked_pid == worker.pid
            worker.task = None
        self._event_selector.unregister(worker.sock)

        worker.sock.close()
        worker.sock = None

        # print(f'[{datetime.now()}] worker_conn_eof: pid={worker.pid}', file=sys.stderr)
        if worker.proc is None:
            self._workers.pop(worker.pid)
            self._stopping_workers -= 1
            self._busy_workers -= 1
            self._maybe_send_task_to_free_worker()


def _worker_process_main(
    logging_level: int,
    sock: socket.socket,
) -> None:
    # print(f'worker[{os.getpid()}]: started', file=sys.stderr)
    sigmonitor.init(sigmonitor.Mode.QUICK_EXIT, sigint=False, sigchld=True)
    mplogging.init_in_worker(logging_level=logging_level, server_sock=sock)
    wakeup_fd = sigmonitor.get_wakeup_fd()
    read_buf = bytearray(_RECV_BUF_SIZE)
    with sigmonitor.scoped_mode(sigmonitor.Mode.RAISE_EXCEPTION_ON_DEMAND):
        with sock:
            with selectors.DefaultSelector() as event_selector:
                event_selector.register(sock, selectors.EVENT_READ)
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
                        total = 0
                        while True:
                            if total == len(read_buf):
                                read_buf.extend(b'\0' * len(read_buf))
                            view = memoryview(read_buf)
                            try:
                                nbytes = sock.recv_into(view[total:])
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
                        packet = _create_packet(result)
                        # print(f'worker[{os.getpid()}]: task reply size={len(packet)}', file=sys.stderr)
                        while packet:
                            try:
                                nbytes = sock.send(packet)
                            except (BlockingIOError, TimeoutError):
                                raise
                            except OSError:
                                break
                            packet = packet[nbytes:]
                        packet.release()


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
