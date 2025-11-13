"""Helpers for interacting with child processes."""

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
import queue
import selectors
import shlex
import subprocess
import sys
import threading
import time
from collections.abc import Iterator, Mapping
from concurrent.futures import Future
from dataclasses import dataclass, field
from enum import Enum, auto, unique
from typing import Any

import pebble
import psutil

from cvise.utils import sigmonitor


@unique
class ProcessEventType(Enum):
    STARTED = auto()
    FINISHED = auto()
    ORPHANED = auto()  # reported instead of FINISHED when worker leaves the child process not terminated


class _MPConnListener:
    """Provides asynchronous interface for accepting connections from multiprocessing children."""

    def __init__(self, backlog: int, pipe_to_notify: multiprocessing.connection.Connection):
        self._pipe_to_notify = pipe_to_notify
        self._listener = multiprocessing.connection.Listener(
            address=None,
            family='AF_PIPE' if sys.platform == 'win32' else 'AF_UNIX',
            backlog=backlog,
            authkey=None,
        )
        self._lock = threading.Lock()
        self._condition = threading.Condition(lock=self._lock)
        self._pending_conns: int = 0
        self._shutdown = False
        self._accepted_conns: collections.deque[multiprocessing.connection.Connection] = collections.deque()
        self._thread = threading.Thread(target=self._conn_listener_thread_main)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_and_wait()

    def address(self) -> Any:
        return self._listener.address

    def expect_new_connection(self) -> None:
        with self._lock:
            self._pending_conns += 1
            self._condition.notify()

    def take_connection(self) -> multiprocessing.connection.Connection | None:
        with self._lock:
            if not self._accepted_conns:
                return None
            return self._accepted_conns.popleft()

    def stop_and_wait(self) -> None:
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True
            self._condition.notify()
        self._thread.join(60)
        if self._thread.is_alive():
            logging.warning('Failed to stop connection listener thread')
        self._listener.close()

    def _conn_listener_thread_main(self) -> None:
        while True:
            with self._lock:
                self._condition.wait_for(lambda: self._shutdown or self._pending_conns)
                if self._shutdown:
                    break
                self._pending_conns -= 1
                assert self._pending_conns >= 0

            conn = self._listener.accept()
            with self._lock:
                self._accepted_conns.append(conn)
            self._pipe_to_notify.send(None)


@dataclass
class _PoolPickledTask:
    data: memoryview
    future: Future


@dataclass
class _PoolWorker:
    process: multiprocessing.Process | None
    connection: multiprocessing.connection.Connection | None
    active_task_future: Future | None
    terminating: bool = False


class ProcessPoolError(Exception):
    pass


class ProcessPool:
    def __init__(self, max_worker_count, worker_initializers, process_monitor, mplogger):
        self._max_worker_count = max_worker_count
        self._cond_read, self._cond_write = multiprocessing.Pipe(duplex=False)
        self._mp_conn_listener = _MPConnListener(backlog=max_worker_count, pipe_to_notify=self._cond_write)
        self._worker_initializers = worker_initializers
        self._process_monitor = process_monitor
        self._mplogger = mplogger
        self._lock = threading.Lock()
        self._task_queue: collections.deque[_PoolPickledTask] = collections.deque()
        self._cancel_term_queue: collections.deque[tuple[int, Future]] = collections.deque()
        self._pre_shutdown = False
        self._shutdown = False
        self._pool_thread = threading.Thread(target=self._pool_thread_main)

    def __enter__(self):
        self._pool_thread.start()
        self._mp_conn_listener.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

        # The listener must be stopped before the pool thread, to avoid deadlocking on accept() in the listener.
        self._mp_conn_listener.__exit__(exc_type, exc_val, exc_tb)

        # Terminate all workers in a blocking fashion.
        with self._lock:
            self._shutdown = True
        self._cond_write.send(None)
        self._pool_thread.join(60)
        if self._pool_thread.is_alive():
            logging.warning('Failed to stop process pool thread')

    def stop(self):
        # Indicate that already running tasks and free workers can be terminated.
        with self._lock:
            if self._pre_shutdown:
                return
            self._pre_shutdown = True
        self._cond_write.send(None)

    def schedule(self, f, args, timeout):
        future = Future()
        task = _PoolPickledTask(data=multiprocessing.reduction.ForkingPickler.dumps((f, args)), future=future)
        with self._lock:
            if self._pre_shutdown:
                future.cancel()
                return future
            self._task_queue.append(task)
            enqueued = len(self._task_queue)
        if enqueued == 1:
            self._cond_write.send(None)
        return future

    def _pool_thread_main(self):
        try:
            self._pool_thread_main2()
        except:
            logging.exception('_pool_thread_main')

    def _pool_thread_main2(self):
        workers: dict[int, _PoolWorker] = {}
        free_worker_pids: collections.deque[int] = collections.deque()
        connecting_workers: int = 0
        terminating_workers: int = 0
        event_selector = selectors.DefaultSelector()
        event_selector.register(self._cond_read, selectors.EVENT_READ)
        mp_conn_listener_address = self._mp_conn_listener.address()
        while True:
            worker: _PoolWorker | None = None
            terminate_workers: list[_PoolWorker] = []
            tasks_to_send: list[_PoolPickledTask] = []
            launch_worker: bool = False
            worker_conn_to_handshake: multiprocessing.connection.Connection | None = None
            with self._lock:
                assert connecting_workers <= len(workers)
                assert len(free_worker_pids) <= len(workers)
                assert terminating_workers <= len(workers)
                assert len(workers) - terminating_workers <= self._max_worker_count
                assert (
                    0
                    <= len(workers) - connecting_workers - terminating_workers - len(free_worker_pids)
                    <= self._max_worker_count
                ), (
                    f'len(workers)={len(workers)} connecting_workers={connecting_workers} terminating_workers={terminating_workers} len(free_worker_pids)={len(free_worker_pids)}'
                )
                if self._pre_shutdown:
                    break
                elif self._cancel_term_queue:
                    while self._cancel_term_queue:
                        worker_pid, future = self._cancel_term_queue.popleft()
                        if worker_pid not in workers:
                            continue
                        worker = workers[worker_pid]
                        if worker.active_task_future != future:
                            continue
                        terminate_workers.append(worker)
                    if not terminate_workers:
                        continue
                elif (
                    self._task_queue
                    and free_worker_pids
                    and len(workers) - connecting_workers - len(free_worker_pids) < self._max_worker_count
                ):
                    l1 = len(self._task_queue)
                    l2 = len(free_worker_pids)
                    l3 = self._max_worker_count - (len(workers) - connecting_workers - len(free_worker_pids))
                    cnt = min(l1, l2, l3)
                    while cnt and self._task_queue:
                        tasks_to_send.append(self._task_queue.popleft())
                        cnt -= 1
                elif self._task_queue and (conn := self._mp_conn_listener.take_connection()):
                    worker_conn_to_handshake = conn
                elif (
                    self._task_queue
                    and len(workers) - terminating_workers < self._max_worker_count
                    and not connecting_workers
                ):
                    launch_worker = True
                elif len(workers) - terminating_workers < self._max_worker_count:
                    launch_worker = True
                elif conn := self._mp_conn_listener.take_connection():
                    worker_conn_to_handshake = conn

            if terminate_workers:
                for worker in terminate_workers:
                    assert worker
                    assert worker.process is not None
                    worker_pid: int | None = worker.process.pid
                    assert worker_pid is not None
                    worker.terminating = True
                    worker.process.terminate()
                    terminating_workers += 1
                    assert worker.connection is not None
                    event_selector.register(worker.process.sentinel, selectors.EVENT_READ, data=worker_pid)
            elif tasks_to_send:
                for task in tasks_to_send:
                    assert free_worker_pids
                    worker_pid = free_worker_pids[0]
                    worker = workers[worker_pid]
                    task.future.add_done_callback(
                        lambda future, pid=worker_pid: self._schedule_term_if_needed(pid, future)
                    )
                    if task.future.cancelled():
                        continue
                    free_worker_pids.popleft()
                    assert worker.connection is not None
                    worker.connection.send_bytes(task.data)
                    worker.active_task_future = task.future
            elif worker_conn_to_handshake:
                worker_pid: int = worker_conn_to_handshake.recv()
                assert connecting_workers > 0
                connecting_workers -= 1
                worker = workers[worker_pid]
                worker.connection = worker_conn_to_handshake
                assert worker_pid not in free_worker_pids
                free_worker_pids.append(worker_pid)
                event_selector.register(worker_conn_to_handshake, selectors.EVENT_READ, data=worker_pid)
            elif launch_worker:
                self._mp_conn_listener.expect_new_connection()
                proc = multiprocessing.Process(
                    target=self._worker_process_main,
                    args=(mp_conn_listener_address, self._worker_initializers),
                )
                proc.start()
                assert proc.pid is not None
                workers[proc.pid] = _PoolWorker(process=proc, connection=None, active_task_future=None)
                connecting_workers += 1
                self._process_monitor.on_worker_started(proc.pid)
            else:
                events = event_selector.select()
                for selector_key, _event_mask in events:
                    fileobj = selector_key.fileobj
                    if fileobj == self._cond_read:
                        # Nothing to do here besides eating the notifications - the next iteration of the outer loop
                        # will pick up the changes (a new task, or a shutdown signal, etc.).
                        while True:
                            self._cond_read.recv()
                            if not self._cond_read.poll():
                                break
                    elif isinstance(fileobj, multiprocessing.connection.Connection):
                        pid: int = selector_key.data
                        worker = workers[pid]
                        assert worker.connection is not None
                        try:
                            message = worker.connection.recv()
                        except (EOFError, OSError):
                            if not worker.terminating and worker.active_task_future:
                                with contextlib.suppress(
                                    concurrent.futures.InvalidStateError
                                ):  # it might've been canceled
                                    worker.active_task_future.set_exception(ProcessPoolError(f'Worker {pid} died'))
                                worker.active_task_future = None
                            event_selector.unregister(worker.connection)
                            worker.connection.close()
                            worker.connection = None
                            if worker.process is None:
                                workers.pop(pid)
                                self._process_monitor.on_worker_stopped(pid)
                                terminating_workers -= 1
                        else:
                            if self._handle_message_from_worker(worker, message) and not worker.terminating:
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
                            self._process_monitor.on_worker_stopped(pid)
                            terminating_workers -= 1

        # In pre-shutdown, we can terminate all pending tasks and workers except the currently-connecting ones (which
        # are needed to avoid deadlocking the accept() call in the connection listener).
        event_selector.close()
        with self._lock:
            for task in self._task_queue:
                task.future.cancel()
        for worker in workers.values():
            if not worker.connection or worker.terminating:
                continue
            if worker.active_task_future:
                worker.active_task_future.cancel()
            worker.process.terminate()
            worker.terminating = True
        # Pump all messages from busy workers, e.g., process events that could've been sent shortly before termination.
        for worker in workers.values():
            if not worker.connection or not worker.active_task_future:
                continue
            while True:
                try:
                    message = worker.connection.recv()
                except (EOFError, OSError):
                    break
                else:
                    self._handle_message_from_worker(worker, message)

        # Wait for the signal of the full shutdown (which comes after the connection listener has stopped).
        while True:
            with self._lock:
                if self._shutdown:
                    break
            self._cond_read.recv_bytes()

        # Terminate the remaining workers and reap all children.
        for worker in workers.values():
            if not worker.terminating:
                assert worker.process is not None
                worker.process.terminate()
        for worker in workers.values():
            if worker.process is not None:
                worker.process.join()
                self._process_monitor.on_worker_stopped(worker.process.pid)

    def _handle_message_from_worker(self, worker: _PoolWorker, message) -> bool:
        if isinstance(message, ProcessEvent):
            self._process_monitor.on_process_event_from_worker(message)
            return False
        if isinstance(message, logging.LogRecord):
            if not worker.terminating:
                self._mplogger.on_log_from_worker(message)
            return False
        if worker.active_task_future is None:
            return False
        assert worker.active_task_future is not None
        with contextlib.suppress(concurrent.futures.InvalidStateError):  # it might've been canceled
            if isinstance(message, Exception):
                worker.active_task_future.set_exception(message)
            else:
                worker.active_task_future.set_result(message)
        worker.active_task_future = None
        return True

    def _schedule_term_if_needed(self, worker_pid: int, future: Future):
        if not future.cancelled():
            return
        with self._lock:
            self._cancel_term_queue.append((worker_pid, future))
        self._cond_write.send(None)

    @staticmethod
    def _worker_process_main(server_address, initializers):
        sigmonitor.init(sigmonitor.Mode.QUICK_EXIT)
        with sigmonitor.scoped_mode(sigmonitor.Mode.RAISE_EXCEPTION):
            try:
                with multiprocessing.connection.Client(server_address, authkey=None) as server_conn:
                    for init in initializers:
                        init(server_conn)
                    # Notify the main C-Vise process that we are ready for executing tasks.
                    server_conn.send(os.getpid())
                    # Handle incoming tasks in an infinite loop (until stopped by a signal).
                    with selectors.DefaultSelector() as event_selector:
                        event_selector.register(server_conn, selectors.EVENT_READ)
                        event_selector.register(sigmonitor.get_wakeup_fd(), selectors.EVENT_READ)
                        while True:
                            events = event_selector.select()
                            had_signal = False
                            can_recv = False
                            for selector_key, _event_mask in events:
                                if selector_key.fileobj == sigmonitor.get_wakeup_fd():
                                    had_signal = True
                                elif selector_key.fileobj:
                                    can_recv = True
                                else:
                                    assert 0
                            if had_signal:
                                sigmonitor.maybe_retrigger_action()
                            if can_recv:
                                f, args = server_conn.recv()
                                try:
                                    result = f(*args)
                                except Exception as e:
                                    result = e
                                pickled = multiprocessing.reduction.ForkingPickler.dumps(result)
                                with sigmonitor.scoped_mode(sigmonitor.Mode.RAISE_EXCEPTION_ON_DEMAND):
                                    server_conn.send_bytes(pickled)
            except (EOFError, OSError):
                return
            except Exception as e:
                raise


class ProcessEvent:
    def __init__(self, worker_pid, child_pid, event_type):
        self.worker_pid = worker_pid
        self.child_pid = child_pid
        self.type = event_type


class ProcessMonitor:
    """Keeps track of subprocesses spawned by Pebble workers."""

    def __init__(self, parallel_tests: int):
        self._lock = threading.Lock()
        self._worker_to_child_pids: dict[int, set[int]] = {}
        # Remember dead worker PIDs, so that we can distinguish an early-reported child PID (arriving before
        # on_worker_started()) from a posthumously received child PID - the latter needs to be killed. The constant is
        # chosen to be big enough to make it practically unlikely to receive a new pid_queue event from a
        # forgotten-to-be-dead worker.
        self._recent_dead_workers: collections.deque[int] = collections.deque(maxlen=parallel_tests * 10)
        self._killer = ProcessKiller()

    def __enter__(self):
        self._killer.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._killer.__exit__(exc_type, exc_val, exc_tb)

    def on_worker_started(self, worker_pid: int) -> None:
        # logging.info(f'ProcessMonitor.on_worker_started: {worker_pid}')
        with self._lock:
            # Children might've already been added in _on_pid_queue_event() if the pid_queue event arrived early.
            self._worker_to_child_pids.setdefault(worker_pid, set())
            # It's rare but still possible that a new worker reuses the PID from a recently terminated one.
            with contextlib.suppress(ValueError):
                self._recent_dead_workers.remove(worker_pid)

    def on_worker_stopped(self, worker_pid: int) -> None:
        # logging.info(f'ProcessMonitor.on_worker_stopped: {worker_pid}')
        with self._lock:
            self._recent_dead_workers.append(worker_pid)
            pids_to_kill = self._worker_to_child_pids.pop(worker_pid)

        for pid in pids_to_kill:
            self._killer.kill_process_tree(pid)

    def get_worker_to_child_pids(self) -> dict[int, set[int]]:
        with self._lock:
            return self._worker_to_child_pids.copy()

    def on_process_event_from_worker(self, event: ProcessEvent) -> None:
        # logging.info(f'ProcessMonitor.on_process_event_from_worker: {event}')
        with self._lock:
            posthumous = event.worker_pid in self._recent_dead_workers
            should_kill = posthumous or (event.type == ProcessEventType.ORPHANED)
            if not posthumous:
                # Update the worker's children PID set. The set might need to be created, since the pid_queue event
                # might've arrived before on_worker_started() gets called.
                children = self._worker_to_child_pids.setdefault(event.worker_pid, set())
                if event.type == ProcessEventType.STARTED:
                    children.add(event.child_pid)
                else:
                    children.discard(event.child_pid)

        if should_kill:
            self._killer.kill_process_tree(event.child_pid)


@dataclass(order=True, frozen=True)
class ProcessKillerTask:
    hard_kill: bool  # whether to kill() - as opposed to terminate()
    when: float  # seconds (in terms of the monotonic timer)
    proc: psutil.Process = field(compare=False)


class ProcessKiller:
    """Helper for terminating/killing process trees.

    For each process, we first try terminate() - SIGTERM on *nix - and if the process doesn't finish within TERM_TIMEOUT
    seconds we use kill() - SIGKILL on *nix. See also https://github.com/marxin/cvise/issues/145.
    """

    TERM_TIMEOUT = 3  # seconds
    EVENT_LOOP_STEP = 1  # seconds

    def __init__(self):
        # Essentially we implement a set of timers, one for each PID; since creating many threading.Timer would be too
        # costly, we use a single thread with an event queue instead.
        self._condition = threading.Condition()
        self._task_queue: list[ProcessKillerTask] = []
        self._shut_down: bool = False
        self._thread = threading.Thread(target=self._thread_main)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._condition:
            self._shut_down = True
            self._condition.notify()
        self._thread.join(timeout=60)  # semi-arbitrary timeout to prevent even theoretical possibility of deadlocks

    def kill_process_tree(self, pid: int) -> None:
        try:
            proc = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return
        task = ProcessKillerTask(hard_kill=False, when=0, proc=proc)
        with self._condition:
            heapq.heappush(self._task_queue, task)
            self._condition.notify()

    def _thread_main(self) -> None:
        while True:
            with self._condition:
                if not self._task_queue and self._shut_down:
                    break
                if self._task_queue and not self._task_queue[0].proc.is_running():
                    # the process exited - nothing left for this task, and no need to wait if we're blocking shutdown
                    heapq.heappop(self._task_queue)
                    continue
                now = time.monotonic()
                timeout = min(self._task_queue[0].when - now, self.EVENT_LOOP_STEP) if self._task_queue else None
                if timeout is None or timeout > 0:
                    self._condition.wait(timeout)
                    continue
                task = heapq.heappop(self._task_queue)
            if task.hard_kill:
                self._do_hard_kill(task.proc)
            else:
                self._do_terminate(task.proc)

    def _do_terminate(self, proc: psutil.Process) -> None:
        try:
            children = proc.children(recursive=True) + [proc]
        except psutil.NoSuchProcess:
            return

        alive_children = []
        for child in children:
            try:
                child.terminate()
            except psutil.NoSuchProcess:
                pass
            else:
                alive_children.append(child)
        if not alive_children:
            return

        when = time.monotonic() + self.TERM_TIMEOUT
        with self._condition:
            for child in alive_children:
                task = ProcessKillerTask(hard_kill=True, when=when, proc=child)
                heapq.heappush(self._task_queue, task)
            self._condition.notify()

    def _do_hard_kill(self, proc: psutil.Process) -> None:
        try:
            children = proc.children(recursive=True) + [proc]
        except psutil.NoSuchProcess:
            return

        for child in children:
            with contextlib.suppress(psutil.NoSuchProcess):
                child.kill()


_process_event_notifier_server_conn = None


class ProcessEventNotifier:
    """Runs a subprocess and reports its PID as start/finish events on the PID queue.

    Intended to be used in multiprocessing workers, to let the main process know the unfinished children subprocesses
    that should be killed.
    """

    _EVENT_LOOP_STEP = 1  # seconds

    def __init__(self, pid_queue: queue.Queue | None = None):
        self._my_pid = os.getpid()

    @staticmethod
    def initialize_in_worker(server_conn: multiprocessing.connection.Connection):
        # print(f'ProcessEventNotifier.initialize_in_worker pid={os.getpid()}', file=sys.stderr)
        global _process_event_notifier_server_conn
        _process_event_notifier_server_conn = server_conn

    def run_process(
        self,
        cmd: list[str] | str,
        shell: bool = False,
        input: bytes | None = None,
        stdout: int = subprocess.PIPE,
        stderr: int = subprocess.PIPE,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        **kwargs,
    ) -> tuple[bytes, bytes, int]:
        if shell:
            assert isinstance(cmd, str)

        # Prevent signals from interrupting in the middle of any operation besides proc.communicate() - abrupt exits
        # could result in spawning a child without having its PID reported or leaving the queue in inconsistent state.
        with sigmonitor.scoped_mode(sigmonitor.Mode.RAISE_EXCEPTION_ON_DEMAND):
            # print(f'run_process {os.getpid()}: BEGIN {cmd if isinstance(cmd, str) else " ".join(cmd)}', file=sys.stderr)
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=stdout,
                    stderr=stderr,
                    shell=shell,
                    env=env,
                    **kwargs,
                )
                self._notify_start(proc)

                with self._auto_notify_end(proc):
                    # If a timeout was specified and the process exceeded it, we need to kill it - otherwise we'll leave a
                    # zombie process on *nix. If it's KeyboardInterrupt/SystemExit, the worker will terminate soon, so we may
                    # have not enough time to properly kill children, and zombies aren't a concern.
                    with _auto_kill_on_timeout(proc):
                        with sigmonitor.scoped_mode(sigmonitor.Mode.RAISE_EXCEPTION):
                            stdout_data, stderr_data = self._communicate_with_sig_checks(proc, input, timeout)
                            # print(f'run_process {os.getpid()}: END', file=sys.stderr)
            except Exception as e:
                # print(f'run_process {os.getpid()}: ERROR: {e}', file=sys.stderr)
                raise

        return stdout_data, stderr_data, proc.returncode  # type: ignore

    def check_output(
        self,
        cmd: list[str] | str,
        shell: bool = False,
        input: bytes | None = None,
        stdout: int = subprocess.PIPE,
        stderr: int = subprocess.PIPE,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        **kwargs,
    ) -> bytes:
        stdout_data, stderr_data, returncode = self.run_process(
            cmd, shell, input, stdout, stderr, env, timeout, **kwargs
        )
        if returncode != 0:
            stderr_data = stderr_data.decode('utf-8', 'ignore').strip()
            delim = ': ' if stderr_data else ''
            name = cmd[0] if isinstance(cmd, list) else shlex.split(cmd)[0]
            raise RuntimeError(f'{name} failed with exit code {returncode}{delim}{stderr_data}')
        return stdout_data

    def _notify_start(self, proc: subprocess.Popen) -> None:
        if not _process_event_notifier_server_conn:
            return
        _process_event_notifier_server_conn.send(
            ProcessEvent(worker_pid=self._my_pid, child_pid=proc.pid, event_type=ProcessEventType.STARTED)
        )

    @contextlib.contextmanager
    def _auto_notify_end(self, proc: subprocess.Popen) -> Iterator[None]:
        try:
            yield
        finally:
            if _process_event_notifier_server_conn:
                event_type = ProcessEventType.ORPHANED if proc.returncode is None else ProcessEventType.FINISHED
                _process_event_notifier_server_conn.send(
                    ProcessEvent(worker_pid=self._my_pid, child_pid=proc.pid, event_type=event_type)
                )

    def _communicate_with_sig_checks(
        self, proc: subprocess.Popen, input: bytes | None, timeout: float | None
    ) -> tuple[bytes, bytes]:
        stop_time = None if timeout is None else time.monotonic() + timeout
        while True:
            sigmonitor.maybe_retrigger_action()

            step_timeout = self._EVENT_LOOP_STEP
            if stop_time is not None:
                left = max(0, stop_time - time.monotonic())
                step_timeout = min(step_timeout, left)

            try:
                return proc.communicate(input=input, timeout=step_timeout)  # type: ignore[arg-type]
            except subprocess.TimeoutExpired:
                if step_timeout == 0:
                    raise  # we reached the original timeout, so bail out
                input = b''  # the input has been written in the first communicate() call


@contextlib.contextmanager
def _auto_kill_on_timeout(proc: subprocess.Popen) -> Iterator[None]:
    try:
        yield
    except subprocess.TimeoutExpired:
        _kill(proc)
        raise


def _kill(proc: subprocess.Popen) -> None:
    # First, close i/o streams opened for PIPE. This allows us to simply use wait() to wait for the process completion.
    # Additionally, it acts as another indication (SIGPIPE on *nix) for the process and its grandchildren to exit.
    if proc.stdin is not None:
        proc.stdin.close()
    if proc.stdout is not None:
        proc.stdout.close()
    if proc.stderr is not None:
        proc.stderr.close()

    # Second, attempt graceful termination (SIGTERM on *nix). We wait for some timeout that's less than Pebble's
    # term_timeout, so that we (hopefully) have time to try hard termination before C-Vise main process kills us.
    # Repeatedly request termination several times a second, because some programs "miss" incoming signals.
    TERMINATE_TIMEOUT = pebble.CONSTS.term_timeout / 2  # type: ignore
    SLEEP_UNIT = 0.1  # semi-arbitrary
    stop_time = time.monotonic() + TERMINATE_TIMEOUT
    while True:
        proc.terminate()
        step_timeout = min(SLEEP_UNIT, stop_time - time.monotonic())
        if step_timeout <= 0:
            break
        try:
            proc.wait(timeout=step_timeout)
        except subprocess.TimeoutExpired:
            pass
        else:
            break
    if proc.returncode is not None:
        return

    # Third - if didn't exit on time - attempt a hard termination (SIGKILL on *nix).
    proc.kill()
    proc.wait()
