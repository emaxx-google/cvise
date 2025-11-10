"""Helpers for interacting with child processes."""

from __future__ import annotations

import collections
import concurrent.futures
import contextlib
import heapq
import multiprocessing
import multiprocessing.connection
import multiprocessing.reduction
import os
import queue
import shlex
import subprocess
import sys
import threading
import time
from collections.abc import Iterator, Mapping
from concurrent.futures import ALL_COMPLETED, Future
from dataclasses import dataclass, field
from enum import Enum, auto, unique
from typing import Callable

import pebble
import psutil

from cvise.utils import sigmonitor

_mp_task_loss_workaround_obj: MPTaskLossWorkaround | None = None

import logging


@unique
class ProcessEventType(Enum):
    STARTED = auto()
    FINISHED = auto()
    ORPHANED = auto()  # reported instead of FINISHED when worker leaves the child process not terminated


class ProcessPool:
    @dataclass
    class _Task:
        func: Callable
        args: tuple
        future: Future
        time_scheduled: float
        serialized: bytes | None = None
        time_pickle: float = 0

    @dataclass
    class _Worker:
        process: multiprocessing.Process
        connection: multiprocessing.connection.Connection | None
        active_task_future: Future | None
        time_launch_start: float = 0
        time_launched: float = 0
        time_accepted: float = 0
        time_handshaked: float = 0
        task_count: int = 0

    def __init__(self, max_worker_count, mp_context, worker_initializers, process_monitor):
        self._max_worker_count = max_worker_count
        self._mp_context = mp_context
        self._worker_listener = multiprocessing.connection.Listener(
            address=None,
            family='AF_PIPE' if sys.platform == 'win32' else 'AF_UNIX',
            backlog=max_worker_count,
            authkey=None,
        )
        self._worker_initializers = worker_initializers
        self._process_monitor = process_monitor
        self._lock = threading.Lock()
        self._cond_read, self._cond_write = mp_context.Pipe(duplex=False)
        self._task_queue: collections.deque[ProcessPool._Task] = collections.deque()
        self._cancel_term_queue: collections.deque[tuple[int, Future]] = collections.deque()
        self._shutdown = False
        self._workers_to_connect = 0
        self._accepted_connections = collections.deque()
        self._condition = threading.Condition(self._lock)
        self._listener_thread = threading.Thread(target=self._listener_thread_main)
        self._pool_thread = threading.Thread(target=self._pool_thread_main)

    def __enter__(self):
        self._pool_thread.start()
        self._listener_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self._pool_thread.join()
        self._listener_thread.join()
        self._worker_listener.close()

    def stop(self):
        # print(f'ProcessPool.stop', file=sys.stderr)
        with self._lock:
            self._shutdown = True
            self._condition.notify()
        self._cond_write.send(None)

    def schedule(self, f, args, timeout):
        time_scheduled = None
        time_scheduled = time.monotonic()
        future = Future()
        # print(f'ProcessPool.schedule: future={future}', file=sys.stderr)
        with self._lock:
            self._task_queue.append(ProcessPool._Task(func=f, args=args, future=future, time_scheduled=time_scheduled))
        self._cond_write.send(None)
        return future

    def _listener_thread_main(self):
        try:
            self._listener_thread_main2()
        except Exception:
            logging.exception('_listener_thread_main exception')

    def _listener_thread_main2(self):
        total_accept = 0
        while True:
            with self._lock:
                if self._shutdown:
                    break
                if not self._workers_to_connect:
                    self._condition.wait()
                    continue
                assert self._workers_to_connect > 0
                self._workers_to_connect -= 1

            st = time.monotonic()
            conn = self._worker_listener.accept()
            total_accept += time.monotonic() - st
            with self._lock:
                self._accepted_connections.append(conn)
            self._cond_write.send(None)

        with self._lock:
            self._accepted_connections.append(None)
        self._cond_write.send(None)
        print(f'_listener_thread_main: total_accept={total_accept}', file=sys.stderr)

    def _pool_thread_main(self):
        try:
            self._pool_thread_main2()
        except Exception:
            logging.exception('_pool_thread_main exception')

    def _pool_thread_main2(self):
        workers: dict[int, ProcessPool._Worker] = {}
        free_worker_pids: collections.deque[int] = collections.deque()
        connecting_worker_pids: set[int] = set()
        busy_worker_pids: set[int] = set()
        dying_worker_pids: set[int] = set()
        total_input_pickle = 0
        total_input_unpickle = 0
        total_result_pickle = 0
        total_result_unpickle = 0
        total_exec = 0
        total_schedule_to_result = 0
        total_until_exec_started = 0
        while True:
            task: ProcessPool._Task | None = None
            worker: ProcessPool._Worker | None = None
            serialize: bool = False
            terminate_workers: list[ProcessPool._Worker] = []
            tasks_to_schedule: list[ProcessPool._Task] = []
            connect_worker: bool = False
            launch_worker: bool = False
            worker_conn = None
            with self._lock:
                # print(f'ProcessPool.thread: max_worker_count={self._max_worker_count} task_queue={len(self._task_queue)} cancel_term_queue={len(self._cancel_term_queue)} workers={len(workers)} free_worker_pids={len(free_worker_pids)} connecting_worker_pids={len(connecting_worker_pids)} busy_worker_pids={len(busy_worker_pids)}', file=sys.stderr)
                assert len(workers) == len(connecting_worker_pids) + len(free_worker_pids) + len(
                    busy_worker_pids
                ) + len(dying_worker_pids), (
                    f'max_worker_count={self._max_worker_count} task_queue={len(self._task_queue)} cancel_term_queue={len(self._cancel_term_queue)} workers={len(workers)} free_worker_pids={len(free_worker_pids)} connecting_worker_pids={len(connecting_worker_pids)} busy_worker_pids={len(busy_worker_pids)} dying_worker_pids={len(dying_worker_pids)}'
                )
                assert len(workers) - len(dying_worker_pids) <= self._max_worker_count
                assert len(busy_worker_pids) <= self._max_worker_count
                if self._shutdown:
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
                elif self._task_queue and free_worker_pids and len(busy_worker_pids) + len(dying_worker_pids) < self._max_worker_count:
                    l1 = len(self._task_queue)
                    l2 = len(free_worker_pids)
                    l3 = self._max_worker_count - len(busy_worker_pids) - len(dying_worker_pids)
                    for _ in range(min(l1, l2, l3)):
                        task = self._task_queue.popleft()
                        tasks_to_schedule.append(task)
                elif self._task_queue and self._accepted_connections:
                    connect_worker = True
                    worker_conn = self._accepted_connections.popleft()
                elif self._task_queue and len(workers) - len(dying_worker_pids) < self._max_worker_count and not connecting_worker_pids:
                    launch_worker = True
                elif self._task_queue and self._task_queue[0].serialized is None:
                    serialize = True
                    task = self._task_queue[0]
                elif len(workers) - len(dying_worker_pids) < self._max_worker_count:
                    launch_worker = True
                elif self._task_queue and any(t for t in self._task_queue if t.serialized is None):
                    serialize = True
                    for t in self._task_queue:
                        if t.serialized is None:
                            task = t
                            break
                    else:
                        assert 0
                elif self._accepted_connections:
                    connect_worker = True
                    worker_conn = self._accepted_connections.popleft()

            if terminate_workers:
                for worker in terminate_workers:
                    assert worker
                    worker_pid: int | None = worker.process.pid
                    assert worker_pid is not None
                    # print(f'terminating worker pid={worker_pid}', file=sys.stderr)
                    if worker.connection:
                        worker.connection.close()
                    worker.process.terminate()
                    assert worker_pid not in dying_worker_pids
                    dying_worker_pids.add(worker_pid)
                    busy_worker_pids.remove(worker_pid)
                    if worker_pid in free_worker_pids:
                        free_worker_pids.remove(worker_pid)
            elif tasks_to_schedule:
                for task in tasks_to_schedule:
                    assert free_worker_pids
                    worker_pid = free_worker_pids[0]
                    worker = workers[worker_pid]
                    task.future.add_done_callback(lambda future, pid=worker_pid: self._schedule_term_if_needed(pid, future))
                    if task.future.cancelled():
                        continue
                    free_worker_pids.popleft()
                    now = time.monotonic()
                    # print(f'sending task pid={worker_pid} time_scheduled={task.time_scheduled:.1f} now=+{now-task.time_scheduled:.5f} time_launch_start={worker.time_launch_start:.1f} time_launched=+{worker.time_launched-worker.time_launch_start:.5f} time_accepted=+{worker.time_accepted-worker.time_launch_start:.5f} time_handshaked=+{worker.time_handshaked-worker.time_launch_start:.5f}', file=sys.stderr)
                    worker.task_count += 1
                    if task.serialized is None:
                        st = time.monotonic()
                        task.serialized = multiprocessing.reduction.ForkingPickler.dumps((task.func, task.args, task.time_scheduled))
                        task.time_pickle = time.monotonic() - st
                    total_input_pickle += task.time_pickle
                    worker.connection.send_bytes(task.serialized)
                    # print(f'ProcessPool.thread: sent; time_pickle={task.time_pickle:.5f}', file=sys.stderr)
                    worker.active_task_future = task.future
                    assert worker_pid not in busy_worker_pids
                    busy_worker_pids.add(worker_pid)
            elif task and serialize:
                st = time.monotonic()
                task.serialized = multiprocessing.reduction.ForkingPickler.dumps((task.func, task.args, task.time_scheduled))
                task.time_pickle = time.monotonic() - st
            elif connect_worker:
                assert worker_conn
                time_accepted = time.monotonic()
                worker_pid: int = worker_conn.recv()
                assert worker_pid in connecting_worker_pids
                connecting_worker_pids.remove(worker_pid)
                workers[worker_pid].connection = worker_conn
                workers[worker_pid].time_accepted = time_accepted
                workers[worker_pid].time_handshaked = time.monotonic()
                assert worker_pid not in free_worker_pids
                free_worker_pids.append(worker_pid)
                # print(f'connected to worker pid={worker_pid}', file=sys.stderr)
            elif launch_worker:
                # print(f'launching worker', file=sys.stderr)
                time_launch_start = time.monotonic()
                with self._lock:
                    self._workers_to_connect += 1
                    self._condition.notify()
                proc = self._mp_context.Process(
                    target=self._worker_process,
                    args=(self._worker_listener.address, self._worker_initializers),
                )
                proc.start()
                assert proc.pid is not None
                workers[proc.pid] = ProcessPool._Worker(process=proc, connection=None, active_task_future=None)
                workers[proc.pid].time_launch_start = time_launch_start
                workers[proc.pid].time_launched = time.monotonic()
                connecting_worker_pids.add(proc.pid)
                self._process_monitor.on_worker_started(proc.pid)
            else:
                to_listen = (
                    [self._cond_read]
                    + [conn for pid in busy_worker_pids if (conn := workers[pid].connection) is not None]
                    + [workers[pid].process.sentinel for pid in dying_worker_pids]
                )
                assert to_listen
                ready = multiprocessing.connection.wait(to_listen)
                ready_conns = set()
                joinable_procs = set()
                for item in ready:
                    if item == self._cond_read:
                        while True:
                            self._cond_read.recv()
                            if not self._cond_read.poll():
                                break
                    elif isinstance(item, multiprocessing.connection.Connection):
                        ready_conns.add(item)
                    else:
                        joinable_procs.add(item)

                died_pids = []
                for pid in dying_worker_pids:
                    worker = workers[pid]
                    if worker.process.sentinel in joinable_procs:
                        died_pids.append(worker.process.pid)
                        worker.process.join()
                        workers.pop(pid)
                        self._process_monitor.on_worker_stopped(pid)
                for pid in died_pids:
                    dying_worker_pids.remove(pid)

                task_completed_pids = []
                for worker_pid in busy_worker_pids:
                    worker = workers[worker_pid]
                    if worker.connection not in ready_conns:
                        continue
                    # print(f'ProcessPool.thread: result for pid={worker_pid} future={worker.active_task_future}', file=sys.stderr)
                    result_bytes = worker.connection.recv_bytes()
                    time_conn_read = time.monotonic()
                    result = multiprocessing.reduction.ForkingPickler.loads(result_bytes)
                    time_unpickled = time.monotonic()
                    if isinstance(result, ProcessEvent):
                        self._process_monitor.on_process_event_from_worker(result)
                        continue
                    total_result_unpickle += time_unpickled - time_conn_read
                    task_completed_pids.append(worker_pid)
                    future = worker.active_task_future
                    assert future is not None
                    with contextlib.suppress(concurrent.futures.InvalidStateError):  # it might've been canceled
                        if isinstance(result, Exception):
                            future.set_exception(result)
                        else:
                            result, time_scheduled, time_received, time_unpickled, time_exec_started, time_execed, duration_result_pickled = result
                            future.set_result(result)
                            now = time.monotonic()
                            total_exec += time_execed - time_exec_started
                            total_input_unpickle += time_unpickled - time_received
                            total_schedule_to_result += now - time_scheduled
                            total_until_exec_started += time_exec_started - time_scheduled
                            total_result_pickle += duration_result_pickled
                            # print(f'finished task pid={worker_pid} time_scheduled={time_scheduled:.1f} time_till_exec_started=+{time_exec_started-time_scheduled:.5f} time_execed={time_execed-time_exec_started:.5f} time_till_conn_read=+{time_conn_read-time_execed:.5f} time_till_unpickled=+{time_unpickled-time_execed:.5f} time_till_set_future=+{now-time_execed:.5f}')
                    worker.active_task_future = None
                    assert worker_pid not in free_worker_pids
                    free_worker_pids.append(worker_pid)
                # assert len(ready_conns) == len(task_completed_pids)
                for worker_pid in task_completed_pids:
                    # print(f'finished task pid={worker_pid}', file=sys.stderr)
                    busy_worker_pids.remove(worker_pid)

        while True:
            with self._lock:
                if self._accepted_connections and self._accepted_connections.popleft() is None:
                    break
            self._cond_read.recv()

        print(f'ProcessPool.thread: shutdown: total_input_pickle={total_input_pickle:.3f} total_input_unpickle={total_input_unpickle:.3f} total_result_pickle={total_result_pickle} total_result_unpickle={total_result_unpickle:.3f} total_exec={total_exec:.3f} total_schedule_to_result={total_schedule_to_result:.3f} total_until_exec_started={total_until_exec_started:.3f}', file=sys.stderr)
        for worker in workers.values():
            if worker.active_task_future:
                worker.active_task_future.cancel()
            if worker.connection:
                worker.connection.close()
            worker.process.terminate()
        for worker in workers.values():
            worker.process.join()
            self._process_monitor.on_worker_stopped(worker.process.pid)
        # print(f'ProcessPool.thread: shutdown complete', file=sys.stderr)

    def _schedule_term_if_needed(self, worker_pid: int, future: Future):
        if not future.cancelled():
            return
        # print(f'_schedule_term_if_needed: worker_pid={worker_pid} future={future}', file=sys.stderr)
        with self._lock:
            self._cancel_term_queue.append((worker_pid, future))
        self._cond_write.send(None)

    @staticmethod
    def _worker_process(server_address, initializers):
        pid = os.getpid()
        # print(f'worker_process: pid={pid}', file=sys.stderr)
        try:
            server_conn = multiprocessing.connection.Client(server_address, authkey=None)
            sigmonitor.init(sigmonitor.Mode.QUICK_EXIT)
            for init in initializers:
                init(server_conn)
            server_conn.send(pid)
            while True:
                b = server_conn.recv_bytes()
                time_received = time.monotonic()
                f, args, time_scheduled = multiprocessing.reduction.ForkingPickler.loads(b)
                time_unpickled = time.monotonic()
                # print(f'worker_process pid={pid}: starting task: now-time_scheduled=+{time.monotonic()-time_scheduled:.5f}', file=sys.stderr)
                try:
                    # time_exec_started = None
                    time_exec_started = time.monotonic()
                    result = f(*args)
                    # time_execed = None
                    time_execed = time.monotonic()
                    # print(f'worker_process pid={pid}: task finished: {result}', file=sys.stderr)
                except Exception as e:
                    # print(f'worker_process pid={pid} task exception {e}', file=sys.stderr)
                    server_conn.send(e)
                    # print(f'worker_process pid={pid}: task exception sent', file=sys.stderr)
                else:
                    # logging.info(f'worker_process {pid}: finishing task: result={result}')
                    d = (result, time_scheduled, time_received, time_unpickled, time_exec_started, time_execed, time.monotonic())
                    b = multiprocessing.reduction.ForkingPickler.dumps(d)
                    time_pickled = time.monotonic()
                    d = (result, time_scheduled, time_received, time_unpickled, time_exec_started, time_execed, time_pickled - time_execed)
                    server_conn.send(d)
                    # print(f'worker_process pid={pid}: task result sent', file=sys.stderr)
        except EOFError:
            return
        except BrokenPipeError:
            return
        except ConnectionResetError:
            return
        except Exception as e:
            # print(f'worker_process pid={pid} exception {e}', file=sys.stderr)
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

    def __init__(self, pid_queue: queue.Queue | None):
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
                _process_event_notifier_server_conn.send(ProcessEvent(worker_pid=self._my_pid, child_pid=proc.pid, event_type=event_type))

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


class MPTaskLossWorkaround:
    """Workaround that attempts to prevent Pebble from losing scheduled tasks.

    The problematic scenario is when Pebble starts terminating a worker for a canceled taskA, but the worker manages to
    acknowledge the receipt of the next taskB shortly before dying - in that case taskB becomes associated with a
    non-existing worker and never finishes.

    Here we try to prevent this by scheduling "barrier" tasks, one for each worker, which report themselves as started
    and then sleep. If a task gets affected by the bug it either (a) won't report anything, or (b) will terminate
    abruptly without resolving its future; we detect "a" via a hardcoded timeout, and "b" by monitoring the task's
    worker PID, and cancel all such "hung" tasks; at the end we notify all other tasks to complete. The expectation is
    that this procedure leaves the workers in a good state ready for regular C-Vise jobs.
    """

    _DEADLINE = 30  # seconds
    _POLL_LOOP_STEP = 0.1  # seconds

    def __init__(self, worker_count: int):
        self._worker_count = worker_count
        # Don't use Manager-based synchronization primitives because of their poor performance. Don't use Queue since
        # it uses background threads which breaks our assumptions and isn't compatible with quick exit on signals.
        self._task_status_queue = multiprocessing.SimpleQueue()
        self._task_exit_flag = multiprocessing.Event()

    def initialize_in_worker(self) -> None:
        """Must be called in a worker process in order to initialize global state needed later."""
        global _mp_task_loss_workaround_obj
        _mp_task_loss_workaround_obj = self

    def execute(self, pool: pebble.ProcessPool) -> None:
        # 1. Send out the barrier tasks.
        futures: list[Future] = [pool.schedule(self._job, args=[task_id]) for task_id in range(self._worker_count)]
        task_procs: dict[int, psutil.Process | None] = {}

        def pump_task_queue():
            while not self._task_status_queue.empty():
                task_id, pid = self._task_status_queue.get()
                try:
                    task_procs[task_id] = psutil.Process(pid)
                except psutil.NoSuchProcess:
                    task_procs[task_id] = None  # remember that the task was claimed by a now-dead worker

        # 2. Detect which tasks started successfully.
        start_time = time.monotonic()
        while time.monotonic() < start_time + self._DEADLINE:
            pump_task_queue()
            if len(task_procs) == self._worker_count:
                break
            time.sleep(self._POLL_LOOP_STEP)  # SimpleQueue doesn't provide polling

        # 3. Shut down all tasks - use graceful termination for the successfully started ones, and cancel the lost ones.
        self._task_exit_flag.set()
        start_time = time.monotonic()
        while time.monotonic() < start_time + self._DEADLINE:
            pump_task_queue()
            task_procs = {task_id: proc for task_id, proc in task_procs.items() if proc and proc.is_running()}
            for task_id, future in enumerate(futures):
                if task_id not in task_procs:
                    future.cancel()
            _done, still_running = concurrent.futures.wait(
                futures, return_when=ALL_COMPLETED, timeout=self._POLL_LOOP_STEP
            )
            if not still_running:
                break

        # 4. Cleanup; make sure to free the pool if the graceful termination above didn't finish within the timeout.
        for future in futures:
            future.cancel()
        self._task_exit_flag.clear()

    @staticmethod
    def _job(task_id: int) -> None:
        assert _mp_task_loss_workaround_obj
        status_queue = _mp_task_loss_workaround_obj._task_status_queue
        exit_flag = _mp_task_loss_workaround_obj._task_exit_flag
        # Don't allow signals to interrupt IPC primitives since this might leave them in locked/inconsistent state; only
        # exit in the safe location from the pool loop.
        with sigmonitor.scoped_mode(sigmonitor.Mode.RAISE_EXCEPTION_ON_DEMAND):
            status_queue.put((task_id, os.getpid()))
            while not exit_flag.wait(timeout=MPTaskLossWorkaround._POLL_LOOP_STEP):
                sigmonitor.maybe_retrigger_action()


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
