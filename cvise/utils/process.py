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
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

import psutil

from cvise.utils import mplogging, sigmonitor


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
        self._accepted_conns: list[multiprocessing.connection.Connection] = []
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

    def take_connections(self) -> list[multiprocessing.connection.Connection]:
        with self._lock:
            conns = self._accepted_conns
            self._accepted_conns = []
            return conns

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


@dataclass(frozen=True, slots=True)
class _PoolTask:
    f: Callable
    args: Sequence[Any]
    future: Future
    timeout: float


@dataclass(frozen=True, slots=True)
class _SerializedPoolTask:
    data: memoryview
    future: Future
    timeout: float


@dataclass(slots=True)
class _PoolWorker:
    process: multiprocessing.Process | None
    connection: multiprocessing.connection.Connection | None
    active_task_future: Future | None
    stopping: bool = False


class ProcessPoolError(Exception):
    pass


class ProcessPool:
    def __init__(self, max_worker_count):
        self._max_worker_count = max_worker_count
        self._cond_read, self._cond_write = multiprocessing.Pipe(duplex=False)
        self._mp_conn_listener = _MPConnListener(backlog=max_worker_count, pipe_to_notify=self._cond_write)
        self._lock = threading.Lock()
        self._task_queue: collections.deque[_PoolTask] = collections.deque()
        self._termination_queue: collections.deque[tuple[int, Future]] = collections.deque()
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
            assert 0

    def stop(self):
        # Indicate that already running tasks and free workers can be terminated.
        with self._lock:
            if self._pre_shutdown:
                return
            self._pre_shutdown = True
        self._cond_write.send(None)

    def schedule(self, f: Callable, args: Sequence[Any], timeout):
        future = Future()
        task = _PoolTask(f=f, args=args, future=future, timeout=timeout)
        with self._lock:
            if self._pre_shutdown:
                future.cancel()
                return future
            self._task_queue.append(task)
            need_signal = len(self._task_queue) + len(self._termination_queue) == 1
        if need_signal:
            self._cond_write.send(None)
        return future

    def _pool_thread_main(self):
        try:
            self._pool_thread_main2()
        except:
            logging.exception('_pool_thread_main')

    def _pool_thread_main2(self):
        workers: dict[int, _PoolWorker] = {}
        serialized_task_queue: collections.deque[_SerializedPoolTask] = collections.deque()
        free_worker_pids: collections.deque[int] = collections.deque()
        starting_workers: int = 0
        stopping_workers: int = 0
        timeouts_heap: list[tuple[float, Future]] = []
        event_selector = selectors.DefaultSelector()
        event_selector.register(self._cond_read, selectors.EVENT_READ)
        mp_conn_listener_address = self._mp_conn_listener.address()
        while True:
            now = time.monotonic()
            while timeouts_heap and (now >= timeouts_heap[0][0] or timeouts_heap[0][1].done()):
                _when, future = heapq.heappop(timeouts_heap)
                with contextlib.suppress(concurrent.futures.InvalidStateError):
                    future.set_exception(TimeoutError(f'Job timed out'))

            # Eat notifications sent until this point. This has to be done before reading members under the mutex below.
            while self._cond_read.poll():
                self._cond_read.recv_bytes()

            worker: _PoolWorker | None = None
            workers_to_start: int = 0
            workers_to_handshake: list[multiprocessing.connection.Connection] = []
            workers_to_stop: list[_PoolWorker] = []
            tasks_to_send: collections.deque[_PoolTask | _SerializedPoolTask] = collections.deque()
            has_tasks_to_serialize = False
            with self._lock:
                assert starting_workers <= len(workers)
                assert len(free_worker_pids) <= len(workers)
                assert stopping_workers <= len(workers)
                assert len(workers) - stopping_workers <= self._max_worker_count
                assert (
                    0
                    <= len(workers) - starting_workers - stopping_workers - len(free_worker_pids)
                    <= self._max_worker_count
                ), (
                    f'len(workers)={len(workers)} starting_workers={starting_workers} stopping_workers={stopping_workers} len(free_worker_pids)={len(free_worker_pids)}'
                )
                if self._pre_shutdown:
                    break

                while self._termination_queue:
                    worker_pid, future = self._termination_queue.popleft()
                    if worker_pid not in workers:
                        continue
                    worker = workers[worker_pid]
                    if worker.active_task_future != future:
                        continue
                    workers_to_stop.append(worker)

                workers_to_start = self._max_worker_count - len(workers) + stopping_workers + len(workers_to_stop)
                assert 0 <= workers_to_start <= self._max_worker_count, (
                    f'workers_to_start={workers_to_start} max_worker_count={self._max_worker_count} workers={len(workers)} starting_workers={starting_workers} stopping_workers={stopping_workers} workers_to_stop={len(workers_to_stop)}'
                )

                workers_to_handshake = self._mp_conn_listener.take_connections()

                available_workers = min(
                    len(free_worker_pids) + len(workers_to_handshake),
                    self._max_worker_count - (len(workers) - starting_workers - len(free_worker_pids)),
                )
                for queue in (serialized_task_queue, self._task_queue):
                    while available_workers and queue:
                        task = queue.popleft()
                        if not task.future.done():
                            tasks_to_send.append(task)
                            available_workers -= 1

                has_tasks_to_serialize = len(self._task_queue) > 0

            for worker in workers_to_stop:
                assert worker
                assert worker.process is not None
                assert worker.process.pid is not None
                worker.stopping = True
                worker.process.terminate()
                stopping_workers += 1
                assert worker.connection is not None
                # observe when the process will become join'able
                event_selector.register(worker.process.sentinel, selectors.EVENT_READ, data=worker.process.pid)

            while tasks_to_send and free_worker_pids:
                task = tasks_to_send.popleft()
                worker_pid = free_worker_pids.popleft()
                self._send_task(task, workers[worker_pid], timeouts_heap)

            for conn in workers_to_handshake:
                worker_pid: int = conn.recv()
                assert starting_workers > 0
                starting_workers -= 1
                worker = workers[worker_pid]
                worker.connection = conn
                event_selector.register(conn, selectors.EVENT_READ, data=worker_pid)
                assert worker_pid not in free_worker_pids
                if tasks_to_send:
                    task = tasks_to_send.popleft()
                    self._send_task(task, worker, timeouts_heap)
                else:
                    free_worker_pids.append(worker_pid)
            assert not tasks_to_send

            for _ in range(workers_to_start):
                self._mp_conn_listener.expect_new_connection()
                proc = multiprocessing.Process(
                    target=self._worker_process_main,
                    args=(logging.getLogger().getEffectiveLevel(), mp_conn_listener_address),
                )
                proc.start()
                assert proc.pid is not None
                # print(f'started worker pid={proc.pid}')
                workers[proc.pid] = _PoolWorker(process=proc, connection=None, active_task_future=None)
                starting_workers += 1

            while True:
                poll_timeout = 0 if has_tasks_to_serialize else timeouts_heap[0][0] - now if timeouts_heap else None
                events = event_selector.select(timeout=poll_timeout)
                for selector_key, _event_mask in events:
                    fileobj = selector_key.fileobj
                    if fileobj == self._cond_read:
                        # Nothing to do here - the next iteration of the outer loop will pick up the changes (a new task, or
                        # a shutdown signal, etc.).
                        pass
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
                            if worker.process is None:
                                workers.pop(pid)
                                stopping_workers -= 1
                        else:
                            message = multiprocessing.reduction.ForkingPickler.loads(raw_message)
                            if self._handle_message_from_worker(worker, message) and not worker.stopping:
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
                with self._lock:
                    task = self._task_queue.popleft()
                    has_tasks_to_serialize = len(self._task_queue) > 0
                data = multiprocessing.reduction.ForkingPickler.dumps((task.f, task.args))
                serialized_task_queue.append(_SerializedPoolTask(data=data, future=task.future, timeout=task.timeout))

        # In pre-shutdown, we can terminate all pending tasks and workers except the currently-connecting ones (which
        # are needed to avoid deadlocking the accept() call in the connection listener).
        event_selector.close()
        with self._lock:
            for task in serialized_task_queue:
                task.future.cancel()
            for task in self._task_queue:
                task.future.cancel()
        for worker in workers.values():
            if not worker.connection or worker.stopping:
                continue
            if worker.active_task_future:
                worker.active_task_future.cancel()
            worker.process.terminate()
            worker.stopping = True
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
            if not worker.stopping:
                assert worker.process is not None
                worker.process.terminate()
        for worker in workers.values():
            if worker.process is not None:
                worker.process.join()

    def _send_task(self, task, worker: _PoolWorker, timeouts_heap) -> None:
        assert worker.process is not None
        assert worker.process.pid is not None
        task.future.add_done_callback(lambda future: self._on_future_resolved(worker.process.pid, future))
        assert worker.connection is not None
        data = (
            multiprocessing.reduction.ForkingPickler.dumps((task.f, task.args))
            if isinstance(task, _PoolTask)
            else task.data
        )
        worker.connection.send_bytes(data)
        assert worker.active_task_future is None
        worker.active_task_future = task.future
        heapq.heappush(timeouts_heap, (time.monotonic() + task.timeout, task.future))

    def _handle_message_from_worker(self, worker: _PoolWorker, message: Any) -> bool:
        is_active_worker = not worker.stopping and (
            worker.active_task_future is None or not worker.active_task_future.done()
        )
        if mplogging.maybe_handle_message_from_worker(message, is_active_worker):
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

    def _on_future_resolved(self, worker_pid: int, future: Future):
        if not future.cancelled() and not isinstance(future.exception(timeout=0), TimeoutError):
            return
        with self._lock:
            self._termination_queue.append((worker_pid, future))
            need_signal = len(self._task_queue) + len(self._termination_queue) == 1
        if need_signal:
            self._cond_write.send(None)

    @staticmethod
    def _worker_process_main(logging_level: int, server_address):
        try:
            ProcessPool._worker_process_main2(logging_level, server_address)
        except Exception as e:
            print(f'{os.getpid()} worker process exception: {e}', file=sys.stderr)

    @staticmethod
    def _worker_process_main2(logging_level: int, server_address):
        sigmonitor.init(sigmonitor.Mode.QUICK_EXIT)
        with sigmonitor.scoped_mode(sigmonitor.Mode.RAISE_EXCEPTION_ON_DEMAND):
            try:
                with multiprocessing.connection.Client(server_address, authkey=None) as server_conn:
                    mplogging.init_in_worker(logging_level=logging_level, server_conn=server_conn)
                    # Notify the main C-Vise process that we are ready for executing tasks.
                    server_conn.send(os.getpid())
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
                return
            except Exception as e:
                raise


class ProcessEventNotifier:
    """Runs a subprocess and reports its PID as start/finish events on the PID queue.

    Intended to be used in multiprocessing workers, to let the main process know the unfinished children subprocesses
    that should be killed.
    """

    _EVENT_LOOP_STEP = 1  # seconds

    def __init__(self, pid_queue: queue.Queue | None = None):
        self._my_pid = os.getpid()

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

        with subprocess.Popen(
            cmd,
            stdout=stdout,
            stderr=stderr,
            shell=shell,
            env=env,
            **kwargs,
        ) as proc:
            with _auto_kill_descendants(proc):  # TODO: filter by own PID to avoid race
                stdout_data, stderr_data = self._communicate_with_sig_checks(proc, input, timeout)
        return stdout_data, stderr_data, proc.returncode

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

    def _communicate_with_sig_checks(
        self, proc: subprocess.Popen, input: bytes | None, timeout: float | None
    ) -> tuple[bytes, bytes]:
        comm_read_socket, comm_write_socket = socket.socketpair()
        comm_read_socket.setblocking(False)
        comm_write_socket.setblocking(False)
        comm_result = _CommunicationResult()
        comm_thread = threading.Thread(
            target=self._comm_thread_main, args=(proc, input, timeout, comm_result, comm_write_socket), daemon=True
        )
        comm_thread.start()

        with selectors.DefaultSelector() as event_selector:
            event_selector.register(sigmonitor.get_wakeup_fd(), selectors.EVENT_READ)
            event_selector.register(comm_read_socket, selectors.EVENT_READ)
            while True:
                events = event_selector.select()
                sigmonitor.maybe_retrigger_action()
                if any(k.fileobj == comm_read_socket for k, _ in events):
                    break

        comm_read_socket.close()

        if comm_result.exception is not None:
            raise comm_result.exception
        comm_thread.join()
        assert comm_result.stdout is not None
        assert comm_result.stderr is not None
        return comm_result.stdout, comm_result.stderr

    @staticmethod
    def _comm_thread_main(
        proc: subprocess.Popen,
        input: bytes | None,
        timeout: float | None,
        comm_result: _CommunicationResult,
        comm_write_socket: socket.socket,
    ) -> None:
        try:
            comm_result.stdout, comm_result.stderr = proc.communicate(input=input, timeout=timeout)
        except Exception as e:
            comm_result.exception = e
        finally:
            try:
                comm_write_socket.send(b'\0')
            finally:
                comm_write_socket.close()


@dataclass(slots=True)
class _CommunicationResult:
    stdout: bytes | None = None
    stderr: bytes | None = None
    exception: Any = None


@contextlib.contextmanager
def _auto_kill_descendants(proc: subprocess.Popen) -> Iterator[None]:
    try:
        yield
    finally:
        if proc.returncode is None:
            _kill_subtree(proc.pid)
            proc.wait()


def _kill_subtree(pid: int) -> None:
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    try:
        children = proc.children(recursive=True) + [proc]
    except psutil.NoSuchProcess:
        return

    alive_children: list[psutil.Process] = []
    for child in children:
        try:
            child.terminate()
        except psutil.NoSuchProcess:
            pass
        else:
            alive_children.append(child)

    _gone, alive = psutil.wait_procs(alive_children, timeout=3)
    for child in alive:
        with contextlib.suppress(psutil.NoSuchProcess):
            child.kill()
