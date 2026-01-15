"""Helpers for interacting with child processes."""

from __future__ import annotations

import contextlib
import datetime
import fcntl
import os
import queue
import select
import selectors
import shlex
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, IO

import psutil

from cvise.utils import sigmonitor


# How many seconds to let a subprocess shut down after SIGTERM; afterwards SIGKILL will be sent.
_SIGTERM_TIMEOUT = 30

VLOG = bool(os.environ.get('VLOG'))


class ProcessEventNotifier:
    """Runs a subprocess and reports its PID as start/finish events on the PID queue.

    Intended to be used in multiprocessing workers, to let the main process know the unfinished children subprocesses
    that should be killed.
    """

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
            if VLOG:
                print(f'[{os.getpid()}] run_process launched pid={proc.pid}', file=sys.stderr)
            with _auto_kill_descendants(proc):
                stdout_data, stderr_data = self._communicate_with_sig_checks(proc, input, timeout)
            # print(f'[{os.getpid()}] run_process end pid={proc.pid}', file=sys.stderr)
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
        if os.name == 'posix':
            return self._communicate_with_sig_checks_posix(proc, input, timeout)
        else:
            return self._communicate_with_sig_checks_portable(proc, input, timeout)

    def _communicate_with_sig_checks_posix(
        self, proc: subprocess.Popen, input: bytes | None, timeout: float | None
    ) -> tuple[bytes, bytes]:
        # print(f'[{os.getpid()}] _communicate_with_sig_checks_posix: start', file=sys.stderr)
        max_time = None if timeout is None else time.monotonic() + timeout

        input_view = memoryview(input or b'')
        input_offset = 0

        write_chunk = select.PIPE_BUF

        sigmonitor.assert_sigchld_monitored()

        def handle_sigmonitor_fd(sigmonitor_sock: socket.socket, proc: subprocess.Popen) -> None:
            sigmonitor.handle_readable_wakeup_fd(sigmonitor_sock)
            proc.poll()
            print(
                f'[{os.getpid()} {datetime.datetime.now()}] handle_sigmonitor_fd: returncode={proc.returncode}',
                file=sys.stderr,
            )

        def handle_stdin_fd(fileobj: IO[bytes]) -> None:
            nonlocal input_offset
            try:
                written = os.write(fileobj.fileno(), input_view[input_offset : input_offset + write_chunk])
                # print(f'[{os.getpid()}] handle_stdin_fd: written={written}', file=sys.stderr)
                input_offset += written
                if input_offset >= len(input_view):
                    selector.unregister(fileobj)
                    fileobj.close()
            except OSError:
                print(f'[{os.getpid()} {datetime.datetime.now()}] handle_stdin_fd: closing', file=sys.stderr)
                selector.unregister(fileobj)
                fileobj.close()

        def handle_output_fd(fileobj: IO[bytes], chunks: list[bytes]) -> None:
            # TODO: use readv with preallocated buf
            chunk = os.read(fileobj.fileno(), 32768)
            # print(f'[{os.getpid()}] handle_output_fd: read={len(chunk)} fileobj={fileobj}', file=sys.stderr)
            if chunk:
                chunks.append(chunk)
            else:
                print(f'[{os.getpid()} {datetime.datetime.now()}] handle_output_fd: closing {fileobj}', file=sys.stderr)
                selector.unregister(fileobj)
                fileobj.close()

        selector = selectors.DefaultSelector()
        for sock in (sigmonitor.get_wakeup_sock(), sigmonitor.get_sigchld_sock()):
            selector.register(sock, selectors.EVENT_READ, data=(handle_sigmonitor_fd, (sock, proc)))

        if proc.stdin:
            selector.register(proc.stdin, selectors.EVENT_WRITE, data=(handle_stdin_fd, (proc.stdin,)))
        else:
            assert not input_view
        stdout_chunks: list[bytes] = []
        if proc.stdout:
            selector.register(proc.stdout, selectors.EVENT_READ, data=(handle_output_fd, (proc.stdout, stdout_chunks)))
        stderr_chunks: list[bytes] = []
        if proc.stderr:
            selector.register(proc.stderr, selectors.EVENT_READ, data=(handle_output_fd, (proc.stderr, stderr_chunks)))

        while len(selector.get_map()) > 2 or proc.returncode is None:
            remaining = None if max_time is None else max(0, max_time - time.monotonic())
            if remaining == 0:
                assert timeout is not None
                if proc.stdin and not proc.stdin.closed:
                    selector.unregister(proc.stdin)
                if proc.stdout and not proc.stdout.closed:
                    selector.unregister(proc.stdout)
                if proc.stderr and not proc.stderr.closed:
                    selector.unregister(proc.stderr)
                if VLOG:
                    print(
                        f'[{os.getpid()}] _communicate_with_sig_checks_posix: timeout pid={proc.pid}', file=sys.stderr
                    )
                raise subprocess.TimeoutExpired(
                    cmd=proc.args,
                    timeout=timeout,
                    output=b''.join(stdout_chunks) if stdout_chunks else None,
                    stderr=b''.join(stderr_chunks) if stderr_chunks else None,
                )
            # print(f'[{os.getpid()}] _communicate_with_sig_checks_posix: select map={len(selector.get_map())} returncode={proc.returncode} proc={id(proc)}', file=sys.stderr)
            events = selector.select(timeout=remaining)
            sigmonitor.maybe_raise_exc()
            for key, _mask in events:
                f, args = key.data
                f(*args)

        if VLOG:
            print(
                f'[{os.getpid()} {datetime.datetime.now()}] _communicate_with_sig_checks_posix: finished pid={proc.pid}',
                file=sys.stderr,
            )
        return b''.join(stdout_chunks), b''.join(stderr_chunks)

    def _communicate_with_sig_checks_portable(
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
                sigmonitor.maybe_raise_exc()
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
            if proc.stdin:
                proc.stdin.close()
            if proc.stderr:
                proc.stderr.close()
            if proc.stdout:
                proc.stdout.close()
            if VLOG:
                print(
                    f'[{os.getpid()} {datetime.datetime.now()}] run_process kill_subtree pid={proc.pid}',
                    file=sys.stderr,
                )
            _kill_subtree(proc.pid)
            proc.wait()
            if VLOG:
                print(
                    f'[{os.getpid()} {datetime.datetime.now()}] run_process killed_subtree pid={proc.pid}',
                    file=sys.stderr,
                )


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
            if VLOG:
                print(f'[{os.getpid()}] SIGTERM to pid={child.pid}', file=sys.stderr)
            child.terminate()
        except psutil.NoSuchProcess:
            pass
        else:
            alive_children.append(child)

    _gone, alive = psutil.wait_procs(alive_children, timeout=_SIGTERM_TIMEOUT)
    for child in alive:
        with contextlib.suppress(psutil.NoSuchProcess):
            if VLOG:
                print(f'[{os.getpid()}] SIGKILL to pid={child.pid}', file=sys.stderr)
            child.kill()
