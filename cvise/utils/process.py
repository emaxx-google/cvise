"""Helpers for interacting with child processes."""

from __future__ import annotations

import contextlib
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
from typing import Any

import psutil

from cvise.utils import sigmonitor


_selector = None


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
            # print(f'[{os.getpid()}] run_process begin pid={proc.pid}', file=sys.stderr)
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
        max_time = None if timeout is None else time.monotonic() + timeout
        stdout_chunks = []
        stderr_chunks = []

        input_view = memoryview(input) if input else None
        input_offset = 0

        write_chunk = select.PIPE_BUF

        wakeup_fd = sigmonitor.get_wakeup_fd()

        global _selector
        if not _selector:
            _selector = selectors.DefaultSelector()
            _selector.register(wakeup_fd, selectors.EVENT_READ)
        selector = _selector

        if proc.stdin:
            if input_view:
                selector.register(proc.stdin, selectors.EVENT_WRITE)
                # print(f'[{os.getpid()}] listen stdin', file=sys.stderr)
            else:
                proc.stdin.close()
        else:
            assert not input_view

        if proc.stdout:
            selector.register(proc.stdout, selectors.EVENT_READ, data=stdout_chunks)
            # print(f'[{os.getpid()}] listen stdout {proc.stdout}', file=sys.stderr)
        if proc.stderr:
            selector.register(proc.stderr, selectors.EVENT_READ, data=stderr_chunks)
            # print(f'[{os.getpid()}] listen stderr {proc.stderr}', file=sys.stderr)

        exited = False
        while len(selector.get_map()) > 1 or not exited:
            poll_timeout = None if max_time is None else max(0, max_time - time.monotonic())
            if poll_timeout == 0:
                assert timeout is not None
                if proc.stdin and not proc.stdin.closed:
                    selector.unregister(proc.stdin)
                if proc.stdout and not proc.stdout.closed:
                    selector.unregister(proc.stdout)
                if proc.stderr and not proc.stderr.closed:
                    selector.unregister(proc.stderr)
                raise subprocess.TimeoutExpired(
                    cmd=proc.args,
                    timeout=timeout,
                    output=b''.join(stdout_chunks) if stdout_chunks else None,
                    stderr=b''.join(stderr_chunks) if stderr_chunks else None,
                )
            # print(f'[{os.getpid()}] select() timeout={poll_timeout}', file=sys.stderr)
            events = selector.select(timeout=poll_timeout)

            for key, mask in events:
                fileobj = key.fileobj
                fd = key.fd
                if fd == wakeup_fd:
                    # print(f'[{os.getpid()}] select(): wakeup_fd', file=sys.stderr)
                    sigmonitor.eat_wakeup_fd_notifications()
                    sigmonitor.maybe_retrigger_action()
                    exited = proc.poll() is not None
                    # print(f'[{os.getpid()}] select(): {"" if exited else "NOT YET "}exited', file=sys.stderr)
                elif mask & selectors.EVENT_READ:
                    chunk = os.read(fd, 32768)
                    # print(
                    # f'[{os.getpid()}] select(): read {"stdout" if key.data is stdout_chunks else "stderr"} {fileobj} len={len(chunk)}',
                    # file=sys.stderr,
                    # )
                    key.data.append(chunk)
                    if not chunk:
                        selector.unregister(fileobj)
                        fileobj.close()
                        # print(
                        # f'[{os.getpid()}] select(): close {"stdout" if key.data is stdout_chunks else "stderr"} {fileobj}',
                        # file=sys.stderr,
                        # )
                elif mask & selectors.EVENT_WRITE:
                    try:
                        written = os.write(fd, input_view[input_offset : input_offset + write_chunk])
                        # print(f'[{os.getpid()}] select(): wrote len={written}', file=sys.stderr)
                        input_offset += written
                        if input_offset >= len(input_view):
                            selector.unregister(fileobj)
                            fileobj.close()
                            # print(f'[{os.getpid()}] select(): close stdin', file=sys.stderr)
                    except OSError:
                        selector.unregister(fileobj)
                        fileobj.close()
                        # print(f'[{os.getpid()}] select(): close stdin', file=sys.stderr)

        # print(f'[{os.getpid()}] finish', file=sys.stderr)
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
            if proc.stdin:
                proc.stdin.close()
            if proc.stderr:
                proc.stderr.close()
            if proc.stdout:
                proc.stdout.close()
            # print(f'[{os.getpid()}] run_process kill_subtree pid={proc.pid}', file=sys.stderr)
            _kill_subtree(proc.pid)
            proc.wait()
            # print(f'[{os.getpid()}] run_process killed_subtree pid={proc.pid}', file=sys.stderr)


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
