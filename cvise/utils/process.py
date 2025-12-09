"""Helpers for interacting with child processes."""

from __future__ import annotations

import contextlib
import os
import queue
import selectors
import shlex
import socket
import subprocess
import threading
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import psutil

from cvise.utils import sigmonitor


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
