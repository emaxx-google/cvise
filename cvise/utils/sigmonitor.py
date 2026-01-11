"""Helper for setting up signal handlers and reliably propagating them.

There's no default SIGTERM handler, which doesn't allow doing proper cleanup on
shutdown.

Meanwhile Python Standard Library already provides a default handler for SIGINT
that raises KeyboardInterrupt, in some cases it's not propagated - e.g.:

> Due to the precarious circumstances under which __del__() methods are
> invoked, exceptions that occur during their execution are ignored, <...>

Such situations, while rare, would result in C-Vise not terminating on the
Ctrl-C keystroke. This helper allows to prevent whether a signal was observer
and letting the code raise the exception to trigger the shutdown.
"""

from __future__ import annotations

import atexit
import concurrent.futures
import contextlib
import datetime
import os
import signal
import socket
import sys
from concurrent.futures import Future
from dataclasses import dataclass
from signal import SIGCHLD, SIGINT, SIGTERM


_SOCK_READ_BUF_SIZE = 1024

VLOG = bool(os.environ.get('VLOG'))


@dataclass(slots=True)
class _Context:
    sigchld_monitored: bool
    future: Future
    read_buf: bytearray
    read_view: memoryview
    # File descriptors for triggering the "wakeup" whenever any signal arrives.
    wakeup_read_sock: socket.socket
    wakeup_write_sock: socket.socket
    # File descriptors for notifying SIGINT/SIGTERM specifically.
    sigintterm_read_sock: socket.socket
    sigintterm_write_sock: socket.socket
    # File descriptors for notifying SIGCHLD specifically.
    sigchld_read_sock: socket.socket
    sigchld_write_sock: socket.socket
    # Whether a signal was observed that needs to be handled later.
    sigint_observed: bool = False
    sigterm_observed: bool = False


_context: _Context | None = None


def init(sigint: bool = True, sigchld: bool = True) -> None:
    global _context
    if _context is None:  # if called multiple times (typically in tests, only update flags)
        wakeup_socks = socket.socketpair()
        sigintterm_socks = socket.socketpair()
        sigchld_socks = socket.socketpair()

        for socks in (wakeup_socks, sigintterm_socks, sigchld_socks):
            for sock in socks:
                sock.setblocking(False)

        read_buf = bytearray(_SOCK_READ_BUF_SIZE)
        _context = _Context(
            sigchld_monitored=sigchld,
            future=Future(),
            read_buf=read_buf,
            read_view=memoryview(read_buf),
            wakeup_read_sock=wakeup_socks[0],
            wakeup_write_sock=wakeup_socks[1],
            sigintterm_read_sock=sigintterm_socks[0],
            sigintterm_write_sock=sigintterm_socks[1],
            sigchld_read_sock=sigchld_socks[0],
            sigchld_write_sock=sigchld_socks[1],
        )

        signal.set_wakeup_fd(_context.wakeup_write_sock.fileno(), warn_on_full_buffer=False)
        atexit.register(_release_socks)

    assert _context is not None
    _context.sigchld_monitored = sigchld

    # Overwrite old signal handlers (in tests, the old handler could've been installed by ourselves as well; calling it
    # would result in an infinite recursion).
    signal.signal(SIGTERM, _on_signal)
    signal.signal(SIGINT, _on_signal if sigint else signal.SIG_IGN)
    signal.signal(SIGCHLD, _on_signal if sigchld else signal.SIG_DFL)


def maybe_raise_exc() -> None:
    assert _context
    # If multiple signals occurred, prefer SIGTERM.
    if _context.sigterm_observed:
        raise _create_exception(SIGTERM)
    elif _context.sigint_observed:
        raise _create_exception(SIGINT)


def get_future() -> Future:
    assert _context is not None
    return _context.future


def get_wakeup_sock() -> socket.socket:
    assert _context is not None
    return _context.wakeup_read_sock


def get_sigintterm_sock() -> socket.socket:
    assert _context is not None
    return _context.sigintterm_read_sock


def get_sigchld_sock() -> socket.socket:
    assert _context is not None
    return _context.sigchld_read_sock


def assert_sigchld_monitored() -> None:
    assert _context is not None
    assert _context.sigchld_monitored


def handle_readable_wakeup_fd(sock: socket.socket) -> None:
    """To be called when the corresponding FD is readable."""
    if VLOG:
        os.write(
            sys.stderr.fileno(),
            f'[{os.getpid()} {datetime.datetime.now()}] handle_readable_wakeup_fd: sock={sock.fileno()}\n'.encode(),
        )
    # Drain the socket.
    assert _context is not None
    try:
        nbytes = os.readv(sock.fileno(), (_context.read_buf,))
        if VLOG:
            os.write(
                sys.stderr.fileno(),
                f'[{os.getpid()} {datetime.datetime.now()}] handle_readable_wakeup_fd: pumped bytes={_context.read_buf[:nbytes]}\n'.encode(),
            )
    except OSError:
        return  # data was read by another thread or shutdown started

    # In case of the common wakeup FD, also set corresponding global flags and copy the notifications into the dedicated
    # sockets.
    if sock != _context.wakeup_read_sock:
        return
    contents = _context.read_view[:nbytes]
    if SIGCHLD in contents:
        _notify_sock(_context.sigchld_write_sock)
    if SIGINT in contents:
        _set_future_exception(_create_exception(SIGINT))
        _notify_sock(_context.sigintterm_write_sock)
    if SIGTERM in contents:
        _set_future_exception(_create_exception(SIGTERM))
        _notify_sock(_context.sigintterm_write_sock)


def signal_observed_for_testing() -> bool:
    assert _context is not None
    return _context.sigint_observed or _context.sigterm_observed


def _release_socks() -> None:
    if _context is None:
        return
    signal.set_wakeup_fd(-1)
    for sock in (
        _context.wakeup_read_sock,
        _context.wakeup_write_sock,
        _context.sigintterm_read_sock,
        _context.sigintterm_write_sock,
        _context.sigchld_read_sock,
        _context.sigchld_write_sock,
    ):
        sock.close()


def _notify_sock(sock: socket.socket) -> None:
    try:
        sock.send(b'\0')
    except BlockingIOError:
        pass  # discard the notification - it's sufficient to have nonzero number of pending bytes


def _on_signal(signum: int, frame) -> None:
    if VLOG:
        os.write(sys.stderr.fileno(), f'[{os.getpid()} {datetime.datetime.now()}] on_signal {signum}\n'.encode())
    assert _context
    if signum == SIGCHLD:
        return  # no action needed (its only purpose is to notify the fds)

    repeated = _context.sigterm_observed or _context.sigint_observed
    if signum == SIGINT:
        _context.sigint_observed = True
    elif signum == SIGTERM:
        _context.sigterm_observed = True

    exc = _create_exception(signum)
    _set_future_exception(exc)
    if repeated:
        raise exc


def _create_exception(signum: int) -> BaseException:
    if signum == SIGINT:
        return KeyboardInterrupt()
    elif signum == SIGTERM:
        return SystemExit(1)
    else:
        raise ValueError(f'Unexpected signal {signum}')


def _set_future_exception(exc: BaseException) -> None:
    with contextlib.suppress(concurrent.futures.InvalidStateError):  # no done() to avoid races
        _context.future.set_exception(exc)
