"""One elapsed-time allowance for a preparation coordinator and its exit.

This bounds local transport and waiting, not remote termination or storage
growth. A timeout leaves the attempt uncertain; only independent readback can
confirm cleanup. Outside a preparation scope, existing callers are unchanged.
"""
from contextlib import contextmanager
from contextvars import ContextVar
import math
import subprocess
import sys
import time


class Exhausted(TimeoutError):
    """A credential-free failure; never include command arguments or output."""


_current = ContextVar('compiler_input_preparation_deadline', default=None)


class Budget:
    def __init__(self, seconds, exit_seconds, receipt_seconds):
        if not (0 < receipt_seconds < exit_seconds < seconds <= 600):
            raise ValueError('invalid preparation time allowance')
        self.started = time.monotonic()
        self.ends = self.started + seconds
        self.work_ends = self.ends - exit_seconds
        self.cleanup_ends = self.ends - receipt_seconds
        self.phase = 'work'
        self.seconds = seconds
        self.exit_seconds = exit_seconds
        self.receipt_seconds = receipt_seconds

    def remaining(self):
        end = {'work': self.work_ends, 'cleanup': self.cleanup_ends,
               'receipt': self.ends}[self.phase]
        return max(0, end - time.monotonic())

    def check(self):
        remaining = self.remaining()
        if remaining <= 0:
            raise Exhausted('input preparation '+self.phase+' time allowance exhausted; outcome unconfirmed')
        return remaining

    def cleanup(self):
        if self.phase != 'work':
            raise ValueError('preparation exit already started')
        self.phase = 'cleanup'

    def receipt(self):
        if self.phase != 'cleanup':
            raise ValueError('preparation cleanup has not finished')
        self.phase = 'receipt'

    def snapshot(self):
        return dict(total_seconds=self.seconds, exit_reserve_seconds=self.exit_seconds,
                    receipt_reserve_seconds=self.receipt_seconds, phase=self.phase,
                    elapsed_seconds=max(0, time.monotonic()-self.started),
                    remaining_seconds=self.remaining(), remote_termination_proven=False)


@contextmanager
def scope(seconds=600, exit_seconds=60, receipt_seconds=15):
    # Nesting must not reset or enlarge a caller's allowance or exit phase.
    if active():
        raise ValueError('preparation time allowance already active')
    budget = Budget(seconds, exit_seconds, receipt_seconds)
    token = _current.set(budget)
    try:
        yield budget
    finally:
        _current.reset(token)


def active():
    return _current.get() is not None


def check():
    budget = _current.get()
    return budget.check() if budget else None


def run(args, *, timeout, **kwargs):
    budget = _current.get()
    if not budget:
        return subprocess.run(args, timeout=timeout, **kwargs)
    timeout = min(timeout, budget.check())
    try:
        result = subprocess.run(args, timeout=timeout, **kwargs)
    except subprocess.TimeoutExpired:
        raise Exhausted('input preparation subprocess time allowance exhausted; outcome unconfirmed') from None
    budget.check()
    return result


def pause(seconds):
    budget = _current.get()
    time.sleep(min(seconds, budget.check()) if budget else seconds)
    check()


def worker_seconds(maximum, ticket_remaining):
    # Keep the existing 30s ticket margin and reserve a bounded patch call plus
    # confirmation time within the SAME work deadline. Neither is a guarantee
    # that remote scheduling, confirmation or cleanup will succeed.
    budget = _current.get()
    if not budget or budget.phase != 'work':
        raise ValueError('worker requires the preparation work allowance')
    seconds = math.floor(min(maximum, budget.check()-60, ticket_remaining-60))
    if seconds < 1:
        raise Exhausted('input preparation setup exhausted worker allowance; outcome unconfirmed')
    return seconds


def public_read(url):
    # A socket timeout is reset by a trickling response and does not bound DNS.
    # Contain that read in a local child, charged to the same elapsed allowance.
    # The public URL carries no credentials; stdout is capped at 16KiB + 1.
    code = ('import sys,urllib.request\n'
            'with urllib.request.urlopen(sys.argv[1],timeout=10) as r:\n'
            ' if r.status != 200: sys.exit(1)\n'
            ' sys.stdout.buffer.write(r.read(16385))\n')
    result = run([sys.executable, '-c', code, url], timeout=10, capture_output=True)
    if result.returncode:
        raise ValueError('public readiness failed')
    return result.stdout
