import subprocess
import sys
import time
import types
import unittest
from unittest.mock import patch

from scripts import compiler_input_deadline as deadline
from scripts import compiler_input_storage as storage


class Clock:
    value = 1000

    def advance(self, seconds):
        self.value += seconds

    def now(self):
        return self.value


class OperationDeadlineTest(unittest.TestCase):
    def test_one_clock_shrinks_calls_and_reserves_exit_without_reset(self):
        clock = Clock()
        observed = []
        def invoke(*args, **kwargs):
            observed.append(kwargs['timeout'])
            clock.advance(40 if len(observed) == 4 else 20)
            return types.SimpleNamespace(returncode=0, stdout='ok', stderr='')
        with patch.object(deadline.time, 'monotonic', clock.now), patch.object(subprocess, 'run', side_effect=invoke):
            with deadline.scope() as budget:
                clock.advance(510)
                self.assertEqual(storage.command(['neutral']), 'ok')
                with self.assertRaises(deadline.Exhausted):
                    storage.command(['neutral'])
                budget.cleanup()
                storage.command(['neutral'])
                budget.receipt()
                with self.assertRaises(deadline.Exhausted):
                    storage.command(['neutral'])
                with self.assertRaises(deadline.Exhausted):
                    storage.command(['must-not-start'])
                self.assertEqual(observed, [30, 10, 30, 30])
                self.assertEqual(budget.snapshot()['total_seconds'], 600)
                self.assertFalse(budget.snapshot()['remote_termination_proven'])
            # The scope cannot leak into an unrelated operation.
            storage.command(['unscoped'], timeout=75)
        self.assertEqual(observed[-1], 75)

    def test_nested_scope_cannot_extend_work_or_reopen_exit(self):
        clock = Clock()
        with patch.object(deadline.time, 'monotonic', clock.now), deadline.scope() as budget:
            clock.advance(539)
            self.assertEqual(deadline.check(), 1)
            with self.assertRaises(ValueError):
                with deadline.scope():
                    self.fail('nested timer started')
            budget.cleanup()
            self.assertEqual(deadline.check(), 46)
            budget.receipt()
            self.assertEqual(deadline.check(), 61)
            with self.assertRaises(ValueError):
                budget.cleanup()
        self.assertFalse(deadline.active())

    def test_timeout_does_not_expose_command_stdin_or_output(self):
        error = subprocess.TimeoutExpired(['private-argument'], 1, output='private-output', stderr='private-stderr')
        with deadline.scope(), patch.object(subprocess, 'run', side_effect=error):
            with self.assertRaises(deadline.Exhausted) as caught:
                deadline.run(['private-argument'], input='private-stdin', timeout=30)
        self.assertNotIn('private', str(caught.exception))

    def test_real_local_child_is_reaped_on_timeout(self):
        started = time.monotonic()
        with deadline.scope(1, .3, .1):
            with self.assertRaises(deadline.Exhausted):
                deadline.run([sys.executable, '-c', 'import time;time.sleep(30)'], timeout=.15, capture_output=True)
        self.assertLess(time.monotonic()-started, 2)
