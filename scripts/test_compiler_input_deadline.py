import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from scripts import compiler_input_deadline as deadline
from scripts import compiler_input_readonly as readonly
from scripts import compiler_input_storage as storage
from scripts import compiler_input_task_prepare as task
from scripts import compiler_input_publication_prepare as publication
from scripts import compiler_input_consumer_prepare as consumer
from scripts.test_compiler_input_task_prepare import declaration, simulated
from scripts.test_compiler_input_publication_prepare import declaration as publication_declaration, run_simulated as publication_simulated
from scripts.test_compiler_input_consumer_prepare import declaration as consumer_declaration, run_simulated as consumer_simulated


class Clock:
    value = 1000

    def advance(self, seconds):
        self.value += seconds

    def now(self):
        return self.value


class PreparationDeadlineTest(unittest.TestCase):
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

    def test_slow_public_response_cannot_reset_socket_allowance(self):
        connected = threading.Event()
        class Trickle(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                connected.set()
                try:
                    for _ in range(40):
                        self.wfile.write(b' ')
                        self.wfile.flush()
                        time.sleep(.05)
                except (BrokenPipeError, ConnectionResetError):
                    pass
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Trickle)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        started = time.monotonic()
        try:
            with deadline.scope(1, .3, .1), self.assertRaises(deadline.Exhausted):
                readonly.public_gate({'readyURL': 'http://127.0.0.1:'+str(server.server_port)+'/readyz'})
            self.assertTrue(connected.is_set())
            self.assertLess(time.monotonic()-started, 1.8)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)

    def test_native_and_resource_write_share_transport_allowance(self):
        clock = Clock()
        caps = []
        def invoke(*args, **kwargs):
            caps.append(kwargs['timeout'])
            clock.advance(2)
            return types.SimpleNamespace(returncode=0, stdout='{}', stderr='')
        with patch.object(deadline.time, 'monotonic', clock.now), deadline.scope(), \
                patch.object(subprocess, 'run', side_effect=invoke), \
                patch.dict(task.os.environ, GITHUB_ACTIONS='true', GITHUB_REF='refs/heads/main'):
            clock.advance(535)
            task.native(declaration(), {'pod': 'neutral-api', 'container': 'api'}, 'plan', {})
            readonly.api_write(['create', '-f', '-'], {})
            with self.assertRaises(deadline.Exhausted):
                task.metadata(declaration())
        self.assertEqual(caps, [5, 3, 1])

    def test_all_three_coordinators_refuse_expired_plan_before_intent(self):
        for adapter, declaration_fn, simulation in (
                (task, declaration, simulated), (publication, publication_declaration, publication_simulated),
                (consumer, consumer_declaration, consumer_simulated)):
            with self.subTest(subject=adapter.SUBJECT), tempfile.TemporaryDirectory() as tmp:
                clock = Clock()
                c = declaration_fn()
                with simulation(c) as state, patch.object(deadline.time, 'monotonic', clock.now):
                    original = adapter.native.side_effect
                    def delayed(*args):
                        value = original(*args)
                        clock.advance(540)
                        deadline.check()
                        return value
                    with patch.object(adapter, 'native', side_effect=delayed):
                        state['evidence'] = Path(tmp)/'evidence.json'
                        with self.assertRaises(deadline.Exhausted):
                            adapter.execute(c, state['evidence'], True)
                    self.assertEqual(state['created'], [])
                    self.assertNotIn('session', state['events'])
                    report = json.loads(state['evidence'].read_text())
                    self.assertEqual(report['status'], 'completion-not-confirmed')
                    self.assertFalse(report['cleanup_confirmed'])
                self.assertFalse(deadline.active())

    def test_worker_queue_uses_remaining_work_then_exit_not_new_630_seconds(self):
        clock = Clock()
        c = declaration()
        with tempfile.TemporaryDirectory() as tmp, simulated(c) as state, \
                patch.object(deadline.time, 'monotonic', clock.now), patch.object(deadline.time, 'sleep', clock.advance):
            original = task.kube.side_effect
            def pending(namespace, verb, *args):
                if verb == 'get' and args[0] == 'pods':
                    return '{"items":[]}'
                return original(namespace, verb, *args)
            with patch.object(task, 'kube', side_effect=pending):
                state['evidence'] = Path(tmp)/'evidence.json'
                with self.assertRaises(deadline.Exhausted):
                    task.execute(c, state['evidence'], True)
            report = json.loads(state['evidence'].read_text())
            self.assertEqual(report['time_budget']['elapsed_seconds'], 540)
            self.assertEqual(report['worker_active_deadline_seconds'], 480)
            operations = task.r.api_write.call_args.args[1]
            self.assertIn(dict(op='test', path='/metadata/uid', value=report['job_uid']), operations)
            self.assertIn(dict(op='replace', path='/spec/activeDeadlineSeconds', value=480), operations)
            self.assertTrue(report['cleanup_confirmed'])
            self.assertTrue(report['terminal_receipt']['confirmed'])
            self.assertNotIn('native-confirm', state['events'])
            self.assertEqual(report['status'], 'completion-not-confirmed')
            self.assertLess(len(report['impact_samples']), 50)

    def test_late_confirmation_preserves_consumed_intent_and_uncertainty(self):
        clock = Clock()
        c = declaration()
        with tempfile.TemporaryDirectory() as tmp, simulated(c) as state, patch.object(deadline.time, 'monotonic', clock.now):
            original = task.native.side_effect
            def delayed(*args):
                value = original(*args)
                if args[2] == 'confirm':
                    clock.advance(540)
                    deadline.check()
                return value
            with patch.object(task, 'native', side_effect=delayed):
                state['evidence'] = Path(tmp)/'evidence.json'
                with self.assertRaises(deadline.Exhausted):
                    task.execute(c, state['evidence'], True)
            report = json.loads(state['evidence'].read_text())
            self.assertTrue(report['intent_attempted'])
            self.assertIn('intent_uid', report)
            self.assertEqual(report['status'], 'completion-not-confirmed')
            self.assertTrue(report['source_preserved'])
            self.assertFalse(report['task_activated'])
            self.assertFalse(report['retirement_authorized'])
            self.assertTrue(report['terminal_receipt']['confirmed'])

    def test_cleanup_waits_consume_same_exit_reserve(self):
        clock = Clock()
        c = declaration()
        uid = 'neutral-owned-uid'
        with patch.object(deadline.time, 'monotonic', clock.now), deadline.scope() as budget, \
                patch.object(deadline.time, 'sleep', clock.advance), \
                patch.object(task.r, 'delete_exact') as delete, \
                patch.object(task.index, 'kube', return_value=json.dumps({'items': [
                    {'metadata': {'ownerReferences': [{'uid': uid}]}}]})):
            clock.advance(540)
            budget.cleanup()
            with self.assertRaises(deadline.Exhausted):
                task.index.cleanup(c, 'neutral', 'neutral-session', uid, None, None)
            delete.assert_called_once_with(c, 'Job', 'neutral', uid)
            self.assertEqual(budget.snapshot()['elapsed_seconds'], 585)
            budget.receipt()
            self.assertEqual(deadline.check(), 15)

    def test_worker_deadline_drift_refuses_before_logs_or_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp, simulated(declaration()) as state:
            original = task.get.side_effect
            def changed(namespace, kind, name):
                value = original(namespace, kind, name)
                if kind == 'job':
                    value['spec']['activeDeadlineSeconds'] = 601
                return value
            with patch.object(task, 'get', side_effect=changed):
                state['evidence'] = Path(tmp)/'evidence.json'
                with self.assertRaises(task.r.Refusal):
                    task.execute(declaration(), state['evidence'], True)
            self.assertNotIn('logs', state['events'])
            self.assertNotIn('native-confirm', state['events'])
            self.assertTrue(state['cleaned'])

    def test_exit_timeout_keeps_receipt_reserve_and_cannot_claim_cleanup(self):
        for cleanup_elapsed, terminal_expected in ((590, True), (600, False)):
            clock = Clock()
            with self.subTest(elapsed=cleanup_elapsed), tempfile.TemporaryDirectory() as tmp, simulated(declaration()) as state, \
                    patch.object(deadline.time, 'monotonic', clock.now):
                def expire(*args):
                    clock.advance(cleanup_elapsed)
                    deadline.check()
                with patch.object(task.index, 'cleanup', side_effect=expire):
                    state['evidence'] = Path(tmp)/'evidence.json'
                    report = task.execute(declaration(), state['evidence'], True)
                self.assertFalse(report['cleanup_confirmed'])
                self.assertEqual(report['status'], 'completion-not-confirmed')
                self.assertEqual(report['terminal_receipt']['confirmed'], terminal_expected)
                self.assertEqual(report['time_budget']['total_seconds'], 600)
                self.assertEqual(state['terminal'] is not None, terminal_expected)
                self.assertTrue(report['intent_attempted'])

    def test_uncertain_intent_creation_still_attempts_independent_terminal(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as tmp, simulated(declaration()) as state, \
                patch.object(deadline.time, 'monotonic', clock.now):
            original = task.r.create.side_effect
            def uncertain(item, dry=False):
                created = original(item, dry)
                if not dry and item['kind'] == 'ConfigMap':
                    clock.advance(540)
                    deadline.check()
                return created
            with patch.object(task.r, 'create', side_effect=uncertain):
                state['evidence'] = Path(tmp)/'evidence.json'
                with self.assertRaises(deadline.Exhausted):
                    task.execute(declaration(), state['evidence'], True)
            report = json.loads(state['evidence'].read_text())
            self.assertTrue(report['intent_attempted'])
            self.assertNotIn('intent_uid', report)
            self.assertTrue(report['terminal_receipt']['confirmed'])
            self.assertFalse(report['cleanup_confirmed'])
            self.assertNotIn('session', state['events'])


if __name__ == '__main__':
    unittest.main()
