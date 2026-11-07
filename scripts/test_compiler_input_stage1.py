import copy
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import compiler_input_stage1 as stage
from scripts.test_compiler_input_hot_index import declaration as index_declaration, metadata


UID = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'


def declaration():
    a = index_declaration()
    a['page']['limit'] = 1
    b = {k: a[k] for k in stage.index.COMMON}
    b.update(version=3, readCount=0)
    return {'version': 1, 'executionAuthorized': True, 'index': a, 'inventory': b}


def backing(total=1 << 40, free=None):
    minimum = total//4+(16 << 30)+(192 << 20)+(256 << 20)
    return {'total_bytes': total, 'free_bytes': minimum+stage.EXTRA_RESERVE+(128 << 20) if free is None else free,
            'available_bytes': 0, 'capacity_gate_passed': True}


def activity():
    return {UID: {'write_failures': 0, 'budget_fallbacks': 0, 'category': '', 'outer_writer':
        dict(schema='fugue.compiler-input-writer.observation.v1', attempts=0, in_flight=0,
             successful_returns=0, deadline_failures=0, canceled_failures=0, other_failures=0)}}


@contextmanager
def simulated(c, failure=''):
    state = {'phase': 'start', 'created': [], 'inventory_calls': 0, 'checks': [], 'items': {}}
    primary = {'status': {'containerStatuses': [{'name': 'postgres', 'restartCount': 0}]}}
    def primary_gate(*args):
        loss = 0
        if failure == 'cumulative-space':
            loss = {'page': 80 << 20, 'inventory': 160 << 20}.get(state['phase'], 0)
        return {}, primary, (4 << 30)-loss
    def meta(*args):
        m = metadata()
        if failure == 'cumulative-wal':
            m['wal_lsn'] = 'A/'+format(0x100+{'page': 150 << 20, 'inventory': 300 << 20}.get(state['phase'], 0), 'X')
        if failure == 'wal-reset' and state['phase'] == 'inventory':
            m['wal_reset'] = 'changed'
        return m
    def writers(*args):
        value = activity()
        if failure == 'cumulative-writer':
            n = {'page': 3, 'inventory': 6}.get(state['phase'], 0)
            value[UID]['write_failures'] = n
            value[UID]['outer_writer'].update(attempts=n, deadline_failures=n)
        return value
    def space(*args, **kwargs):
        value = backing()
        if failure == 'continuous-reserve' and state['phase'] == 'inventory':
            value['free_bytes'] -= (128 << 20)+1
        return value
    def create(item):
        state['created'].append(item)
        state['items'][item['metadata']['name']] = {**copy.deepcopy(item), 'metadata': {**item['metadata'], 'uid': UID}}
        return {'metadata': {'uid': UID}}
    def get(namespace, kind, name):
        value = copy.deepcopy(state['items'][name])
        if failure in ('intent-readback', 'terminal-readback') and name.endswith('-'+failure.split('-')[0].replace('intent', 'receipt')):
            value['data']['declaration-sha256'] = 'changed'
        return value
    def page(config, path, apply, cumulative_watch, evidence_limit):
        assert 16384 <= evidence_limit <= stage.EVIDENCE_LIMIT-(2 << 20)
        state['phase'] = 'page'
        path.write_text('{}')
        cumulative_watch()
        require_status = 'index-page-committed' if apply else 'preview-passed'
        return {'status': require_status, 'cleanup_confirmed': failure != 'page-cleanup',
                'job': 'index-job', 'job_uid': UID, 'declaration_sha256': 'b'*64,
                'terminal_receipt': {'confirmed': failure != 'page-receipt'},
                'result': {'source_bytes': (8 << 20)+1 if failure == 'page-budget' else 1, 'index_bytes': 1}}
    def inventory(config, path, apply, cumulative_watch, output_limit, evidence_limit):
        state['inventory_calls'] += 1
        state['phase'] = 'inventory'
        assert output_limit*4 <= evidence_limit <= stage.EVIDENCE_LIMIT
        if failure == 'inventory-exception':
            raise RuntimeError('synthetic log retrieval failure')
        if failure == 'evidence-budget':
            path.write_text('x'*stage.EVIDENCE_LIMIT)
            raise stage.readonly.Refusal('synthetic evidence limit')
        path.write_text('{}')
        cumulative_watch()
        complete = failure != 'structured-failure'
        return {'status': 'diagnostic-finished' if apply else 'preview-passed', 'cleanup_confirmed': failure != 'inventory-cleanup',
                'job': 'inventory-job', 'job_uid': UID, 'declaration_sha256': 'c'*64,
                'execution': {'terminal': {'metadata_complete': failure != 'missing-terminal',
                    'signal': 9 if failure == 'terminal-signal' else 0,
                    'exit_code': 1 if failure == 'terminal-exit' else (0 if complete else 1)}},
                'result': {'graph_command_completed': complete}}
    with ExitStack() as stack:
        stack.enter_context(patch.dict(stage.os.environ, GITHUB_ACTIONS='true', GITHUB_REF='refs/heads/main', GITHUB_SHA='a'*40))
        for obj, key, function in [(stage, 'command', lambda *a: 'a'*40), (stage, 'kube', lambda *a: '{"items":[]}'),
                (stage.impact, 'node_gate', lambda *a: {}), (stage.readonly, 'primary_gate', primary_gate),
                (stage.impact, 'backing_gate', space), (stage.readonly, 'serving_gate', lambda *a: ({}, {}, {'exact': True})),
                (stage.index, 'metadata', meta), (stage.impact, 'api_activity', writers),
                (stage.impact, 'api_log_signals', lambda *a: {}), (stage.readonly, 'create', create),
                (stage.readonly, 'get', get), (stage.index, 'execute', page), (stage.readonly, 'execute', inventory)]:
            stack.enter_context(patch.object(obj, key, side_effect=function))
        yield state


class StageOneTest(unittest.TestCase):
    def test_exact_scope_and_shared_identity(self):
        c = declaration()
        self.assertEqual(stage.validate(c), c)
        changes = [('version', True), ('executionAuthorized', False), ('retry', 1)]
        for key, value in changes:
            bad = copy.deepcopy(c)
            bad[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                stage.validate(bad)
        for key, value in [('nodeUID', 'b'*8+'-'+('b'*4+'-')*3+'b'*12), ('minimumFreeBytes', 1 << 30), ('readCount', 1)]:
            bad = copy.deepcopy(c)
            bad['inventory'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                stage.validate(bad)
        c['index']['page']['limit'] = 2
        with self.assertRaises(ValueError):
            stage.validate(c)

    def test_continuous_reserve_uses_fresh_total_and_free_not_available(self):
        v = backing()
        stage.check_backing(v, True)
        v['free_bytes'] -= 1
        with self.assertRaises(ValueError):
            stage.check_backing(v, True)
        stage.check_backing(v, False)
        # Additional filesystem total raises the floor, even if old free is
        # unchanged. Available zero does not replace or inflate free.
        v = backing()
        v['total_bytes'] += 1 << 30
        with self.assertRaises(ValueError):
            stage.check_backing(v, True)

    def test_two_steps_keep_original_cumulative_baseline(self):
        for failure in ('cumulative-space', 'cumulative-wal', 'wal-reset', 'cumulative-writer', 'continuous-reserve'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp, simulated(declaration(), failure) as state:
                path = Path(tmp)/'stage.json'
                with self.assertRaises(ValueError):
                    stage.execute(declaration(), path, True)
                result = json.loads(path.read_text())
                self.assertEqual(result['status'], 'completion-not-confirmed')
                self.assertTrue(result['intent_consumed'])
                self.assertEqual(state['inventory_calls'], 1)
                self.assertEqual(len(state['created']), 2)
                terminal = json.loads(state['created'][-1]['data']['receipt.json'])
                self.assertEqual(terminal['status'], 'completion-not-confirmed')

    def test_page_cleanup_or_intent_uncertainty_prevents_inventory(self):
        for failure in ('intent-readback', 'page-cleanup', 'page-receipt', 'page-budget'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp, simulated(declaration(), failure) as state:
                with self.assertRaises(ValueError):
                    stage.execute(declaration(), Path(tmp)/'stage.json', True)
                self.assertEqual(state['inventory_calls'], 0)

    def test_incomplete_inventory_cleanup_or_termination_is_never_success(self):
        for failure in ('inventory-cleanup', 'missing-terminal', 'terminal-signal', 'terminal-exit', 'inventory-exception'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp, simulated(declaration(), failure) as state:
                path = Path(tmp)/'stage.json'
                with self.assertRaises((ValueError, RuntimeError)):
                    stage.execute(declaration(), path, True)
                result = json.loads(path.read_text())
                self.assertEqual(result['status'], 'completion-not-confirmed')
                self.assertTrue(result['terminal_receipt']['confirmed'])
                self.assertFalse(result['cleanup_confirmed'])
                self.assertEqual(state['inventory_calls'], 1)

    def test_evidence_overflow_still_persists_independent_uncertain_terminal(self):
        with tempfile.TemporaryDirectory() as tmp, simulated(declaration(), 'evidence-budget') as state:
            with self.assertRaisesRegex(ValueError, 'evidence budget'):
                stage.execute(declaration(), Path(tmp)/'stage.json', True)
            terminal = json.loads(state['created'][-1]['data']['receipt.json'])
            self.assertEqual(terminal['status'], 'completion-not-confirmed')
            self.assertEqual(terminal['files']['stage-inventory.json']['bytes'], stage.EVIDENCE_LIMIT)
            self.assertLess(len(state['created'][-1]['data']['receipt.json'].encode()), 32768)

    def test_structured_inventory_failure_is_consumed_not_complete_h(self):
        with tempfile.TemporaryDirectory() as tmp, simulated(declaration(), 'structured-failure') as state:
            path = Path(tmp)/'stage.json'
            report = stage.execute(declaration(), path, True)
            self.assertEqual(report['status'], 'stage-attempt-finished')
            self.assertTrue(report['terminal_receipt']['confirmed'])
            self.assertFalse(report['inventory']['graph_command_completed'])
            self.assertFalse(report['retirement_authorized'])
            self.assertEqual(state['inventory_calls'], 1)
            terminal = json.loads(state['created'][-1]['data']['receipt.json'])
            self.assertEqual(terminal['intent_uid'], UID)
            self.assertEqual(terminal['index']['job_uid'], UID)
            self.assertEqual(terminal['inventory']['job_uid'], UID)
            self.assertEqual(terminal['files']['stage-index.json']['sha256'], stage.hashlib.sha256(b'{}').hexdigest())
            self.assertEqual(terminal['window_sha256'], stage.readonly.identity({k: report[k] for k in ('baseline', 'impact_samples')}))
            with self.assertRaises(FileExistsError):
                stage.execute(declaration(), path, True)

    def test_preview_never_consumes_or_runs_native_apply(self):
        with tempfile.TemporaryDirectory() as tmp, simulated(declaration()) as state:
            report = stage.execute(declaration(), Path(tmp)/'stage.json', False)
            self.assertEqual(report['status'], 'preview-passed')
            self.assertEqual(state['created'], [])

    def test_terminal_readback_failure_remains_uncertain(self):
        with tempfile.TemporaryDirectory() as tmp, simulated(declaration(), 'terminal-readback'):
            report = stage.execute(declaration(), Path(tmp)/'stage.json', True)
            self.assertFalse(report['terminal_receipt']['confirmed'])

    def test_parent_growth_refuses_before_crossing_reserved_evidence_allowance(self):
        with tempfile.TemporaryDirectory() as tmp, simulated(declaration()) as state:
            path = Path(tmp)/'stage.json'
            with patch.object(stage.impact, 'api_log_signals', return_value={'oversized': 'x'*stage.PARENT_EVIDENCE_LIMIT}):
                with self.assertRaisesRegex(ValueError, 'parent evidence budget exceeded'):
                    stage.execute(declaration(), path, True)
            self.assertEqual(state['inventory_calls'], 0)
            self.assertLessEqual(sum(p.stat().st_size for p in Path(tmp).iterdir()), stage.EVIDENCE_LIMIT)
            terminal = json.loads(state['created'][-1]['data']['receipt.json'])
            self.assertEqual(terminal['status'], 'completion-not-confirmed')
            self.assertLessEqual(path.stat().st_size, stage.PARENT_EVIDENCE_LIMIT)

    def test_untrusted_context_refuses_before_any_resources(self):
        with tempfile.TemporaryDirectory() as tmp, simulated(declaration()) as state, patch.dict(stage.os.environ, GITHUB_ACTIONS='false'):
            with self.assertRaises(ValueError):
                stage.execute(declaration(), Path(tmp)/'stage.json', True)
            self.assertEqual(state['created'], [])

    def test_window_watchdog_is_finite_and_never_refreshes_baseline(self):
        with simulated(declaration()):
            window = stage.Window(declaration()['index'], lambda *a: None)
            original = copy.deepcopy(window.baseline)
            for _ in range(50):
                window.check()
            with self.assertRaisesRegex(ValueError, 'watchdog exhausted'):
                window.check()
            self.assertEqual(window.baseline, original)

    def test_maintenance_gate_excludes_only_exact_current_child(self):
        job = {'metadata': {'uid': UID}}
        def listing(namespace, *args):
            return json.dumps({'items': [job] if args[3] == 'fugue.pro/purpose='+stage.readonly.PURPOSE else []})
        with patch.object(stage, 'kube', side_effect=listing):
            stage.maintenance_gate(declaration()['index'], UID)
            for uid in ('', 'different-child'):
                with self.subTest(uid=uid), self.assertRaisesRegex(ValueError, 'concurrent maintenance'):
                    stage.maintenance_gate(declaration()['index'], uid)
        with simulated(declaration()), patch.object(stage, 'kube', side_effect=listing):
            window = stage.Window(declaration()['index'], lambda *a: None)
            with self.assertRaisesRegex(ValueError, 'concurrent maintenance'):
                window.check()


if __name__ == '__main__':
    unittest.main()
