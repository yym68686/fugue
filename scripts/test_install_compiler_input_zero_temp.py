import copy
from pathlib import Path
import types
import unittest
from unittest.mock import patch

from scripts import install_compiler_input_zero_temp as z


def config():
    return dict(version=1, namespace='neutral', cluster='neutral-db', clusterUID='a'*8+'-aaaa-aaaa-aaaa-'+'a'*12,
                primary='neutral-db-1', primaryUID='b'*8+'-bbbb-bbbb-bbbb-'+'b'*12,
                systemID='123', database='neutral', container='postgres')


class ZeroTempInstallerTest(unittest.TestCase):
    def test_accepts_only_fixed_identity_not_sql_or_settings(self):
        for field, value in [('version', True), ('systemID', "1';SELECT 1;--"), ('systemID', str(1 << 64)),
                             ('namespace', 'other;'), ('primaryUID', ''), ('sql', 'SELECT 1')]:
            c = config()
            c[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                z.validate(c)

    def test_no_argument_tightening_and_rolled_back_preview(self):
        sql = z.sql(config(), True)
        self.assertIn("pg_catalog.set_config('temp_file_limit','0',true)", sql)
        self.assertIn('zero_temp_v1() RETURNS void', sql)
        self.assertIn('SECURITY DEFINER', sql)
        self.assertIn('SET search_path=pg_catalog', sql)
        for unsafe in ['GRANT SET', 'ALTER ROLE', 'ALTER SYSTEM', 'UPDATE ']:
            self.assertNotIn(unsafe, sql)
        self.assertTrue(sql.endswith('COMMIT;\n'))
        self.assertIn("system_identifier::text='123'", sql)
        preview = z.sql(config(), False)
        self.assertTrue(preview.endswith('ROLLBACK;\n'))
        self.assertEqual(sql[:-len('COMMIT;\n')], preview[:-len('ROLLBACK;\n')])

    def test_identity_and_health_checked_before_and_after(self):
        c = config()
        cluster = dict(metadata=dict(uid=c['clusterUID']), spec=dict(instances=1),
                       status=dict(systemID='123', currentPrimary=c['primary'], targetPrimary=c['primary'],
                                   phase='Cluster in healthy state', readyInstances=1))
        pod = dict(metadata=dict(uid=c['primaryUID'], labels={'cnpg.io/cluster': c['cluster']}),
                   status=dict(containerStatuses=[dict(name='postgres', ready=True)],
                               conditions=[dict(type='Ready', status='True')]))
        with patch.object(z, 'get', side_effect=[cluster, pod, cluster, pod]), patch.object(z.subprocess, 'run') as run:
            run.return_value = types.SimpleNamespace(returncode=0, stdout='1\n', stderr='')
            self.assertTrue(z.install(c, True)['applied'])
            self.assertEqual(run.call_count, 1)
            self.assertEqual(run.call_args.kwargs['timeout'], 30)
        for name in ['uid', 'primary', 'ready', 'deleting', 'poduid', 'label']:
            bad, badpod = copy.deepcopy(cluster), copy.deepcopy(pod)
            if name == 'uid': bad['metadata']['uid'] = 'changed'
            if name == 'primary': bad['status']['targetPrimary'] = 'other'
            if name == 'ready': bad['status']['readyInstances'] = 0
            if name == 'deleting': bad['metadata']['deletionTimestamp'] = 'now'
            if name == 'poduid': badpod['metadata']['uid'] = 'changed'
            if name == 'label': badpod['metadata']['labels']['cnpg.io/cluster'] = 'other'
            with self.subTest(name=name), patch.object(z, 'get', side_effect=[bad, badpod]), patch.object(z.subprocess, 'run') as run:
                with self.assertRaises(ValueError): z.install(c, True)
                run.assert_not_called()

    def test_failure_is_not_retried_or_exposed(self):
        with patch.object(z, 'ready'), patch.object(z.subprocess, 'run') as run:
            run.return_value = types.SimpleNamespace(returncode=1, stdout='', stderr='secret SQL detail')
            with self.assertRaisesRegex(ValueError, 'not confirmed') as exc:
                z.install(config(), True)
            self.assertNotIn('secret', str(exc.exception))
            self.assertEqual(run.call_count, 1)

    def test_schema_ci_lane_installs_with_supersession_check(self):
        ci = (Path(__file__).resolve().parent.parent / '.github/workflows/ci.yml').read_text()
        lane = ci[ci.index('\n  deploy_schema:'):ci.index('\n  deploy_telemetry:')]
        self.assertIn('python3 -m unittest scripts.test_install_compiler_input_zero_temp', lane)
        self.assertIn('git diff --exit-code HEAD origin/main', lane)
        self.assertIn('-m scripts.install_compiler_input_zero_temp "$config" --apply', lane)
        self.assertLess(lane.index('install_compiler_input_zero_temp "$config" --apply'), lane.index('uses: ./.github/actions/deploy-declarative-component'))


if __name__ == '__main__':
    unittest.main()
