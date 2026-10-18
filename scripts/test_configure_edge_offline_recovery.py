import copy
import unittest
from scripts import configure_edge_offline as offline
from scripts import test_configure_edge_offline as fixtures

class OfflineRecoveryConfigurationTests(unittest.TestCase):
    def test_recovered_preview_binds_runtime_typed_declaration_digest(self):
        config, _ = fixtures.OfflineConfigurationTests().fixture()
        hold = config['projection_policy']['offline_edges'][0]
        hold.update(reason='quota_exhausted', expected_recovery_date='2030-01-01')
        hold['recovery'] = {'hostname':'api.example.test','path':'/a&b','probe_interval_seconds':300,'probe_timeout_seconds':5,'success_threshold':3,'failure_threshold':1,'freshness_seconds':1800}
        wire_digest = 'sha256:5f2a440655b056227813ab50cd3fe4ff772704af0fdc5b4f07c1c0e13ddb0dc0'
        self.assertEqual(offline.recovery_declaration_digest(hold), wire_digest)
        self.assertNotEqual(offline.digest(hold), wire_digest)
        before = {'business_snapshot_revision':'before','intent':{'dns':[{'hostname':'app.example.test'}]},'policy':{'minimum_healthy_edges':1},'issues':[]}
        after = copy.deepcopy(before)
        observation = {'edge_id':'edge-a','declaration_digest':wire_digest,'last_attempt':'2030-01-01T00:00:00Z','last_success':'2030-01-01T00:00:00Z','proof_valid_until':'2030-01-01T01:00:00Z','successes':3,'failures':0,'admitted':True}
        after['runtime_snapshot'] = {'dns_edge_endpoints':[{'edge_id':'edge-a'},{'edge_id':'edge-b'}],'facts':{'offline_edge_recovery':{'captured_at':'2030-01-01T00:00:01Z','observations':[observation]}}}
        self.assertEqual(offline.validate_preview(before, after, config['projection_policy']), ['edge-a','edge-b'])
        for key,value in [('declaration_digest',offline.digest(hold)),('proof_valid_until','2029-12-31T23:59:59Z'),('last_attempt','2030-01-01T00:10:00Z'),('admitted',False),('failures',1)]:
            changed = copy.deepcopy(after)
            changed['runtime_snapshot']['facts']['offline_edge_recovery']['observations'][0][key] = value
            with self.assertRaises(ValueError):
                offline.validate_preview(before, changed, config['projection_policy'])

    def test_recovery_policy_is_bounded_and_explicit(self):
        config, _ = fixtures.OfflineConfigurationTests().fixture()
        hold = config['projection_policy']['offline_edges'][0]
        hold['recovery'] = {'hostname':'api.example.test','path':'/','probe_interval_seconds':300,'probe_timeout_seconds':5,'success_threshold':3,'failure_threshold':1,'freshness_seconds':1800}
        offline.validate(config)
        for key,value in [('probe_interval_seconds',299),('success_threshold',1),('freshness_seconds',600)]:
            bad = copy.deepcopy(config); bad['projection_policy']['offline_edges'][0]['recovery'][key] = value
            with self.assertRaises(ValueError): offline.validate(bad)
        for path in ['//x','/x?bad=1','']:
            bad = copy.deepcopy(config); bad['projection_policy']['offline_edges'][0]['recovery']['path'] = path
            with self.assertRaises(ValueError): offline.validate(bad)

if __name__ == '__main__': unittest.main()
