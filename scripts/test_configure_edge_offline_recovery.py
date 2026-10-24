import copy
import datetime
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

    def test_capacity_policy_and_preview_require_complete_code_and_transfer_evidence(self):
        config, _ = fixtures.OfflineConfigurationTests().fixture()
        hold = config['projection_policy']['offline_edges'][0]
        hold['recovery'] = {'hostname':'api.example.test','path':'/','probe_interval_seconds':300,'probe_timeout_seconds':5,'success_threshold':3,'failure_threshold':1,'freshness_seconds':1800}
        capacity = {'version':'edge-capacity-recovery-v1','traffic_class':'streaming','reference_edge_ids':['edge-b'],'minimum_bytes_per_second':262144,'max_elapsed_milliseconds':5000,'minimum_stable_seconds':600}
        hold['recovery']['capacity'] = capacity
        offline.validate(config)
        digest = offline.recovery_declaration_digest(hold)
        wire_hold=copy.deepcopy(hold);wire_hold['reason']='quota_exhausted';wire_hold.pop('expected_recovery_date',None)
        self.assertEqual(offline.recovery_declaration_digest(wire_hold),'sha256:cb5b318de682263959e35fd8548180d01ac29408af3777883036cc081b41ab03')
        legacy = copy.deepcopy(hold); legacy['recovery'].pop('capacity')
        self.assertNotEqual(digest, offline.recovery_declaration_digest(legacy))
        for key, value in [('reference_edge_ids',['edge-a']),('reference_edge_ids',['edge-b','edge-b']),('reference_edge_ids',['EDGE']),('minimum_stable_seconds',1801),('minimum_bytes_per_second',True),('max_elapsed_milliseconds',15001),('traffic_class','dynamic_api')]:
            changed = copy.deepcopy(config); changed['projection_policy']['offline_edges'][0]['recovery']['capacity'][key] = value
            with self.assertRaises(ValueError): offline.validate(changed)
        success = datetime.datetime.fromisoformat('2030-01-01T00:10:00+00:00')
        code = {k:k for k in ['node_uid','front_pod_uid','worker_pod_uid','service_uid','authority_uid']}
        code.update({k:'sha256:'+'a'*64 for k in ['authority_record_digest','worker_image_digest','front_image_digest','proxy_image_digest']})
        code.update(worker_source_sha='b'*40,slot='a',fence=1,bundle='bundle-a')
        e = {'checked_at':success.isoformat(),'report_digest':'sha256:'+'c'*64,'route_digest':'sha256:'+'d'*64,'reference_edge_id':'edge-b','bytes':1<<20,'elapsed_milliseconds':2000,'bytes_per_second':524288,'code':code}
        observation = {'first_success':'2030-01-01T00:00:00Z','last_result':'capacity_verified','successes':3,'failures':0,'capacity':e}
        self.assertTrue(offline.capacity_observation_ready(hold, observation, success))
        for key,value in [('capacity',None),('last_result','code_not_current'),('first_success',success.isoformat()),('successes',0),('failures',1)]:
            changed=copy.deepcopy(observation);changed[key]=value
            self.assertFalse(offline.capacity_observation_ready(hold,changed,success))
        for key,value in [('elapsed_milliseconds',6000),('bytes_per_second',float('nan')),('bytes',1),('reference_edge_id','edge-a'),('checked_at','2030-01-01T00:00:00Z'),('code',{})]:
            changed=copy.deepcopy(observation);changed['capacity'][key]=value
            self.assertFalse(offline.capacity_observation_ready(hold,changed,success))
        before={'business_snapshot_revision':'before','intent':{'dns':[{'hostname':'app.example.test'}]},'policy':{'minimum_healthy_edges':1},'issues':[]}
        after=copy.deepcopy(before)
        observation.update(edge_id='edge-a',declaration_digest=digest,admitted=True,last_attempt=success.isoformat(),last_success=success.isoformat(),proof_valid_until='2030-01-01T01:00:00Z')
        after['runtime_snapshot']={'dns_edge_endpoints':[{'edge_id':'edge-a'},{'edge_id':'edge-b'}],'facts':{'offline_edge_recovery':{'captured_at':success.isoformat(),'observations':[observation]}}}
        self.assertEqual(offline.validate_preview(before,after,config['projection_policy']),['edge-a','edge-b'])
        observation.pop('capacity')
        with self.assertRaises(ValueError):offline.validate_preview(before,after,config['projection_policy'])

if __name__ == '__main__': unittest.main()
