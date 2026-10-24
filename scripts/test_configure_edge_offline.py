import copy
import unittest

from scripts import configure_edge_offline as offline
from scripts.test_reconfigure_physical_dns import fixture as physical_fixture


class OfflineConfigurationTests(unittest.TestCase):
    def test_preview_failure_evidence_is_bounded_and_contains_no_values(self):
        before = {'intent': {'routes': [{'credential': 'private-before'} for index in range(300)]}}
        after = {'intent': {'routes': [{'credential': 'private-after'} for index in range(300)]}}
        evidence = offline.preview_evidence(before, after)
        self.assertEqual(len(evidence['changed_paths']), 256)
        self.assertEqual(evidence['changed_paths_omitted'], 44)
        self.assertNotEqual(evidence['before_intent_digest'], evidence['after_intent_digest'])
        self.assertNotIn('private-', str(evidence))

    def test_runtime_summary_is_explicit_and_preserves_all_admission_fields(self):
        valid = {'preview_mode':'runtime_summary','runtime_summary':{'schema':'fugue.runtime-preview-summary/v1','full_snapshot_digest':'sha256:'+'a'*64,'omitted_physical_evidence_count':1,'omitted_physical_evidence_bytes':1024},'omitted_runtime_fields':['selected_edge_group','decision_id','exclusion_evidence','runtime_snapshot.dns_selections[].physical_evidence'],'runtime_snapshot':{'dns_selections':[{'hostname':'app.example.test','candidates':[{'edge_id':'edge-b'}]}],'facts':{'offline_edge_recovery':{'observations':[{'edge_id':'edge-a','admitted':False}]}},'dns_edge_endpoints':[{'edge_id':'edge-b'}]}}
        original = copy.deepcopy(valid)
        self.assertEqual(offline.validate_runtime_summary(valid),valid['runtime_summary'])
        self.assertEqual(valid, original)
        for scenario in ['old-runtime','missing-summary','null-summary','bad-digest','omitted-facts','missing-omission','negative-bytes','too-many','bool-count','remaining-raw','empty-size']:
            changed = copy.deepcopy(valid)
            if scenario == 'old-runtime': changed['preview_mode']='runtime'
            elif scenario == 'missing-summary': changed.pop('runtime_summary')
            elif scenario == 'null-summary': changed['runtime_summary']=None
            elif scenario == 'bad-digest': changed['runtime_summary']['full_snapshot_digest']='unverified'
            elif scenario == 'omitted-facts': changed['omitted_runtime_fields'].append('runtime_snapshot.facts')
            elif scenario == 'missing-omission': changed['omitted_runtime_fields']=[]
            elif scenario == 'negative-bytes': changed['runtime_summary']['omitted_physical_evidence_bytes']=-1
            elif scenario == 'too-many': changed['runtime_summary']['omitted_physical_evidence_count']=2
            elif scenario == 'bool-count': changed['runtime_summary']['omitted_physical_evidence_count']=True
            elif scenario == 'remaining-raw': changed['runtime_snapshot']['dns_selections'][0]['physical_evidence']={}
            elif scenario == 'empty-size': changed['runtime_summary']['omitted_physical_evidence_bytes']=0
            with self.subTest(scenario=scenario), self.assertRaises(ValueError):offline.validate_runtime_summary(changed)
        empty=copy.deepcopy(valid);empty['runtime_summary'].update(omitted_physical_evidence_count=0,omitted_physical_evidence_bytes=0)
        offline.validate_runtime_summary(empty)

    def fixture(self):
        config, _, source = physical_fixture()
        config.pop('hostname')
        config.update(schema='fugue.edge-offline-reconfiguration/v1', baseline_mode='latest_verified_same_policy')
        config['precondition']['operation'] = 'edge_offline'
        source['dns_placement_mode'] = 'consumer_readiness'
        config['projection_policy'] = copy.deepcopy(source)
        config['projection_policy']['generation'] += '-offline'
        config['projection_policy']['offline_edges'] = [{'edge_id': 'edge-a', 'edge_group_id': 'edge-group-a', 'public_ipv4': '8.8.8.8', 'reason': 'maintenance', 'expected_recovery_date': '2020-01-01'}]
        return config, source

    def test_explicit_hold_and_resume_preserve_other_policy(self):
        config, source = self.fixture()
        offline.validate(config)
        self.assertEqual(offline.check_delta(source, config['projection_policy']), 'edge-a')
        resumed = copy.deepcopy(source)
        resumed['generation'] += '-resumed'
        self.assertEqual(offline.check_delta(config['projection_policy'], resumed), 'edge-a')
        for change in ['quorum', 'deadline', 'two', 'date']:
            changed = copy.deepcopy(config['projection_policy'])
            if change == 'quorum':
                changed['minimum_healthy_edges'] = 10
            elif change == 'deadline':
                changed['max_stale_seconds'] = 900
            elif change == 'two':
                changed['offline_edges'].append({**changed['offline_edges'][0], 'edge_id': 'edge-b'})
            else:
                changed['offline_edges'][0]['expected_recovery_date'] = '2026-02-30'
            with self.assertRaises(ValueError):
                offline.check_delta(source, changed)

    def test_preview_rejects_held_candidate_and_unrelated_changes(self):
        config, _ = self.fixture()
        before = {'business_snapshot_revision': 'before', 'intent': {'dns': [{'hostname': 'app.example.test'}]}, 'policy': {'minimum_healthy_edges': 1}, 'issues': []}
        after = copy.deepcopy(before)
        after['runtime_snapshot'] = {'dns_edge_endpoints': [{'edge_id': 'edge-b'}]}
        self.assertEqual(offline.validate_preview(before, after, config['projection_policy']), ['edge-b'])
        for field in ['candidate', 'intent', 'policy', 'issue']:
            changed = copy.deepcopy(after)
            if field == 'candidate':
                changed['runtime_snapshot']['dns_edge_endpoints'][0]['edge_id'] = 'edge-a'
            elif field == 'issue':
                changed['issues'] = [{'code': 'new_failure'}]
            else:
                changed[field] = {}
            with self.assertRaises(ValueError):
                offline.validate_preview(before, changed, config['projection_policy'])
