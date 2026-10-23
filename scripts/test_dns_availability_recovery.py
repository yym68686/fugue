import copy
import datetime
import json
import unittest

from scripts import configure_dynamic_quality as quality
from scripts import dns_availability_recovery as recovery
from scripts.test_configure_dynamic_quality import AvailabilityConfigurationTests
from scripts.test_reconfigure_physical_dns import Ledger, fixture as baseline_fixture


def fixture():
    config, previous = AvailabilityConfigurationTests().fixture()
    config['precondition']['operation'] = 'dns_availability_recovery'
    config['baseline_mode'] = 'declared'
    _, old, _ = baseline_fixture()
    route = {'hostname':'app.example.test','traffic_class':'streaming','policy':copy.deepcopy(config['projection_policy']['dns_query_policy']['dynamic_quality']['policy'])}
    previous['dns_query_policy']['physical_routes'] = [copy.deepcopy(route)]
    config['projection_policy']['dns_query_policy']['physical_routes'] = [copy.deepcopy(route)]
    old['dns_policy_digest'] = quality.digest(previous)
    config['precondition']['previous_policy']['content_hash'] = quality.digest(old)
    ledger = RecoveryLedger(dict(config,hostname='app.example.test'),old,previous)
    full = ledger.authorities[('release_set','global','full')]
    at = (recovery.now()-datetime.timedelta(seconds=30)).isoformat()
    full['release'].update(verification_state='serving_unverified',pinned_rollback_generation='full-current',rollback_target_generation='full-current',released_at=at)
    full['messages']=[{'message_type':'rollback','release_id':'serving-release','artifact_id':'serving-artifact','artifact_kind':'release_set','scope_key':'global','release_channel':'full','generation':'full-current','created_at':at}]
    ledger.lkg.update(verified_by_release_id='previous-positive-release',generation='full-current')
    ledger.preview.update(intent={'generation':'unchanged'},business_snapshot_revision='fixed')
    ledger.preview['policy']['dns_answer_rules'] = [{'hostname':'app.example.test'}]
    ledger.preview['runtime_snapshot'] = {'dns_selections':[{'hostname':'app.example.test','candidates':[{'edge_id':'edge-a'}]}],'facts':{'dynamic_quality_coverage':{'mode':'all_dynamic','query_count':1,'hostnames':{'app.example.test':'availability_only_quality_unavailable'}}}}
    return config, previous, ledger


class RecoveryLedger(Ledger):
    def __call__(self,method,path,body=None):
        if path.startswith('/v1/admin/platform-config/routes/project?'):
            if '&view=configuration' in path:
                return {'preview_mode':'configuration','business_snapshot_revision':'old' if 'old-policy' in path else 'new','intent':{'generation':'unchanged'},'issues':[{'code':'runtime_evidence_not_collected'}]}
            if 'producer_policy_artifact_id=old-policy' in path:
                raise AssertionError('recovery depended on successful old runtime/DNS')
        return super().__call__(method,path,body)


class AvailabilityRecoveryTests(unittest.TestCase):
    def test_preview_rejection_retains_bounded_differences_without_raw_values(self):
        before = {'preview_mode': 'configuration', 'business_snapshot_revision': 'snapshot-a', 'intent': {'routes': [{'upstream_url': 'private-before'}]}}
        after = copy.deepcopy(before)
        runtime = copy.deepcopy(before)
        runtime['intent']['routes'][0]['upstream_url'] = 'private-after'
        evidence = recovery.preview_evidence(before, after, runtime)
        self.assertTrue(evidence['comparisons']['before_after']['equal'])
        self.assertEqual(evidence['comparisons']['after_runtime']['changed_paths'], ['/intent/routes/0/upstream_url'])
        self.assertNotIn('private-before', json.dumps(evidence))
        self.assertNotIn('private-after', json.dumps(evidence))
        before['intent']['routes'] = [{'upstream_url': 'a'}]*200
        runtime['intent']['routes'] = [{'upstream_url': 'b'}]*200
        evidence = recovery.preview_evidence(before, before, runtime)['comparisons']['after_runtime']
        self.assertEqual(len(evidence['changed_paths']), 128)
        self.assertEqual(evidence['omitted_paths'], 72)

    def test_actual_preview_failure_is_saved_before_guard_refuses_activation(self):
        config, _, ledger = fixture()
        ledger.preview['intent'] = {'generation': 'changed-business'}
        evidence = []
        with self.assertRaises(ValueError): quality.publish_once(config, ledger, evidence.append)
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0]['phase'], 'validate_preview')
        self.assertFalse(evidence[0]['producer_activated'])
        self.assertFalse(evidence[0]['preview_comparison']['comparisons']['after_runtime']['equal'])
        self.assertEqual(ledger.release_bodies, [])

    def test_explicit_same_artifact_renewal_resolves_current_transaction_fences(self):
        for phase in ['before', 'during_preview']:
            with self.subTest(phase=phase):
                config, _, ledger = fixture()
                config['baseline_mode'] = recovery.FOLLOW_SAME_ARTIFACT
                original = copy.deepcopy(config)
                def renew(_=None):
                    full = ledger.authorities[('release_set', 'global', 'full')]
                    full['release'].update(id='renewed-release', fencing_token=10)
                    full['messages'][0]['release_id'] = 'renewed-release'
                    ledger.lkg['verification_evidence_hash'] = 'sha256:'+'e'*64
                    ledger.expected_precondition = copy.deepcopy(original['precondition'])
                    ledger.expected_precondition['serving_full'].update(release_id='renewed-release', fencing_token=10)
                    ledger.expected_precondition['verification_evidence_hash'] = 'sha256:'+'e'*64
                    ledger.on_capture = None
                if phase == 'before': renew()
                else: ledger.on_capture = renew
                result = quality.publish_once(config, ledger, lambda _: None)
                precondition = ledger.release_bodies[0]['producer_reconfiguration']
                self.assertEqual(precondition['serving_full']['release_id'], 'renewed-release')
                self.assertEqual(precondition['serving_full']['fencing_token'], 10)
                self.assertEqual(precondition['verification_evidence_hash'], 'sha256:'+'e'*64)
                self.assertEqual(precondition['previous_policy'], original['precondition']['previous_policy'])
                self.assertEqual(config, original)
                self.assertTrue(result['producer_activated'])
                self.assertFalse(result['routing_acceptance_complete'])

    def test_same_artifact_resolution_never_adopts_new_content_or_weak_evidence(self):
        for scenario in ['content', 'artifact', 'regressed', 'same-fence-other-release', 'same-release-other-fence', 'producer', 'ordinary-unverified', 'missing-ledger', 'expired', 'foreign-lkg', 'bad-evidence', 'other-operation']:
            with self.subTest(scenario=scenario):
                config, _, ledger = fixture()
                config['baseline_mode'] = recovery.FOLLOW_SAME_ARTIFACT
                full = ledger.authorities[('release_set', 'global', 'full')]
                if scenario == 'content': full['artifact']['content_hash'] = 'sha256:'+'f'*64
                if scenario == 'artifact': full['artifact']['id'] = 'other-artifact'; full['release']['artifact_id'] = 'other-artifact'
                if scenario == 'regressed': full['release']['fencing_token'] = 8
                if scenario == 'same-fence-other-release': full['release']['id'] = 'other-release'
                if scenario == 'same-release-other-fence': full['release']['fencing_token'] = 10
                if scenario == 'producer': ledger.authorities[('policy_snapshot', 'platform-config-producer', 'shadow')]['release']['fencing_token'] += 1
                if scenario == 'ordinary-unverified': del full['release']['rollback_target_generation']
                if scenario == 'missing-ledger': full['messages'] = []
                if scenario == 'expired': ledger.lkg['expires_at'] = (recovery.now()-datetime.timedelta(seconds=1)).isoformat()
                if scenario == 'foreign-lkg': ledger.lkg['artifact_id'] = 'foreign'
                if scenario == 'bad-evidence': ledger.lkg['verification_evidence_hash'] = 'unverified'
                if scenario == 'other-operation': config['precondition']['operation'] = 'dns_availability'
                with self.assertRaises(ValueError): quality.publish_once(config, ledger, lambda _: None)
                self.assertEqual(ledger.release_bodies, [])

    def test_fenced_configuration_uses_real_rollback_without_runtime_success(self):
        config, _, ledger = fixture()
        full = copy.deepcopy(ledger.authorities[('release_set','global','full')])
        lkg = copy.deepcopy(ledger.lkg)
        result = quality.publish_once(config,ledger,lambda _:None)
        self.assertTrue(result['producer_activated'])
        self.assertFalse(result['routing_acceptance_complete'])
        self.assertFalse(result['coverage']['runtime_verified'])
        self.assertEqual(full,ledger.authorities[('release_set','global','full')])
        self.assertEqual(lkg,ledger.lkg)
        self.assertEqual(ledger.release_bodies[0]['producer_reconfiguration'],config['precondition'])

    def test_invalid_or_changed_baseline_never_publishes(self):
        for scenario in ['unverified','no_message','wrong_message','wrong_time','foreign_producer','override','expired','wrong_lkg','evidence','full_fence','producer_binding','implicit_renewal','capture_race']:
            with self.subTest(scenario=scenario):
                config, _, ledger = fixture()
                full = ledger.authorities[('release_set','global','full')]
                if scenario == 'unverified': del full['release']['rollback_target_generation']
                elif scenario == 'no_message': full['messages']=[]
                elif scenario == 'wrong_message': full['messages'][0]['artifact_id']='foreign'
                elif scenario == 'wrong_time': full['messages'][0]['created_at']=recovery.now().isoformat()
                elif scenario == 'foreign_producer': full['release']['released_by_id']='foreign'
                elif scenario == 'override': full['release']['override_mode']='soft'
                elif scenario == 'expired': ledger.lkg['expires_at']=(recovery.now()-datetime.timedelta(seconds=1)).isoformat()
                elif scenario == 'wrong_lkg': ledger.lkg['artifact_id']='foreign'
                elif scenario == 'evidence': ledger.lkg['verification_evidence_hash']='sha256:'+'f'*64
                elif scenario == 'full_fence': full['release']['fencing_token']+=1
                elif scenario == 'producer_binding': full['artifact']['metadata']['producer_policy_release_id']='foreign'
                elif scenario == 'implicit_renewal': config['baseline_mode']='latest_verified_same_policy'
                elif scenario == 'capture_race': ledger.on_capture=lambda _:full['release'].update(fencing_token=99)
                with self.assertRaises(ValueError):quality.publish_once(config,ledger,lambda _:None)
                self.assertEqual(ledger.release_bodies,[])

    def test_only_one_new_explicit_pool_hold(self):
        hold={'edge_id':'edge-b','edge_group_id':'edge-group-test','public_ipv4':'8.8.8.8','reason':'quota_exhausted','recovery':{'hostname':'app.example.test','path':'/','probe_interval_seconds':300,'probe_timeout_seconds':5,'success_threshold':3,'failure_threshold':1,'freshness_seconds':1800}}
        for scenario in ['success','no_hold','outside_pool','no_recovery','maintenance','two_holds','remove_old','change_old','other_constraint']:
            with self.subTest(scenario=scenario):
                config, previous, _=fixture();next=copy.deepcopy(config['projection_policy'])
                old_hold={**hold,'edge_id':'edge-c'}
                previous['offline_edges']=[copy.deepcopy(old_hold)]
                next['offline_edges']=[copy.deepcopy(hold),copy.deepcopy(old_hold)]
                if scenario=='no_hold':next['offline_edges']=[copy.deepcopy(old_hold)]
                elif scenario=='outside_pool':next['dns_query_policy']['availability_routes'][0]['policy']['ordered_edge_ids']=['edge-a']
                elif scenario=='no_recovery':del next['offline_edges'][0]['recovery']
                elif scenario=='maintenance':next['offline_edges'][0]['reason']='maintenance'
                elif scenario=='two_holds':next['offline_edges'].insert(0,{**hold,'edge_id':'edge-a'})
                elif scenario=='remove_old':next['offline_edges'].pop()
                elif scenario=='change_old':next['offline_edges'][1]['reason']='maintenance'
                elif scenario=='other_constraint':next['dns_query_policy']['minimum_ttl_seconds']=30
                if scenario in ['success','no_hold','no_recovery']:self.assertEqual(recovery.check_delta(previous,next),'app.example.test')
                else:
                    with self.assertRaises(ValueError):recovery.check_delta(previous,next)

    def test_configuration_draft_does_not_substitute_for_successor_evidence(self):
        config, _, ledger=fixture()
        before=ledger('GET','/v1/admin/platform-config/routes/project?view=configuration&view=configuration')
        for scenario in ['changed_business','new_issue','no_coverage','outside_pool']:
            after,runtime=copy.deepcopy(before),copy.deepcopy(ledger.preview)
            if scenario=='changed_business':after['intent']={}
            elif scenario=='new_issue':after['issues'].append({'code':'invalid'})
            elif scenario=='no_coverage':runtime['runtime_snapshot']['facts']={}
            else:runtime['runtime_snapshot']['dns_selections'][0]['candidates']=[{'edge_id':'foreign'}]
            with self.assertRaises(ValueError):recovery.validate_preview(before,after,runtime,config['projection_policy'],'app.example.test')


if __name__=='__main__':unittest.main()
