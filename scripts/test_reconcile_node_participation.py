import contextlib
import copy
import datetime as dt
import json
import unittest
from unittest.mock import patch

from scripts import reconcile_node_participation as p


def policy():
    return dict(schema='fugue.automatic-node-participation/v1', generation=1, namespace='system', origin='https://api.example.test',
                readers={'guardian':'guardian', 'canary-prober':'canary'}, members=[dict(node='intermittent', node_uid='physical-uid', group='pool-a', component='worker-a', front='front', workers={'a':'worker-a', 'b':'worker-b'}, reason='network_unavailable', adopt_hold=True, stable_seconds=300,
                probes=[{'host':'one.example.test','path':'/'}, {'host':'two.example.test','path':'/health'}])])


class Participant(p.Participant):
    def __init__(self):
        c = policy()
        super().__init__(c, c['members'][0])
        self.value = {'metadata':dict(name='intermittent', uid='physical-uid', resourceVersion='10', labels={'fugue.io/role.edge':'true', 'fugue.io/edge-group-id':'pool-a'}, annotations={'unrelated':'preserve'}), 'status':{'conditions':[{'type':'Ready','status':'Unknown'}]}}
        self.writes = 0
        self.allowed = True
        self.final_record = 'current'
        self.lease_entries = 0

    def get(self, kind, name):
        if kind == 'node': return copy.deepcopy(self.value)
        if kind == 'daemonset': return {'metadata':{'name':name, 'uid':name+'-uid'}, 'spec':{'updateStrategy':{'type':'OnDelete'}, 'template':{'metadata':{'labels':{'fugue.io/edge-group-id':'pool-a'}}}}}
        raise AssertionError((kind,name))

    def kube(self, *args, body=None):
        if args[:2] == ('get','nodes'):
            other = copy.deepcopy(self.value)
            other['metadata'].update(name='reference', uid='reference-uid')
            other['metadata']['annotations'] = {}
            return json.dumps({'items':[self.value,other]})
        if args[:2] == ('patch','node'):
            ops = json.loads(args[args.index('-p')+1])
            self.assert_fence(ops)
            self.value['metadata']['annotations'] = ops[2]['value']
            self.value['metadata']['resourceVersion'] = str(int(self.value['metadata']['resourceVersion'])+1)
            self.writes += 1
            return json.dumps(self.value)
        raise AssertionError(args)

    def assert_fence(self, ops):
        assert ops[0] == {'op':'test','path':'/metadata/uid','value':self.value['metadata']['uid']}
        assert ops[1] == {'op':'test','path':'/metadata/resourceVersion','value':self.value['metadata']['resourceVersion']}
        assert self.lease_entries > 0

    def readers(self): return 2
    def public_service(self): return None
    def approved_network_recovery(self): return self.allowed
    def recovery_config(self): return {'authorityRecord':self.final_record, 'authorityUID':'authority-uid'}

    @contextlib.contextmanager
    def leases(self):
        self.lease_entries += 1
        try: yield []
        finally: self.lease_entries -= 1


class FinalRecovery:
    def __init__(self,c): self.c=c
    def snapshot(self): return ({}, {'code':'current'}, {'code':'current'}, '', {}, {})


class ParticipationTests(unittest.TestCase):
    def test_policy_requires_bounded_uid_bound_automatic_intent(self):
        p.validate(policy())
        mutations=[lambda c:c.update(generation=True),lambda c:c.update(origin='http://api.example.test'),lambda c:c.update(origin='https://user:secret@api.example.test'),lambda c:c['members'][0].update(reason='maintenance'),lambda c:c['members'][0].update(stable_seconds=0),lambda c:c['members'][0].update(node_uid=''),lambda c:c['members'].append(copy.deepcopy(c['members'][0])),lambda c:c['members'][0]['probes'].pop(),lambda c:c['members'][0]['probes'][0].update(path='/\nunsafe')]
        for change in mutations:
            c=policy();change(c)
            with self.assertRaises(ValueError):p.validate(c)

    @patch.object(p,'Recovery',FinalRecovery)
    def test_offline_recovery_readmission_and_next_outage_require_no_manual_change(self):
        member=Participant()
        at=dt.datetime(2026,1,1,tzinfo=dt.timezone.utc)
        with patch.object(p,'now',return_value=at), patch.object(p,'reconcile',return_value={'verified':True}) as recover:
            self.assertEqual(member.run(True)['state'],'offline')
            self.assertIn(p.ANNOTATION,member.value['metadata']['annotations'])
            before=member.writes
            self.assertEqual(member.run(True)['state'],'offline')
            self.assertEqual(member.writes,before)
            member.value['status']['conditions'][0]['status']='True'
            self.assertEqual(member.run(True)['state'],'recovering')
            self.assertEqual(member.run(True)['state'],'recovering')
            recover.assert_not_called()
        with patch.object(p,'now',return_value=at+dt.timedelta(seconds=301)), patch.object(p,'reconcile',return_value={'verified':True}) as recover:
            self.assertEqual(member.run(True)['state'],'active')
            recover.assert_called_once()
            self.assertNotIn(p.ANNOTATION,member.value['metadata']['annotations'])
            self.assertEqual(member.value['metadata']['annotations']['unrelated'],'preserve')
            before=member.writes
            self.assertEqual(member.run(True)['state'],'active')
            self.assertEqual(member.writes,before)
            member.value['status']['conditions'][0]['status']='Unknown'
            self.assertEqual(member.run(True)['state'],'offline')
            self.assertIn(p.ANNOTATION,member.value['metadata']['annotations'])

    def test_preview_and_manual_maintenance_never_change_node(self):
        member=Participant()
        self.assertEqual(member.run(False)['state'],'offline')
        self.assertEqual(member.writes,0)
        member.value['metadata']['annotations'][p.ANNOTATION]=json.dumps(dict(p.hold(member.config),reason='maintenance'))
        with self.assertRaisesRegex(ValueError,'Manual'):member.run(True)
        self.assertEqual(member.writes,0)
        member.value['metadata']['uid']='replacement'
        with self.assertRaisesRegex(ValueError,'physical identity'):member.run(True)

    @patch.object(p,'Recovery',FinalRecovery)
    def test_failed_or_expired_network_evidence_keeps_hold(self):
        member=Participant();member.value['status']['conditions'][0]['status']='True'
        member.value['metadata']['annotations'][p.ANNOTATION]=p.canonical(p.hold(member.config))
        at=dt.datetime(2026,1,1,tzinfo=dt.timezone.utc)
        with patch.object(p,'now',return_value=at):member.run(True)
        with patch.object(p,'now',return_value=at+dt.timedelta(seconds=301)),patch.object(p,'reconcile',return_value={'verified':True}):
            member.allowed=False
            self.assertEqual(member.run(True)['state'],'recovering')
            self.assertIn(p.ANNOTATION,member.value['metadata']['annotations'])
            member.allowed=True
            with patch.object(FinalRecovery,'snapshot',return_value=({}, {'code':'current'}, {'code':'old'},'',{},{})):
                with self.assertRaisesRegex(ValueError,'catch-up'):member.run(True)
            self.assertIn(p.ANNOTATION,member.value['metadata']['annotations'])

    def test_node_local_recovery_readmits_without_legacy_workloads(self):
        member = Participant()
        first = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        public, snap = object(), {'activation': {'updated_at': first.isoformat()}, 'address': '9.9.9.9'}
        with patch.object(member, 'public_service', return_value=public), \
                patch.object(member, 'node_local_snapshot', return_value=snap), \
                patch.object(member, 'approved_network_recovery', return_value=True), \
                patch.object(member, 'require_settled_group'), \
                patch.object(p.worker_driver.front, 'proof', return_value={'edge': 'intermittent', 'group': 'pool-a'}) as probe, \
                patch.object(p, 'validate_retained_workload') as legacy:
            with patch.object(p, 'now', return_value=first):
                self.assertEqual(member.run(True)['state'], 'offline')
                member.value['status']['conditions'][0]['status'] = 'True'
                self.assertEqual(member.run(True)['state'], 'recovering')
            with patch.object(p, 'now', return_value=first + dt.timedelta(seconds=301)):
                self.assertEqual(member.run(True)['code']['mode'], 'node-local')
            self.assertNotIn(p.ANNOTATION, member.value['metadata']['annotations'])
            self.assertEqual(probe.call_count, 2)
            legacy.assert_not_called()


if __name__=='__main__':unittest.main()
