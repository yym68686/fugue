import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts import reconcile_cell_trust as trust


def fixture():
    def key(n):return base64.urlsafe_b64encode(bytes([n])*32).decode().rstrip('=')
    cell='cell-test';edge='node-a';prefix='fugue-'+cell+'-'
    material={prefix+'signing':{'keyring.json':trust.canonical({'schema':'edge-control-group-bundle-signing-keyring/v1','generation':1,'group':{'edge_group_id':cell,'primary_key_id':'signing-v1','primary_key':key(1)}})},
              prefix+'inventory':{'keyring.json':trust.canonical({'schema':'edge-inventory-platform-identity-keyring/v1','generation':1,'edge_group_id':cell,'active_key_id':'inventory-v1','active_key':key(2)})},
              prefix+'recovery':{'keyring.json':trust.canonical({'schema':'edge-control-group-recovery-keyring/v1','generation':1,'edge_group_id':cell,'keys':[{'key_id':'recovery-v1','secret':key(3),'not_before_unix':1790640000,'not_after_unix':1800640000,'revoked':False}]})},
              prefix+'token':{'token':key(4)},
              prefix+'readers':{'keyring.json':trust.canonical({'schema':'edge-control-group-bundle-reader-keyring/v1','generation':1,'edge_group_id':cell,'credentials':[{'credential_id':'reader-v1','edge_id':edge,'token_digest':'sha256:'+hashlib.sha256(key(4).encode()).hexdigest(),'not_before_unix':1790640000,'not_after_unix':1800640000,'revoked':False}]})}}
    purposes={'signing':'bundle-signing','inventory':'inventory-writer','recovery':'recovery','token':'reader-token/'+edge,'readers':'bundle-readers'}
    config={'schema':'fugue.cell-trust/v1','namespace':'test-system','cell':cell,'generation':1,'previousGeneration':0,'previousDigest':'','edgeIds':[edge],
            'secrets':[{'name':prefix+n,'purpose':purpose,'digest':trust.digest(material[prefix+n])} for n,purpose in purposes.items()]}
    return config,material


class CellTrustTests(unittest.TestCase):
    def test_dedicated_encrypted_package_cannot_select_foreign_secrets(self):
        config, material = fixture()
        config['materialSecret'] = 'FUGUE_EDGE_CELL_TRUST_CELL_TEST'
        self.assertEqual(config, trust.validate(config))
        self.assertEqual(5, len(trust.resources(config, material)))
        for value in ['FUGUE_EDGE_CELL_TRUST_CELL_OTHER', 'FUGUE_API_KEY', '', None]:
            config['materialSecret'] = value
            with self.assertRaises(ValueError):
                trust.validate(config)

    def test_material_and_readers_are_explicitly_bound(self):
        config,material=fixture()
        resources=trust.resources(config,material)
        self.assertEqual(len(resources),5)
        for item in resources:
            self.assertEqual(item['metadata']['labels']['fugue.io/authority-cell-id'],'cell-test')
        for mode in ['foreign','token','shared','extra','generation','membership']:
            with self.subTest(mode=mode):
                c,m=copy.deepcopy(config),copy.deepcopy(material)
                name='fugue-cell-test-inventory';ring=trust.strict_json(m[name]['keyring.json'])
                if mode=='foreign':ring['edge_group_id']='cell-foreign'
                if mode=='shared':ring['active_key']=trust.strict_json(m['fugue-cell-test-signing']['keyring.json'])['group']['primary_key']
                if mode=='generation':ring['generation']=2
                if mode=='extra':ring['arbitrary']=True
                m[name]['keyring.json']=trust.canonical(ring)
                if mode=='token':m['fugue-cell-test-token']['token']=base64.urlsafe_b64encode(bytes([5])*32).decode().rstrip('=')
                if mode=='membership':c['edgeIds']=['node-b']
                for item in c['secrets']:item['digest']=trust.digest(m[item['name']])
                with self.assertRaises(ValueError):trust.resources(c,m)

    def test_existing_keys_cannot_be_adopted_or_rotated(self):
        config,material=fixture();desired=trust.resources(config,material)[0]
        current=copy.deepcopy(desired);current['metadata'].update(uid='uid-a',resourceVersion='1')
        trust.inspect(current,desired,config)
        for mode in ['foreign','drift','deleting','replacement','new-generation','same-generation-drift']:
            with self.subTest(mode=mode):
                bad=copy.deepcopy(current)
                if mode=='foreign':bad['metadata']['labels']['app.kubernetes.io/managed-by']='other'
                if mode=='drift':bad['data']['keyring.json']=base64.b64encode(b'changed').decode()
                if mode=='deleting':bad['metadata']['deletionTimestamp']='later'
                if mode=='replacement':del bad['metadata']['uid']
                if mode=='new-generation':bad['metadata']['annotations'][trust.GENERATION]='2'
                if mode=='same-generation-drift':bad['metadata']['annotations'][trust.DECLARATION]='sha256:'+'a'*64
                with self.assertRaises(ValueError):trust.inspect(bad,desired,config)
        config['generation'],config['previousGeneration']=2,1
        with self.assertRaises(ValueError):trust.validate(config)

    def test_partial_initial_write_is_resumable_without_key_regeneration(self):
        config,material=fixture();state={};writes=[];fail=[True]
        def read(item):return copy.deepcopy(state.get(item['metadata']['name']))
        def write(args,body):
            if '--dry-run=server' in args:return ''
            self.assertEqual(args[0],'create')
            if len(writes)==2 and fail[0]:raise RuntimeError('simulated transport failure')
            value=copy.deepcopy(body);value['metadata'].update(uid='uid-'+str(len(writes)),resourceVersion='1');state[value['metadata']['name']]=value;writes.append(value['metadata']['name']);return ''
        with patch.object(trust,'read',side_effect=read),patch.object(trust,'kubectl',side_effect=write):
            with self.assertRaises(RuntimeError):trust.reconcile(config,material)
            self.assertEqual(len(writes),2)
            fail[0]=False;trust.reconcile(config,material);trust.reconcile(config,material,check=True)
            self.assertEqual(len(writes),5);self.assertEqual(len(set(writes)),5)

    def test_preflight_failure_performs_no_writes(self):
        config,material=fixture();writes=[]
        def write(args,body):
            if '--dry-run=server' not in args:writes.append(body)
            if body['metadata']['name'].endswith('recovery'):raise RuntimeError('admission rejected')
            return ''
        with patch.object(trust,'read',return_value=None),patch.object(trust,'kubectl',side_effect=write):
            with self.assertRaises(RuntimeError):trust.reconcile(config,material)
            self.assertEqual(writes,[])




def add_reader(previous, material, edge='node-b', key_byte=5):
    config, material = copy.deepcopy(previous), copy.deepcopy(material)
    config.update(schema='fugue.cell-trust/v2', operation='add-reader',
                  generation=previous['generation'] + 1,
                  previousGeneration=previous['generation'], previousDigest=trust.digest(previous))
    config['edgeIds'] = sorted(previous['edgeIds'] + [edge])
    token_name = 'fugue-cell-test-token-' + edge
    token = base64.urlsafe_b64encode(bytes([key_byte]) * 32).decode().rstrip('=')
    material[token_name] = {'token': token}
    config['secrets'].append({'name': token_name, 'purpose': 'reader-token/' + edge,
                              'digest': trust.digest(material[token_name])})
    reader = next(s for s in config['secrets'] if s['purpose'] == 'bundle-readers')
    ring = json.loads(material[reader['name']]['keyring.json'])
    ring['generation'] = config['generation']
    ring['credentials'].append({'credential_id': 'reader-' + edge, 'edge_id': edge,
                                'token_digest': 'sha256:' + hashlib.sha256(token.encode()).hexdigest(),
                                'not_before_unix': 1790640000, 'not_after_unix': 1800640000, 'revoked': False})
    ring['credentials'].sort(key=lambda c: c['edge_id'])
    material[reader['name']]['keyring.json'] = trust.canonical(ring)
    reader['digest'] = trust.digest(material[reader['name']])
    return config, material


class FakeSecrets:
    """Small API double enforcing create uniqueness and JSON Patch preconditions."""
    def __init__(self, config, material):
        self.state = {}
        self.writes = []
        self.before = None
        for item in trust.resources(config, material):
            item['metadata'].update(uid='uid-' + item['metadata']['name'], resourceVersion='1')
            self.state[item['metadata']['name']] = item

    def read(self, item):
        return copy.deepcopy(self.state.get(item['metadata']['name']))

    def kubectl(self, args, body):
        dry = '--dry-run=server' in args
        name = body['metadata']['name'] if args[0] == 'create' else args[2]
        if self.before:
            self.before(name, dry)
        if args[0] == 'create':
            if name in self.state:
                raise RuntimeError('already exists')
            result = copy.deepcopy(body)
            result['metadata'].update(uid='new-' + name, resourceVersion='1')
        elif args[0] == 'patch':
            result = copy.deepcopy(self.state[name])
            for op in body:
                path = [p.replace('~1', '/').replace('~0', '~') for p in op['path'].split('/')[1:]]
                parent = result
                for key in path[:-1]:
                    parent = parent[key]
                if op['op'] == 'test':
                    if parent[path[-1]] != op['value']:
                        raise RuntimeError('CAS conflict')
                elif op['op'] == 'replace':
                    if path[-1] not in parent:
                        raise RuntimeError('missing patch target')
                    parent[path[-1]] = copy.deepcopy(op['value'])
                else:
                    raise AssertionError('unexpected patch operation')
            result['metadata']['resourceVersion'] = str(int(result['metadata']['resourceVersion']) + 1)
        else:
            raise AssertionError('unexpected write verb')
        if not dry:
            self.writes.append((name, copy.deepcopy(body)))
            self.state[name] = result
        return ''


class CellReaderAdditionTests(unittest.TestCase):
    def setUp(self):
        self.previous, self.old_material = fixture()
        self.config, self.material = add_reader(self.previous, self.old_material)
        self.fake = FakeSecrets(self.previous, self.old_material)
        self.clock = patch.object(trust.time, 'time', return_value=1790740000)
        self.clock.start()
        self.addCleanup(self.clock.stop)

    def reconcile(self, **kwargs):
        with patch.object(trust, 'read', side_effect=self.fake.read), patch.object(trust, 'kubectl', side_effect=self.fake.kubectl):
            trust.reconcile(self.config, self.material, previous=self.previous, **kwargs)

    def change_ring(self, change, purpose='bundle-readers'):
        item = next(s for s in self.config['secrets'] if s['purpose'] == purpose)
        ring = json.loads(self.material[item['name']]['keyring.json'])
        change(ring)
        self.material[item['name']]['keyring.json'] = trust.canonical(ring)
        item['digest'] = trust.digest(self.material[item['name']])

    def test_addition_preserves_old_material_and_is_idempotent(self):
        before = copy.deepcopy(self.fake.state)
        self.reconcile()
        self.assertEqual(self.fake.writes[0][0], 'fugue-cell-test-token-node-b')
        for name, old in before.items():
            current = self.fake.state[name]
            self.assertEqual(current['metadata']['uid'], old['metadata']['uid'])
            if name != 'fugue-cell-test-readers':
                self.assertEqual(current['data'], old['data'])
        readers = json.loads(base64.b64decode(self.fake.state['fugue-cell-test-readers']['data']['keyring.json']))
        old_readers = json.loads(self.old_material['fugue-cell-test-readers']['keyring.json'])
        self.assertEqual(readers['credentials'][0], old_readers['credentials'][0])
        count = len(self.fake.writes)
        with patch.object(trust.time, 'time', return_value=1800640001):
            self.reconcile()
            self.reconcile(check=True)
        self.assertEqual(len(self.fake.writes), count)

    def test_second_addition_retains_original_signing_generation(self):
        config3, material3 = add_reader(self.config, self.material, 'node-c', 6)
        self.assertEqual(len(trust.resources(config3, material3, self.config)), 7)
        self.assertEqual(material3['fugue-cell-test-signing'], self.old_material['fugue-cell-test-signing'])

    def test_changed_predecessor_rejected_before_cluster_access(self):
        for mode in ['digest', 'generation', 'cell', 'package', 'missing']:
            with self.subTest(mode=mode), patch.object(trust, 'read') as read:
                prior = copy.deepcopy(self.previous)
                if mode == 'digest': prior['previousDigest'] = 'different'
                if mode == 'generation': prior['generation'] = 7
                if mode == 'cell': prior['cell'] = 'cell-other'
                if mode == 'package': prior['materialSecret'] = 'FUGUE_EDGE_CELL_TRUST_CELL_TEST'
                if mode == 'missing': prior = None
                with self.assertRaises(ValueError):
                    trust.reconcile(self.config, self.material, previous=prior)
                read.assert_not_called()

    def test_rotating_any_old_key_or_credential_is_forbidden(self):
        mutations = [
            ('bundle-readers', lambda r: r['credentials'][0].update(not_after_unix=1800640001)),
            ('bundle-readers', lambda r: r['credentials'][0].update(revoked=True)),
            ('bundle-readers', lambda r: r['credentials'][0].update(credential_id='replacement')),
            ('bundle-signing', lambda r: r['group'].update(primary_key_id='replacement')),
            ('inventory-writer', lambda r: r.update(active_key_id='replacement')),
            ('recovery', lambda r: r['keys'][0].update(not_after_unix=1800640001)),
        ]
        for purpose, mutation in mutations:
            with self.subTest(purpose=purpose):
                self.config, self.material = add_reader(self.previous, self.old_material)
                self.change_ring(mutation, purpose)
                with self.assertRaises(ValueError): self.reconcile()
                self.assertEqual(self.fake.writes, [])
        self.config, self.material = add_reader(self.previous, self.old_material)
        token = next(s for s in self.config['secrets'] if s['purpose'] == 'reader-token/node-a')
        self.material[token['name']]['token'] = base64.urlsafe_b64encode(bytes([9]) * 32).decode().rstrip('=')
        token['digest'] = trust.digest(self.material[token['name']])
        self.change_ring(lambda r: r['credentials'][0].update(token_digest='sha256:' + hashlib.sha256(self.material[token['name']]['token'].encode()).hexdigest()))
        with self.assertRaises(ValueError): self.reconcile()
        self.assertEqual(self.fake.writes, [])

    def test_removal_rename_multiple_additions_and_revoked_new_reader_rejected(self):
        for mode in ['remove', 'rename', 'multiple', 'revoked', 'shared']:
            with self.subTest(mode=mode):
                self.config, self.material = add_reader(self.previous, self.old_material)
                if mode == 'remove': self.config['edgeIds'].remove('node-a')
                if mode == 'rename':
                    item = self.config['secrets'][0]
                    old = item['name']; item['name'] += '-new'
                    self.material[item['name']] = self.material.pop(old)
                if mode == 'multiple':
                    self.config, self.material = add_reader(self.config, self.material, 'node-c', 6)
                    self.config.update(generation=2, previousGeneration=1, previousDigest=trust.digest(self.previous))
                    self.change_ring(lambda r: r.update(generation=2))
                if mode == 'revoked': self.change_ring(lambda r: r['credentials'][1].update(revoked=True))
                if mode == 'shared':
                    item = next(s for s in self.config['secrets'] if s['purpose'] == 'reader-token/node-b')
                    self.material[item['name']]['token'] = self.old_material['fugue-cell-test-token']['token']
                    item['digest'] = trust.digest(self.material[item['name']])
                    self.change_ring(lambda r: r['credentials'][1].update(token_digest=r['credentials'][0]['token_digest']))
                with self.assertRaises(ValueError): self.reconcile()
                self.assertEqual(self.fake.writes, [])

    def test_missing_or_foreign_old_secret_cannot_be_adopted(self):
        for mode in ['absent', 'foreign', 'drift', 'deleting', 'same-generation', 'missing-uid']:
            with self.subTest(mode=mode):
                self.fake = FakeSecrets(self.previous, self.old_material)
                item = self.fake.state['fugue-cell-test-readers']
                if mode == 'absent': del self.fake.state['fugue-cell-test-readers']
                if mode == 'foreign': item['metadata']['annotations'][trust.DECLARATION] = 'sha256:' + 'f'*64
                if mode == 'drift': item['data']['keyring.json'] = base64.b64encode(b'changed').decode()
                if mode == 'deleting': item['metadata']['deletionTimestamp'] = 'now'
                if mode == 'same-generation': item['metadata']['annotations'][trust.GENERATION] = '2'
                if mode == 'missing-uid': del item['metadata']['uid']
                with self.assertRaises(ValueError): self.reconcile()
                self.assertEqual(self.fake.writes, [])

    def test_partial_update_retries_without_changing_old_reader_access(self):
        def fail(name, dry):
            if not dry and name == 'fugue-cell-test-readers': raise RuntimeError('transport lost')
        self.fake.before = fail
        with self.assertRaises(RuntimeError): self.reconcile()
        self.assertEqual(self.fake.state['fugue-cell-test-readers']['data']['keyring.json'], base64.b64encode(self.old_material['fugue-cell-test-readers']['keyring.json'].encode()).decode())
        completed = [name for name, _ in self.fake.writes]
        self.fake.before = None
        self.reconcile()
        for name in completed:
            self.assertEqual(sum(n == name for n, _ in self.fake.writes), 1)
        self.reconcile(check=True)

    def test_server_preflight_failure_and_check_only_do_not_write(self):
        with self.assertRaises(ValueError): self.reconcile(check=True)
        self.assertEqual(self.fake.writes, [])
        def deny(name, dry):
            if dry and name == 'fugue-cell-test-readers': raise RuntimeError('admission rejected')
        self.fake.before = deny
        with self.assertRaises(RuntimeError): self.reconcile()
        self.assertEqual(self.fake.writes, [])

    def test_actual_write_fences_uid_and_resource_version(self):
        for field, value in [('uid', 'replacement'), ('resourceVersion', '77')]:
            with self.subTest(field=field):
                self.fake = FakeSecrets(self.previous, self.old_material)
                def race(name, dry):
                    if not dry and name == 'fugue-cell-test-readers':
                        self.fake.state[name]['metadata'][field] = value
                self.fake.before = race
                with self.assertRaisesRegex(RuntimeError, 'CAS conflict'): self.reconcile()
                ring = json.loads(base64.b64decode(self.fake.state['fugue-cell-test-readers']['data']['keyring.json']))
                self.assertEqual(len(ring['credentials']), 1)

    def test_expiry_blocks_new_access_and_is_rechecked_after_preflight(self):
        for now in [1790639999, 1800640000]:
            with self.subTest(now=now), patch.object(trust.time, 'time', return_value=now):
                with self.assertRaises(ValueError): self.reconcile()
                self.assertEqual(self.fake.writes, [])
        with patch.object(trust.time, 'time', side_effect=[1790740000, 1790740000, 1800640000]):
            with self.assertRaises(ValueError): self.reconcile()
        ring = json.loads(base64.b64decode(self.fake.state['fugue-cell-test-readers']['data']['keyring.json']))
        self.assertEqual(len(ring['credentials']), 1)

    def test_keyring_generations_reject_booleans(self):
        old, material = fixture()
        ring = json.loads(material['fugue-cell-test-signing']['keyring.json'])
        ring['generation'] = True
        material['fugue-cell-test-signing']['keyring.json'] = trust.canonical(ring)
        old['secrets'][0]['digest'] = trust.digest(material['fugue-cell-test-signing'])
        with self.assertRaises(ValueError): trust.resources(old, material)

    def test_reader_addition_generation_is_bounded_and_sequential(self):
        for generation, previous in [(True, 0), (2, True), (4, 1), (2**64, 2**64-1)]:
            c = copy.deepcopy(self.config)
            c.update(generation=generation, previousGeneration=previous)
            with self.assertRaises(ValueError): trust.validate(c)

    def test_completed_reader_write_can_finish_metadata_after_expiry(self):
        self.config['secrets'].sort(key=lambda item: item['purpose'] != 'bundle-readers')
        def fail(name, dry):
            if not dry and name == 'fugue-cell-test-signing':
                raise RuntimeError('transport lost after reader publication')
        self.fake.before = fail
        with self.assertRaises(RuntimeError): self.reconcile()
        self.assertEqual(len(json.loads(base64.b64decode(self.fake.state['fugue-cell-test-readers']['data']['keyring.json']))['credentials']), 2)
        before_retry = len(self.fake.writes)
        self.fake.before = None
        with patch.object(trust.time, 'time', return_value=1800640001):
            self.reconcile()
        for _, patch_body in self.fake.writes[before_retry:]:
            self.assertFalse(any(op['path'] == '/data' for op in patch_body))

    def test_cli_suppresses_private_material_on_failure(self):
        marker = 'synthetic-private-material-must-not-be-logged'
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'declaration.json'
            path.write_text(json.dumps(self.config))
            previous = Path(folder) / 'previous.json'
            previous.write_text(json.dumps(self.previous))
            env = dict(os.environ, FUGUE_EDGE_CELL_TRUST=json.dumps({self.config['cell']: {'private': marker}}))
            result = subprocess.run([sys.executable, '-m', 'scripts.reconcile_cell_trust', str(path), '--previous', str(previous)],
                                    capture_output=True, text=True, env=env, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, '')
        self.assertNotIn(marker, result.stderr)
        self.assertIn('private diagnostics suppressed', result.stderr)


if __name__ == '__main__':
    unittest.main()
