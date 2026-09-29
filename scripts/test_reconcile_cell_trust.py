import base64
import copy
import hashlib
import json
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


if __name__=='__main__':unittest.main()
