import base64
import copy
import json
from pathlib import Path
import tempfile
import unittest

from scripts import run_ssh_identity_maintenance as maintenance
from scripts import reconcile_managed_ssh_keys as keys


class IdentityMaintenanceTest(unittest.TestCase):
    def fixture(self):
        ring={'schema':'fugue.ssh-identity-keyring/v1','active_key_id':'storage-one','keys':{'storage-one':base64.b64encode(b'a'*32).decode()}}
        declaration={'apiVersion':'identity.fugue.dev/v1','kind':'ManagedSSHKeyrings','namespace':'test-system','generation':1,'expectedPreviousGeneration':0,'restoreMissing':False,'identity':{'secretName':'identity-ring','contentDigest':keys.content_digest(ring),'expectedPreviousDigest':''},'signing':{'secretName':'signing-ring','contentDigest':'sha256:'+'b'*64,'expectedPreviousDigest':''}}
        config={'schema':'fugue.ssh-identity-maintenance-operation/v1','operation':'plan','planID':'inventory-one','expectedPlanDigest':'','namespace':'test-system','keyringDeclaration':'deploy/environments/production/managed-ssh/keys.json','databaseSecret':'database','storageDirectory':'/tmp/fugue/ssh-identity-maintenance'}
        secret={'metadata':{'uid':'keyring-a','labels':{'app.kubernetes.io/managed-by':keys.MANAGER,'ssh.fugue.dev/keyring-purpose':'identity'},'annotations':{keys.GENERATION:'1',keys.CONTENT_DIGEST:declaration['identity']['contentDigest']}},'data':{'keyring.json':base64.b64encode(keys.canonical(ring).encode()).decode()}}
        database={'metadata':{'uid':'database-a'},'data':{'FUGUE_DATABASE_URL':base64.b64encode(b'postgres://fixture').decode()}}
        def kube(args):
            self.assertEqual(args[:2],['get','secret'])
            return json.dumps(secret if args[2]=='identity-ring' else database)
        return config,declaration,secret,database,kube

    def test_explicit_plan_and_exact_apply_digest_required(self):
        config,declaration,secret,database,kube=self.fixture()
        maintenance.validate(config)
        for field,value in [('operation','delete'),('planID','../escape'),('storageDirectory','/tmp/public'),('expectedPlanDigest','sha256:'+'c'*64),('keyringDeclaration','/tmp/unreviewed.json')]:
            candidate=dict(config)
            candidate[field]=value
            with self.assertRaises(ValueError):maintenance.validate(candidate)
        config['operation']='apply'
        with self.assertRaises(ValueError):maintenance.validate(config)
        config['expectedPlanDigest']='sha256:'+'d'*64
        maintenance.validate(config)

    def test_key_material_requires_declared_generation_content_and_owner(self):
        config,declaration,secret,database,kube=self.fixture()
        ring,dsn=maintenance.material(config,declaration,kube)
        self.assertEqual(json.loads(ring)['active_key_id'],'storage-one')
        self.assertEqual(dsn,'postgres://fixture')
        for mutate in [lambda value:value['metadata']['annotations'].update({keys.GENERATION:'2'}),lambda value:value['metadata']['labels'].update({'app.kubernetes.io/managed-by':'foreign'}),lambda value:value['data'].update({'keyring.json':base64.b64encode(b'{}').decode()})]:
            previous=copy.deepcopy(secret)
            mutate(secret)
            with self.assertRaises(ValueError):maintenance.material(config,declaration,kube)
            secret.clear()
            secret.update(previous)

    def test_storage_cannot_follow_symlink_or_adopt_public_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            private=Path(temporary)/'private'
            maintenance.protected_directory(private)
            link=Path(temporary)/'link'
            link.symlink_to(private,target_is_directory=True)
            with self.assertRaises(ValueError):maintenance.protected_directory(link)
            private.chmod(0o755)
            with self.assertRaises(ValueError):maintenance.protected_directory(private)

    def test_database_route_is_derived_from_exact_service_authority(self):
        service={'metadata':{'uid':'database-service'},'spec':{'type':'ClusterIP','clusterIP':'10.43.0.8','selector':{'role':'database'},'ports':[{'port':5432}]}}
        def kube(args):
            self.assertEqual(args,['get','service','database','-n','test-system','-o','json'])
            return json.dumps(service)
        dsn='postgres://fixture:private@database.test-system.svc.cluster.local:5432/fixture?sslmode=verify-full'
        self.assertEqual(maintenance.database_route(dsn,'test-system',kube),'10.43.0.8')
        for rejected in [dsn.replace('test-system','foreign'),dsn.replace(':5432',''),dsn.replace(':5432',':6543'),dsn.replace('postgres://','http://')]:
            with self.assertRaises(ValueError) as error:maintenance.database_route(rejected,'test-system',kube)
            self.assertNotIn('private',str(error.exception))
        service['spec']['clusterIP']='0.0.0.0'
        with self.assertRaises(ValueError):maintenance.database_route(dsn,'test-system',kube)


if __name__=='__main__':
    unittest.main()
