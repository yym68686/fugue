import copy
import json
import unittest

from scripts import reconcile_managed_ssh_runtime as runtime


class ManagedSSHRuntimeTest(unittest.TestCase):
    def fixture(self):
        receipt={"schema":"fugue.managed-ssh-image-receipt/v1","image":"registry.example/ssh@sha256:"+"a"*64,"helper_digest":"sha256:"+"b"*64,"architecture":"amd64","source_commit":"c"*40,"verified":True,"network_policy_enforced":True,"checks":["candidate-publication","native-ssh","native-scp","authorization-refresh","graceful-retirement","network-policy-allow-and-deny","unchanged-workload"]}
        options={"schema":"fugue.managed-ssh-runtime/v1","namespace":"test-ssh","runtime_ids":["runtime_test_1"],"releases":{"amd64":{"image":receipt["image"],"helper_digest":receipt["helper_digest"],"architecture":"amd64"}},"state_storage_class":"test-retained","api_servers":[{"cidr":"192.0.2.10/32","port":6443}],"ingress_namespace":"test-system","ingress_pod_labels":{"component":"front"},"controller_namespace":"test-system","controller_pod_labels":{"component":"ssh-control"},"ingress_cidrs":[],"required_edges":[],"rollback_retention_seconds":86400}
        config={"apiVersion":"ssh.fugue.dev/v1","kind":"ManagedSSHRuntimeIntent","generation":1,"expectedPreviousGeneration":0,"expectedPreviousDigest":"","controllerNamespace":"test-system","controllerAccount":"test-control","runtimeConfigMap":"test-runtime","tenantNamespaces":["test-tenant"],"runtime":options,"artifactReceiptDigest":runtime.digest(receipt),"artifactSourceCommit":receipt["source_commit"],"artifactRunID":123}
        state={}
        writes=[]
        def kube(args,value=None):
            if args[0]=="get":
                namespace=args[args.index('-n')+1] if '-n' in args else ''
                key=(args[1].lower(),namespace,args[2])
                if key==('namespace','','test-tenant'):
                    return json.dumps({'metadata':{'name':'test-tenant'}})
                return json.dumps(state[key]) if key in state else ''
            desired=copy.deepcopy(value)
            desired['metadata']['resourceVersion']=str(len(writes)+1)
            desired['metadata']['uid']='uid-'+desired['metadata']['name']
            state[(desired['kind'].lower(),desired['metadata'].get('namespace',''),desired['metadata']['name'])]=desired
            writes.append(desired)
            return ''
        return config,receipt,state,writes,kube

    def test_publishes_configuration_last_and_scopes_mutation(self):
        config,receipt,state,writes,kube=self.fixture()
        self.assertTrue(runtime.reconcile(config,receipt,kube)['verified'])
        self.assertEqual(writes[-1]['kind'],'ConfigMap')
        self.assertEqual(writes[0]['kind'],'Namespace')
        for resource in writes:
            self.assertNotIn(resource['kind'],['ClusterRole','ClusterRoleBinding','Deployment','Pod','Secret'])
            if resource['kind']=='Role' and resource['metadata']['namespace']=='test-tenant':
                granted={entry for rule in resource['rules'] for entry in rule['resources']}
                self.assertEqual(granted,{'pods','pods/exec','roles','rolebindings'})
        self.assertTrue(runtime.reconcile(config,receipt,kube)['verified'])

    def test_refuses_artifact_without_network_acceptance(self):
        config,receipt,state,writes,kube=self.fixture()
        receipt['network_policy_enforced']=False
        config['artifactReceiptDigest']=runtime.digest(receipt)
        with self.assertRaises(ValueError):
            runtime.reconcile(config,receipt,kube)
        self.assertEqual(writes,[])

    def test_runtime_identity_accepts_location_ids_and_rejects_invalid_types(self):
        config,receipt,state,writes,kube=self.fixture()
        config['runtime']['runtime_ids']=['runtime_managed_shared_loc_country-zz_ab12cd34']
        self.assertTrue(runtime.reconcile(config,receipt,kube)['verified'])
        for identifier in [None, 1, 'runtime_wrong/path', 'runtime_has space']:
            config['runtime']['runtime_ids']=[identifier]
            with self.assertRaises(ValueError):
                runtime.validate(config,receipt)

    def test_foreign_resource_rejects_before_any_writes(self):
        config,receipt,state,writes,kube=self.fixture()
        state[('role','test-tenant','managed-ssh-controller')]={'metadata':{'labels':{'app.kubernetes.io/managed-by':'another-owner'},'resourceVersion':'1'}}
        with self.assertRaises(ValueError):
            runtime.reconcile(config,receipt,kube)
        self.assertEqual(writes,[])

    def test_rejects_implicit_privilege_expansion_and_stale_configuration(self):
        config,receipt,state,writes,kube=self.fixture()
        for change in ['tenant','egress','source','image']:
            candidate=copy.deepcopy(config)
            if change=='tenant':candidate['tenantNamespaces']=['kube-system']
            if change=='egress':candidate['runtime']['api_servers'][0]['cidr']='0.0.0.0/0'
            if change=='source':candidate['artifactSourceCommit']='d'*40
            if change=='image':candidate['runtime']['releases']['amd64']['image']='registry.example/ssh:latest'
            with self.assertRaises(ValueError):runtime.validate(candidate,receipt)
        runtime.reconcile(config,receipt,kube)
        candidate=copy.deepcopy(config)
        candidate['runtime']['rollback_retention_seconds']=3600
        with self.assertRaises(ValueError):runtime.reconcile(candidate,receipt,kube)
        candidate.update(generation=2,expectedPreviousGeneration=1,expectedPreviousDigest=runtime.digest(config))
        self.assertTrue(runtime.reconcile(candidate,receipt,kube)['verified'])


if __name__=='__main__':
    unittest.main()
