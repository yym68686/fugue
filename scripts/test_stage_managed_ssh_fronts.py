import copy
import json
import unittest

from scripts import stage_managed_ssh_fronts as stage


class SSHFrontStageTest(unittest.TestCase):
    def fixture(self):
        receipt={'schema':'fugue.ssh-front-image-receipt/v1','verified':True,'image':'registry.example/front@sha256:'+'a'*64,'source_commit':'b'*40,'architecture':'amd64','checks':['actual-front-image','signed-route-load','persisted-exact-instance-ack','preserved-target-streams','invalid-configuration-keeps-lkg','credentials-excluded-from-urls','service-public-handoff','compensation-preserves-established-streams'],'transport':{'verified':True}}
        config={'schema':'fugue.ssh-front-stage/v1','generation':1,'expectedPreviousGeneration':0,'expectedPreviousDigest':'','namespace':'test-platform','stateConfigMap':'front-stage','image':receipt['image'],'artifactReceiptDigest':stage.digest(receipt),'artifactSourceCommit':receipt['source_commit'],'artifactRunID':123,'apiURL':'https://control.example','signingSecret':'route-signing','signingKeyID':'route-current','signingPreviousKeyID':'','revokedSigningKeyIDs':[],'portStart':24000,'portEnd':25000,'cacheHostRoot':'/var/lib/fugue/ssh-front/test','healthSourceCIDRs':['192.0.2.10/32'],'backendNamespace':'test-backends','nodes':[{'id':'worker-a','group':'group-a','publicAddress':'8.8.8.8','credential':{'kind':'secret','name':'node-token'}}]}
        state={}
        writes=[]
        def kube(args,value=None):
            if args[0]=='get':
                namespace=args[args.index('-n')+1] if '-n' in args else ''
                key=(args[1].lower(),namespace,args[2])
                if key in state:return json.dumps(state[key])
                if args[1]=='secret':return json.dumps({'data':{'FUGUE_EDGE_TOKEN':'fixture','FUGUE_BUNDLE_SIGNING_KEY':'fixture'}})
                if args[1]=='node':return json.dumps({'metadata':{'labels':{'fugue.io/role.edge':'true'}},'status':{'addresses':[{'address':'8.8.8.8'}],'nodeInfo':{'architecture':'amd64','operatingSystem':'linux'},'conditions':[{'type':'Ready','status':'True'}]}})
                return ''
            desired=copy.deepcopy(value)
            metadata=desired['metadata']
            key=(desired['kind'].lower(),metadata.get('namespace',''),metadata['name'])
            if key in state and metadata.get('resourceVersion')!=state[key]['metadata']['resourceVersion']:
                raise ValueError('resource version conflict')
            metadata.update(resourceVersion=str(len(writes)+1),uid='uid-'+metadata['name'])
            state[key]=desired
            writes.append(desired)
            return ''
        return config,receipt,state,writes,kube

    def test_stage_never_changes_public_transport_or_existing_executors(self):
        config,receipt,state,writes,kube=self.fixture()
        result=stage.reconcile(config,receipt,kube)
        self.assertFalse(result['public_transport_changed'])
        self.assertEqual([value['kind'] for value in writes],['NetworkPolicy','Deployment','ConfigMap'])
        deployment=writes[1]
        pod=deployment['spec']['template']['spec']
        self.assertFalse(pod['hostNetwork'])
        self.assertFalse(pod['automountServiceAccountToken'])
        self.assertEqual(pod['nodeName'],'worker-a')
        for key in ['node.kubernetes.io/not-ready', 'node.kubernetes.io/unreachable']:
            matching = [entry for entry in pod['tolerations'] if entry.get('key') == key and entry.get('effect') == 'NoExecute']
            self.assertEqual(len(matching), 1)
            self.assertNotIn('tolerationSeconds', matching[0])
        self.assertTrue(pod['containers'][0]['securityContext']['readOnlyRootFilesystem'])
        self.assertFalse(any('hostPort' in port for port in pod['containers'][0]['ports']))
        self.assertEqual(pod['volumes'][0]['hostPath']['path'],config['cacheHostRoot']+'/'+deployment['metadata']['name'])
        self.assertTrue(stage.reconcile(config,receipt,kube)['verified'])
        previous_name=deployment['metadata']['name']
        config.update(generation=2,expectedPreviousGeneration=1,expectedPreviousDigest=stage.digest(config))
        stage.reconcile(config,receipt,kube)
        self.assertNotEqual(previous_name,writes[-2]['metadata']['name'])
        self.assertIn(('deployment',config['namespace'],previous_name),state)

    def test_node_file_source_is_explicit_and_cannot_shadow_reviewed_secret(self):
        config,receipt,state,writes,kube=self.fixture()
        pod=stage.objects(config,config['nodes'][0])[1]['spec']['template']['spec']
        environment={entry['name']:entry for entry in pod['containers'][0]['env']}
        self.assertEqual(environment['FUGUE_EDGE_NODE_ENV_FILE']['value'],'')
        config['nodes'][0]['credential']={'kind':'node-file','path':'/etc/fugue/edge-node.env'}
        stage.validate(config,receipt)
        pod=stage.objects(config,config['nodes'][0])[1]['spec']['template']['spec']
        self.assertEqual(pod['volumes'][1]['hostPath'],{'path':'/etc/fugue/edge-node.env','type':'File'})
        self.assertTrue(pod['containers'][0]['volumeMounts'][1]['readOnly'])
        self.assertFalse(any(value['name']=='FUGUE_EDGE_TOKEN' for value in pod['containers'][0]['env']))

    def test_reobserving_old_stage_does_not_restart_existing_fronts(self):
        config,receipt,state,writes,kube=self.fixture()
        stage.reconcile(config,receipt,kube)
        identity=('deployment',config['namespace'],stage.slot_name(config,config['nodes'][0]))
        legacy=[{'operator':'Exists','effect':'NoSchedule'}]
        state[identity]['spec']['template']['spec']['tolerations']=legacy
        stage.reconcile(config,receipt,kube)
        self.assertEqual(state[identity]['spec']['template']['spec']['tolerations'],legacy)

    def test_foreign_candidate_and_missing_predecessor_fail_before_writes(self):
        config,receipt,state,writes,kube=self.fixture()
        name=stage.slot_name(config,config['nodes'][0])
        state[('deployment',config['namespace'],name)]={'metadata':{'labels':{'app.kubernetes.io/managed-by':'another-owner'}}}
        with self.assertRaises(ValueError):stage.reconcile(config,receipt,kube)
        self.assertEqual(writes,[])
        state.clear()
        config.update(generation=2,expectedPreviousGeneration=1,expectedPreviousDigest='sha256:'+'c'*64)
        with self.assertRaises(ValueError):stage.reconcile(config,receipt,kube)
        self.assertEqual(writes,[])

    def test_rejects_unsafe_or_unverified_stage_inputs(self):
        config,receipt,state,writes,kube=self.fixture()
        for field,value in [('image','registry.example/front:latest'),('apiURL','https://control.example?token=private'),('cacheHostRoot','/var/lib/fugue/edge'),('healthSourceCIDRs',['0.0.0.0/0']),('artifactSourceCommit','c'*40),('portStart',22)]:
            candidate=copy.deepcopy(config)
            candidate[field]=value
            with self.assertRaises(ValueError):stage.reconcile(candidate,receipt,kube)
        self.assertEqual(writes,[])
        receipt['transport']['verified']=False
        config['artifactReceiptDigest']=stage.digest(receipt)
        with self.assertRaises(ValueError):stage.reconcile(config,receipt,kube)
        self.assertEqual(writes,[])


if __name__=='__main__':
    unittest.main()
