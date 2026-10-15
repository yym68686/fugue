import copy
import unittest
from scripts.configure_postgres_cpu_policy import plan, FIELDS


class PostgresCPUPolicyTest(unittest.TestCase):
    def setUp(self):
        self.config={'schema':'fugue.managed-postgres-cpu-policy/v1','namespace':'platform','deployment':'controller','container':'controller','expectedImage':'registry.example/controller@sha256:'+'a'*64,'expectedConfigSha':'b'*40,'cpuRequestUpscale':True,'cpuLimitUpscale':True}
        self.live={'metadata':{'name':'controller','namespace':'platform','uid':'fixture-uid','resourceVersion':'4','generation':2,'annotations':{'fugue.pro/production-config-sha':'b'*40}},'spec':{'replicas':2,'strategy':{'type':'RollingUpdate','rollingUpdate':{'maxUnavailable':0,'maxSurge':1}},'template':{'spec':{'containers':[{'name':'controller','image':self.config['expectedImage'],'env':[{'name':name,'value':'false'} for name in FIELDS.values()]+[{'name':'MEMORY_POLICY','value':'true'}]}]}}},'status':{'observedGeneration':2,'updatedReplicas':2,'availableReplicas':2}}

    def test_only_two_direction_gates_change_and_image_is_fenced(self):
        original=copy.deepcopy(self.live)
        patch,evidence=plan(self.config,self.live)
        self.assertEqual(original,self.live)
        writes=[x for x in patch if x['op']!='test']
        self.assertEqual([x['path'] for x in writes],['/spec/template/spec/containers/0/env/0/value','/spec/template/spec/containers/0/env/1/value','/metadata/annotations'])
        self.assertEqual(len(evidence['changes']),2)
        self.assertEqual(patch[0]['value'],'fixture-uid')
        self.assertEqual(patch[2]['value'],self.config['expectedImage'])

    def test_replay_and_config_recovery(self):
        for e in self.live['spec']['template']['spec']['containers'][0]['env'][:2]:e['value']='true'
        self.assertEqual(plan(self.config,self.live)[0],[])
        self.config['cpuRequestUpscale']=False
        patch,_=plan(self.config,self.live)
        self.assertEqual(patch[3]['value'],'false')

    def test_fails_closed_on_competing_code_or_unsafe_rollout(self):
        for path,value in [('image','other'),('availableReplicas',1),('updatedReplicas',1),('maxUnavailable',1),('replicas',1)]:
            live=copy.deepcopy(self.live)
            if path=='image':live['spec']['template']['spec']['containers'][0]['image']=value
            elif path=='maxUnavailable':live['spec']['strategy']['rollingUpdate'][path]=value
            elif path=='replicas':live['spec'][path]=value
            else:live['status'][path]=value
            with self.subTest(path=path),self.assertRaises(ValueError):plan(self.config,live)

    def test_cannot_smuggle_database_or_image_mutations(self):
        for key in ['storage','resources','newImage']:
            config=copy.deepcopy(self.config);config[key]={}
            with self.subTest(key=key),self.assertRaises(ValueError):plan(config,self.live)
