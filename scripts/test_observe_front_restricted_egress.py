import contextlib
import copy
import io
import json
import subprocess
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from scripts import observe_front_restricted_egress as gate


def config():
    return {'schema':'fugue.front-restricted-egress-observation/v1','namespacePrefixes':['tenant-'],'maxConsumers':4,'crictl':'/usr/local/bin/crictl'}


def app(name='app-one'):
    return {'metadata':{'name':name,'namespace':'tenant-one','uid':name+'-uid','resourceVersion':'1'},'spec':{'appID':name,'appSpec':{'replicas':1,'network_policy':{'egress':{'mode':'restricted','allow_public_internet':True}}}}}


class RestrictedEgressTests(unittest.TestCase):
    def test_only_active_explicit_public_consumers_are_selected(self):
        wanted=app()
        disabled=app('disabled');disabled['spec']['appSpec']['network_policy']['egress']['allow_public_internet']=False
        stopped=app('stopped');stopped['spec']['appSpec']['replicas']=0
        open_app=app('open');open_app['spec']['appSpec']['network_policy']['egress']['mode']='unrestricted'
        foreign=app('foreign');foreign['metadata']['namespace']='platform-system'
        rows={'items':[wanted,disabled,stopped,open_app,foreign]}
        self.assertEqual(gate.consumers(config(),rows),[wanted])
        for observed in [{'items':[]},{'items':[wanted],'metadata':{'continue':'more'}},{'items':[app(str(i)) for i in range(5)]}]:
            with self.assertRaises(ValueError):gate.consumers(config(),observed)

    def test_source_runtime_identity_must_match_before_entering_network_namespace(self):
        params={'crictl':'/usr/local/bin/crictl','python':'/usr/bin/python3','container_id':'a'*64,'pod_uid':'pod-one','pod_name':'source','namespace':'tenant-one','private_address':'10.1.1.4'}
        baseline={'status':{'labels':{'io.kubernetes.pod.uid':'pod-one','io.kubernetes.pod.name':'source','io.kubernetes.pod.namespace':'tenant-one'}},'info':{'pid':100}}
        for bad in [None,'uid','namespace','pid']:
            value=copy.deepcopy(baseline)
            if bad=='uid':value['status']['labels']['io.kubernetes.pod.uid']='replaced'
            if bad=='namespace':value['status']['labels']['io.kubernetes.pod.namespace']='other'
            if bad=='pid':value['info']['pid']=1
            with patch('sys.argv',['probe',json.dumps(params)]),patch('subprocess.check_output',return_value=json.dumps(value).encode()),patch('socket.create_connection',return_value=contextlib.nullcontext()),patch('subprocess.run',return_value=SimpleNamespace(returncode=0,stdout='{"private_management_blocked":true}',stderr='')) as execute,contextlib.redirect_stdout(io.StringIO()):
                if bad:
                    with self.assertRaises(ValueError):exec(gate.remote_program(),{})
                    execute.assert_not_called()
                else:
                    exec(gate.remote_program(),{})
                    self.assertEqual(execute.call_args.args[0][:5],['nsenter','-t','100','-n','--'])
                    self.assertIn('ssl.create_default_context()',execute.call_args.args[0][-2])

    def test_publication_races_require_a_fresh_exact_stable_pair(self):
        old={'edge':'node-a','group':'cell-a','version':'one'}
        new=dict(old,version='two')
        args=('8.8.8.8',15443,'api.example.test','/','node-a','cell-a')
        with patch.object(gate.front,'proof',side_effect=[old,new,new,new,new,new]):
            result=gate.front.stable_proof_pair(*args)
            self.assertEqual(result,{'proof':new,'publication_retries':1})
        for values in [[old,new,old],[old,dict(new,edge='other'),new],[old,new,new]*3]:
            with patch.object(gate.front,'proof',side_effect=values),self.assertRaises(ValueError):
                gate.front.stable_proof_pair(*args)

    def test_only_exec_transport_eof_retries_without_hiding_failed_probe(self):
        success=SimpleNamespace(returncode=0,stdout='{"private_management_blocked":true}',stderr='')
        eof=SimpleNamespace(returncode=1,stdout='',stderr='error: stream: EOF')
        with patch.object(gate.subprocess,'run',side_effect=[eof,success]):
            self.assertEqual(gate.execute_observation(['kubectl'])['observer_transport_retries'],1)
        for error in ['Traceback: probe timeout', 'command terminated with exit code 1', 'route mismatch']:
            with patch.object(gate.subprocess,'run',return_value=SimpleNamespace(returncode=1,stdout='',stderr=error)) as execution,self.assertRaises(ValueError):
                gate.execute_observation(['kubectl'])
            self.assertEqual(execution.call_count,1)

    def test_rejects_unbounded_or_implicit_observer(self):
        gate.validate(config())
        for key,value in [('maxConsumers',0),('maxConsumers',True),('crictl','/bin/../sh'),('namespacePrefixes',[''])]:
            changed=dict(config(),**{key:value})
            with self.assertRaises(ValueError):gate.validate(changed)

if __name__=='__main__':unittest.main()
