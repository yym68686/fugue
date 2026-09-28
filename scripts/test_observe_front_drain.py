import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts import observe_front_drain as drain


class FrontDrainTests(unittest.TestCase):
    def test_traffic_still_using_old_executor_never_becomes_drained_by_later_empty_sample(self):
        config = {'address':'192.0.2.9'}
        profile = {'node':'node-a','group':'cell-a','namespace':'test-system','legacySelector':{'role':'old'},'candidateSelector':{'role':'new'},'probes':[{'host':'api.example.test','path':'/'}]}
        service = {'metadata':{'uid':'service-one','resourceVersion':'7'}}
        old = {'metadata':{'uid':'old','name':'old'},'spec':{}}
        candidate = {'metadata':{'uid':'new','name':'new'},'spec':{}}
        for failure in [None,'existing connection','recreated','transport changed']:
            with self.subTest(failure=failure):
                reads=[0]
                def serving(*_):
                    reads[0]+=1
                    out=copy.deepcopy(service)
                    if failure=='transport changed' and reads[0]>1:out['metadata']['resourceVersion']='8'
                    return out
                def pod(p,s,c):
                    out=copy.deepcopy(candidate if c else old)
                    if failure=='recreated' and reads[0]>1 and not c:out['metadata']['uid']='replacement'
                    return out
                socket=SimpleNamespace(proof=lambda _: {'edge':'node-a','group':'cell-a'},close=lambda: None)
                inventories=[{'count':1 if failure=='existing connection' else 0,'connection_ids':['active'] if failure=='existing connection' else []},{'count':0,'connection_ids':[]},{'count':0,'connection_ids':[]}]
                with patch.object(drain.handoff,'load_stage',return_value=({}, {}, profile)),patch.object(drain,'serving',side_effect=serving),patch.object(drain.front,'pod',side_effect=pod),patch.object(drain.connection,'HeldTLS',return_value=socket),patch.object(drain.connection,'fact',return_value={'pod_uid':'new','connection_id':'fresh'}),patch.object(drain,'connections',side_effect=inventories),patch.object(drain.time,'sleep'):
                    if failure in ['recreated','transport changed']:
                        with self.assertRaises(ValueError):drain.observe(config)
                    else:
                        result=drain.observe(config)
                        self.assertEqual(result['drained'],failure is None)
                        self.assertFalse(result['authorizes_traffic'])
                        self.assertEqual(len(result['observations']),3)

    def test_zero_requires_a_complete_connection_inventory(self):
        for value in [{'count':0},{'count':0,'active':[{'id':'active'}]},{'count':True,'active':[]},{'count':2,'active':[{'id':'same'},{'id':'same'}]}]:
            with self.subTest(value=value),patch.object(drain.front,'read',return_value=value),self.assertRaises(ValueError):
                drain.connections({'namespace':'test-system'},{'metadata':{'name':'front'}})
        with patch.object(drain.front,'read',return_value={'count':0,'active':[]}):
            self.assertEqual(drain.connections({'namespace':'test-system'},{'metadata':{'name':'front'}}),{'count':0,'connection_ids':[]})

if __name__=='__main__':unittest.main()
