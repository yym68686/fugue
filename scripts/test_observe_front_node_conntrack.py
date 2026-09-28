import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts import observe_front_node_conntrack as observer


def fixture():
    image = 'registry.example/observer@sha256:'+'a'*64
    config = {'schema':'fugue.front-connection-observer/v1','namespace':'test-system','selector':{'role':'observer'},'daemonSet':'node-observer','container':'observer','image':image,'hostRoot':'/host','python':'/usr/bin/python3'}
    node = {'metadata':{'name':'node-a','uid':'node-one'},'status':{'addresses':[{'address':'192.0.2.1'}]}}
    pod = {'metadata':{'name':'observer-one','namespace':'test-system','uid':'pod-one','ownerReferences':[{'kind':'DaemonSet','name':'node-observer','controller':True}]},'spec':{'nodeName':'node-a','hostPID':True,'containers':[{'name':'observer','securityContext':{'privileged':True,'runAsUser':0},'volumeMounts':[{'name':'host','mountPath':'/host'}]}],'volumes':[{'name':'host','hostPath':{'path':'/'}}]},'status':{'phase':'Running','containerStatuses':[{'name':'observer','ready':True,'imageID':image}]}}
    held = SimpleNamespace(client_address='192.0.2.1',client_port=43123,target_address='192.0.2.9',target_port=15443)
    return config, node, pod, held


class NodeConntrackTests(unittest.TestCase):
    def test_existing_observer_must_match_source_node_image_owner_and_host_namespace(self):
        for bad in [None,'image','namespace','node','owner','host pid','privilege','host root','subpath','not ready','recreated','ambiguous node','ambiguous pod','incomplete']:
            with self.subTest(bad=bad):
                config,node,pod,held=fixture()
                nodes={'items':[node]};pods={'items':[pod]}
                if bad=='image':pod['status']['containerStatuses'][0]['imageID']='registry.example/observer:latest'
                if bad=='namespace':pod['metadata']['namespace']='foreign'
                if bad=='node':pod['spec']['nodeName']='node-b'
                if bad=='owner':pod['metadata']['ownerReferences'][0]['name']='foreign'
                if bad=='host pid':pod['spec']['hostPID']=False
                if bad=='privilege':pod['spec']['containers'][0]['securityContext']['privileged']=False
                if bad=='host root':pod['spec']['volumes'][0]['hostPath']['path']='/unrelated'
                if bad=='subpath':pod['spec']['containers'][0]['volumeMounts'][0]['subPath']='unrelated'
                if bad=='not ready':pod['status']['containerStatuses'][0]['ready']=False
                if bad=='ambiguous node':nodes['items'].append(copy.deepcopy(node))
                if bad=='ambiguous pod':pods['items'].append(copy.deepcopy(pod))
                if bad=='incomplete':pods['metadata']={'continue':'next'}
                after=copy.deepcopy(pods)
                if bad=='recreated':after['items'][0]['metadata']['uid']='replacement'
                with patch.object(observer.front,'read',side_effect=[nodes,pods,{'address':'10.0.0.1','port':43123},nodes,after]) as read:
                    if bad:
                        with self.assertRaises(ValueError):observer.read(held,'10.0.0.2',config)
                        if bad!='recreated':self.assertLessEqual(read.call_count,2)
                    else:
                        self.assertEqual(observer.read(held,'10.0.0.2',config),('10.0.0.1',43123))
                        self.assertEqual(held.nat_observer['pod_uid'],'pod-one')
                        command=read.call_args_list[2].args
                        self.assertEqual(command[:18],('-n','test-system','exec','observer-one','-c','observer','--','nsenter','-t','1','-n','--','chroot','/host','/usr/bin/python3','-I','-B','-c'))
                        self.assertEqual(command[-10:],('--source','192.0.2.1','--destination','192.0.2.9','--source-port','43123','--destination-port','15443','--pod','10.0.0.2'))

    def test_observer_configuration_is_explicit_and_cannot_use_mutable_image_or_paths(self):
        config,_,_,_=fixture();observer.validate(config)
        for key,value in [('image','registry.example/observer:latest'),('hostRoot','/host/../'),('python','python3'),('selector',{'role':'a,b'})]:
            with self.subTest(key=key),self.assertRaises(ValueError):observer.validate(dict(config,**{key:value}))

if __name__=='__main__':unittest.main()
