import copy
import unittest
from scripts import select_front_transport_transition as transition


def declaration(phase='observe', target='', generation=1):
    return {'schema':'fugue.front-transport-transition/v1','generation':generation,'phase':phase,'target':target}


def inventories():
    profile='deploy/environments/production/front-observation/test.json'
    probes={'schema':'fugue.front-probe-transport/v1','generation':1,'namespace':'platform-system','listeners':[{'name':'probe-a','address':'8.8.8.8','port':15443,'observation':profile}]}
    staging={'schema':'fugue.front-serving-stage/v1','generation':1,'namespace':'platform-system','services':[{'name':'front-a','observation':profile,'probeListener':'probe-a'}]}
    handoff={'schema':'fugue.front-serving-handoff/v1','namespace':'platform-system','service':'front-a','address':'8.8.8.8','observation':profile,'expectedGeneration':1,'generation':2,'rollbackGeneration':3,'verificationSeconds':60}
    return probes,staging,handoff


class FrontTransitionSelectionTests(unittest.TestCase):
    def test_initial_declaration_is_inert_and_only_one_phase_can_execute(self):
        baseline=declaration()
        transition.advance(None,baseline)
        probes,staging,public=inventories()
        for phase,target in [('probe','probe-a'),('stage','front-a'),('handoff','deploy/environments/production/front-serving-handoff/next.json')]:
            current=declaration(phase,target,2)
            transition.advance(baseline,current)
            for requested in ['probe','stage','handoff']:
                self.assertEqual(transition.resolve(current,requested,probes,staging,lambda _:public),target if phase==requested else '')
        for bad in [declaration('probe','probe-a'),declaration(generation=2)]:
            with self.assertRaises(ValueError):transition.advance(None,bad)
        for generation in [1,3,True]:
            with self.assertRaises(ValueError):transition.advance(baseline,declaration(generation=generation))

    def test_identity_and_profile_mismatch_cannot_select_transport(self):
        probes,staging,public=inventories()
        path='deploy/environments/production/front-serving-handoff/next.json'
        for mutation in ['unknown probe','unknown service','address','profile','namespace','stage generation']:
            p,s,h=copy.deepcopy((probes,staging,public))
            phase,target='handoff',path
            if mutation=='unknown probe':phase,target='probe','absent'
            if mutation=='unknown service':h['service']='absent'
            if mutation=='address':h['address']='9.9.9.9'
            if mutation=='profile':h['observation']='deploy/environments/production/front-observation/other.json'
            if mutation=='namespace':s['namespace']='foreign-system'
            if mutation=='stage generation':s['generation']=2
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):transition.resolve(declaration(phase,target,2),phase,p,s,lambda _:h)

    def test_targets_cannot_inject_commands_paths_or_implicit_changes(self):
        for value in [declaration('observe','front-a'),declaration('handoff','../../secret'),declaration('handoff','deploy/environments/production/front-serving-handoff/a.json\n'),declaration('stage','$(command)'),declaration('probe','a b'),dict(declaration(),unknown=True)]:
            with self.assertRaises(ValueError):transition.validate(value)

if __name__=='__main__':unittest.main()
