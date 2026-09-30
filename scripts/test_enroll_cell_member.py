import copy
import datetime
import json
import unittest
from unittest.mock import patch

from scripts import enroll_cell_member as m
from scripts.test_bootstrap_cell_inventory import config as initial_config, health as initial_health


def config():
    c = initial_config()
    c.update(schema="fugue.cell-member-enrollment/v1", full_publication={"artifact_id":c["release_set"]["id"],"content_hash":c["release_set"]["digest"],"release_id":"release-full","fencing_token":11}, verification_evidence_hash="sha256:"+"e"*64, member_node_ids=["node-one","node-two"], serving_epoch={"slot":"a","fence_sequence":7,"min_healthy_instances":1}, control={"deployment":"cell-control","pod":"cell-control-pod","instance_uid":"33333333-3333-3333-3333-333333333333","source_sha":"c"*40,"image_digest":"sha256:"+"f"*64,"state_file":"/state/group.json"}, admission_window={"not_before":(m.initial.now()-datetime.timedelta(seconds=5)).isoformat(),"expires_at":(m.initial.now()+datetime.timedelta(minutes=10)).isoformat()})
    return c


def authority(c):
    content={"publication_role":"cell-routes","consumer_topology":{"publication_role":"cell-routes","schema_version":"fugue.traffic-consumer-topology/v1","authority_cell_id":c["authority_cell_id"],"edge_node_ids":c["member_node_ids"],"dns_node_ids":[]},"artifact_kinds":["edge_route_bundle","caddy_route_config"]}
    c["release_set"]["digest"]=m.digest(content);c["full_publication"]["content_hash"]=m.digest(content)
    a={"id":c["release_set"]["id"],"artifact_kind":"release_set","scope_key":"authority-cell:"+c["authority_cell_id"],"content":content,"content_hash":m.digest(content),"status":"validated"}
    r={"id":"release-full","artifact_id":a["id"],"artifact_kind":"release_set","scope_key":a["scope_key"],"release_channel":"full","status":"active","fencing_token":11,"verification_state":"verified"}
    lkg={"artifact_id":a["id"],"content_hash":a["content_hash"],"scope_key":a["scope_key"],"artifact_kind":"release_set","verified_by_release_id":r["id"],"verification_evidence_hash":c["verification_evidence_hash"],"expires_at":(m.initial.now()+datetime.timedelta(hours=1)).isoformat()}
    return {"artifact":a,"release":r},lkg


def facts(c,r,admitted=False):
    h=initial_health(c,r);h["platform_serving"]["traffic_release"]["release_channel"]="full";h["inventory_producer_active"]=admitted;return h


def converged(c,r):
    results=[]
    for kind in m.initial.KINDS:
        rows=[]
        for node in c["member_node_ids"]:
            credential="kubernetes:"+c["namespace"]+":"+c["worker"]["service_account"]+":"+c["worker"]["instance_uid"] if node==c["worker"]["node"] else "other-verified-pod"
            rows.append({"node_id":node,"state":"pass","observed":{"identity_verified":True,"credential_id":credential,"release_set_id":c["release_set"]["id"],"fencing_token":r["fencing_token"]}})
        results.append({"artifact_kind":kind,"pass":True,"required_expected":2,"required_passing":2,"assessments":rows})
    return {"convergence":results}


class MemberEnrollmentTests(unittest.TestCase):
    def test_control_epoch_requires_exact_process_freshness_and_fence(self):
        c=config();control=c["control"];image="registry.example/control@"+control["image_digest"]
        pod={"metadata":{"uid":control["instance_uid"],"ownerReferences":[{"kind":"ReplicaSet","name":"control-rs","uid":"rs-id","controller":True}]},"spec":{"containers":[{"name":"edge-control","image":image}]},"status":{"phase":"Running","containerStatuses":[{"name":"edge-control","imageID":image,"ready":True,"restartCount":0}]}}
        deployment={"metadata":{"uid":"deployment-id","annotations":{"fugue.pro/production-config-sha":control["source_sha"]}}}
        rs={"metadata":{"uid":"rs-id","ownerReferences":[{"kind":"Deployment","uid":"deployment-id","controller":True}]}}
        base={"edge_group_id":c["authority_cell_id"],"inventory":{"edge_group_id":c["authority_cell_id"],"active_epoch":{"edge_group_id":c["authority_cell_id"],**c["serving_epoch"]},"observed_at":m.initial.now().isoformat()}}
        for failure in [None,"old fence","minimum","foreign cell","expired","wrong Pod","restarted"]:
            state=copy.deepcopy(base);process=copy.deepcopy(pod)
            if failure=="old fence":state["inventory"]["active_epoch"]["fence_sequence"]-=1
            elif failure=="minimum":state["inventory"]["active_epoch"]["min_healthy_instances"]+=1
            elif failure=="foreign cell":state["edge_group_id"]="cell-foreign"
            elif failure=="expired":state["inventory"]["observed_at"]=(m.initial.now()-datetime.timedelta(minutes=2)).isoformat()
            elif failure=="wrong Pod":process["metadata"]["uid"]="wrong"
            elif failure=="restarted":process["status"]["containerStatuses"][0]["restartCount"]=1
            with patch.object(m.initial,"resource",side_effect=lambda _,kind,name:{"pod":process,"deployment":deployment,"replicaset":rs}[kind]),patch.object(m.initial,"kubectl",return_value=state):
                if failure:
                    with self.assertRaises(ValueError):m.control_epoch(c)
                else:self.assertEqual(m.control_epoch(c),c["serving_epoch"])

    def test_observation_rechecks_authority_after_proofs_and_rejects_bootstrap(self):
        c=config()
        for scenario in ["bootstrap","epoch advanced","full advanced",None]:
            with patch.object(m.initial,"isolated_worker",return_value={}),patch.object(m.initial,"resource",return_value={} if scenario=="bootstrap" else None),patch.object(m,"full",side_effect=[{},ValueError("full changed")] if scenario=="full advanced" else [{},{}]),patch.object(m,"control_epoch",side_effect=[{},ValueError("epoch changed")] if scenario=="epoch advanced" else [{},{}]),patch.object(m,"health",return_value={}),patch.object(m,"convergence"),patch.object(m.initial,"kubectl") as write:
                if scenario:
                    with self.assertRaises(ValueError):m.observe(c,lambda *_:None,False)
                else:m.observe(c,lambda *_:None,False)
                write.assert_not_called()

    def test_declaration_requires_complete_membership_fence_and_bounded_window(self):
        c=config();m.validate(c)
        for mutate in [lambda d:d.update(member_node_ids=["node-one"]),lambda d:d["serving_epoch"].update(fence_sequence=0),lambda d:d["serving_epoch"].update(slot="b"),lambda d:d["control"].update(state_file="/state/../private"),lambda d:d["admission_window"].update(expires_at=(m.initial.now()+datetime.timedelta(hours=1)).isoformat()),lambda d:d["full_publication"].update(content_hash="sha256:"+"a"*64)]:
            d=copy.deepcopy(c);mutate(d)
            with self.assertRaises(ValueError):m.validate(d)
        c["admission_window"]={"not_before":(m.initial.now()-datetime.timedelta(minutes=2)).isoformat(),"expires_at":(m.initial.now()-datetime.timedelta(minutes=1)).isoformat()}
        with self.assertRaises(ValueError):m.window_open(c)

    def test_full_requires_exact_positive_lkg_and_membership(self):
        c=config();state,lkg=authority(c)
        def api(method,path,body=None):
            self.assertEqual(method,"GET");return {"lkg":lkg} if path.endswith("/lkg") else state
        self.assertEqual(m.full(c,api),state["release"])
        for kind in ["unverified","member","lkg","expiry","fence"]:
            c=config();state,lkg=authority(c)
            if kind=="unverified":state["release"]["verification_state"]="serving_unverified"
            elif kind=="member":state["artifact"]["content"]["consumer_topology"]["edge_node_ids"]=["node-two"]
            elif kind=="lkg":lkg["verification_evidence_hash"]="sha256:"+"0"*64
            elif kind=="expiry":lkg["expires_at"]=(m.initial.now()-datetime.timedelta(seconds=1)).isoformat()
            else:state["release"]["fencing_token"]+=1
            with self.assertRaises(ValueError):m.full(c,api)

    def test_real_member_health_and_all_consumer_proofs_are_required(self):
        c=config();state,_=authority(c);r=state["release"]
        for kind in [None,"shadow","wrong release","wrong node","stale","tls","invented inventory"]:
            h=facts(c,r)
            if kind=="shadow":h["platform_serving"]["traffic_release"]["release_channel"]="shadow"
            elif kind=="wrong release":h["platform_serving"]["traffic_release"]["fencing_token"]+=1
            elif kind=="wrong node":h["edge_id"]="foreign"
            elif kind=="stale":h["platform_serving"]["verified_at"]=(m.initial.now()-datetime.timedelta(minutes=2)).isoformat()
            elif kind=="tls":h["platform_serving"]["tls_probes"]=0
            elif kind=="invented inventory":h["inventory_producer_active"]=True
            with patch.object(m.initial,"kubectl",return_value=h):
                if kind:
                    with self.assertRaises(ValueError):m.health(c,r,False)
                else:m.health(c,r,False)
        for kind in [None,"missing","duplicate","wrong Pod","foreign publication"]:
            d=converged(c,r)
            if kind=="missing":d["convergence"][0]["required_passing"]=1
            elif kind=="duplicate":d["convergence"][0]["assessments"][1]=copy.deepcopy(d["convergence"][0]["assessments"][0])
            elif kind=="wrong Pod":d["convergence"][0]["assessments"][0]["observed"]["credential_id"]="foreign-pod"
            elif kind=="foreign publication":d["convergence"][0]["assessments"][0]["observed"]["fencing_token"]+=1
            if kind:
                with self.assertRaises(ValueError):m.convergence(c,lambda *_:d,r)
            else:m.convergence(c,lambda *_:d,r)

    def test_only_verified_private_member_initializes_once_then_reports_inventory(self):
        c=config();state,_=authority(c);h=facts(c,state["release"]);worker={"image":"registry.example/worker@"+c["worker"]["image_digest"],"mount":{"name":"state","mountPath":"/state","subPath":"activation","readOnly":True}}
        actual=None;jobs=[];observations=[]
        def observe(_,api,admitted):
            self.assertEqual(admitted,actual is not None)
            out=copy.deepcopy(h);out["inventory_heartbeat_at"]=m.initial.now().isoformat();return worker,out
        def execute(*args,body=None):
            nonlocal actual
            self.assertEqual(args[0],"create");job=json.loads(body);jobs.append(job);spec=job["spec"]["template"]["spec"];arguments=spec["containers"][0]["args"]
            self.assertEqual(arguments[arguments.index("--initial-generation")+1],"7")
            self.assertFalse(spec["automountServiceAccountToken"])
            self.assertEqual(spec["nodeName"],c["worker"]["node"])
            self.assertEqual(spec["volumes"][0]["persistentVolumeClaim"]["claimName"],c["worker"]["pvc"])
            actual={"schema":"edge-front-group-activation/v1","edge_group_id":c["authority_cell_id"],"generation":7,"active_slot":"a","worker_source_commit":c["worker"]["source_sha"],"worker_image_digest":c["worker"]["image_digest"],"authority":"edge-control","operation":"initialize","reason":"Verified isolated Cell member "+m.digest(c),"bundle_generation":h["bundle_version"],"updated_at":(m.initial.now()-datetime.timedelta(seconds=1)).isoformat()}
            return job
        with patch.object(m,"observe",side_effect=observe),patch.object(m.initial,"activation",side_effect=lambda _:actual),patch.object(m.initial,"kubectl",side_effect=execute),patch.object(m.time,"sleep"):
            result=m.enroll(c,lambda *_:self.fail("unexpected artifact mutation"),observations.append)
            self.assertEqual(len(jobs),1)
            self.assertFalse(result["public_transport_changed"])
            self.assertFalse(result["artifacts_published"])
            m.enroll(c,lambda *_:self.fail("unexpected artifact mutation"),observations.append)
            self.assertEqual(len(jobs),1)

    def test_failed_observation_cannot_initialize_or_grant_bootstrap(self):
        c=config()
        with patch.object(m.initial,"activation",return_value=None),patch.object(m,"observe",side_effect=ValueError("proof failed")),patch.object(m.initial,"kubectl") as write:
            with self.assertRaisesRegex(ValueError,"proof failed"):m.enroll(c,lambda *_:None,lambda _:None)
            write.assert_not_called()

    def test_declared_observation_deadline_prevents_activation(self):
        c=config();worker={};facts={}
        with patch.object(m.initial,"activation",return_value=None),patch.object(m,"observe",return_value=(worker,facts)),patch.object(m.time,"monotonic",side_effect=[0,0,c["observation"]["timeout_seconds"]]),patch.object(m.initial,"kubectl") as write:
            with self.assertRaisesRegex(ValueError,"deadline"):m.enroll(c,lambda *_:None,lambda _:None)
            write.assert_not_called()

    def test_completed_initialization_retries_readonly_after_original_window(self):
        c=config();applied=m.initial.now()-datetime.timedelta(minutes=2)
        c["admission_window"]={"not_before":(applied-datetime.timedelta(minutes=1)).isoformat(),"expires_at":(applied+datetime.timedelta(minutes=1)).isoformat()}
        actual={"schema":"edge-front-group-activation/v1","edge_group_id":c["authority_cell_id"],"generation":7,"active_slot":"a","worker_source_commit":c["worker"]["source_sha"],"worker_image_digest":c["worker"]["image_digest"],"authority":"edge-control","operation":"initialize","reason":"Verified isolated Cell member "+m.digest(c),"bundle_generation":"verified-bundle","updated_at":applied.isoformat()}
        h={"bundle_version":"verified-bundle","platform_serving":{"route_probes":5,"tls_probes":5},"inventory_heartbeat_at":m.initial.now().isoformat()}
        m.check_activation(c,actual)
        with patch.object(m.initial,"isolated_worker",return_value={}),patch.object(m.initial,"resource",return_value=None),patch.object(m,"full",return_value={}),patch.object(m,"control_epoch"),patch.object(m,"health",return_value=h),patch.object(m,"convergence"),patch.object(m.initial,"activation",return_value=actual),patch.object(m.initial,"kubectl") as write,patch.object(m.time,"sleep"):
            m.enroll(c,lambda *_:None,lambda _:None)
            write.assert_not_called()
            with self.assertRaisesRegex(ValueError,"window"):m.observe(c,lambda *_:None,False)
        actual["updated_at"]=m.initial.now().isoformat()
        with self.assertRaisesRegex(ValueError,"outside"):m.check_activation(c,actual)


if __name__ == "__main__":
    unittest.main()
