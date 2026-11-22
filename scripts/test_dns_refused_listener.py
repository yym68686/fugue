import datetime as dt
import struct
import subprocess
import unittest
import tempfile
import json
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

from scripts import dns_refused_listener as listener
from scripts import dns_refused_wire as wire
from scripts import preview_dns_refused_stage as preview
from scripts.test_dns_no_endpoint_observation import fixture
from scripts import test_dns_transport_failed_predecessor as staged

def uid(i): return str(UUID(int=i))
def sites():
    return {"control":"1.1.1.1","sources":[{"pod":"pod-"+v,"pod_uid":uid(i+10),"node":"node-"+v,
            "node_uid":uid(i+20),"ip":ip,"country":v,"container":"probe",
            "image":"registry.example/probe@sha256:"+"1"*64} for i,(ip,v) in enumerate((("8.8.8.8","us"),("9.9.9.9","de")))]}

class RefusedTests(unittest.TestCase):
    def test_pair_rejects_single_receipt_and_keeps_role_without_private_input(self):
        from scripts import recover_dns_transport as recovery
        data = {"candidate":{"snapshot":{"ready":True}},"fallback":{"snapshot":{"ready":False},"signing_key":"private-value"}}
        for result, category, role in (
            (subprocess.CompletedProcess([],0,'{"verified":true}',""),"verifier_failed",None),
            (subprocess.CompletedProcess([],1,"","fallback: DNS observation lacks complete record readiness"),"record_readiness","fallback")):
            with patch.object(recovery.subprocess,"run",return_value=result), self.assertRaises(recovery.DNSObservationRejected) as raised:
                recovery.verify_observation_input(data,"/test/verifier",["--verify-serving-pair"],'{"verified_serving_pair":true}')
            self.assertEqual(raised.exception.category,category)
            self.assertEqual(getattr(raised.exception,"backend_role",None),role)
            if role:
                self.assertEqual(raised.exception.__notes__,["refused listener signed pair: fallback","snapshot ready at capture: no"])

    def test_signed_readiness_rejection_reports_only_the_capture_flag(self):
        from scripts import recover_dns_transport as recovery
        for flag, expected in ((True, "yes"), (False, "no"), ("private-input", "unknown")):
            request = {"snapshot": {"ready": flag}, "signing_key": "private-input"}
            result = subprocess.CompletedProcess([], 1, stdout="", stderr="DNS observation lacks complete record readiness")
            with patch.object(recovery, "candidate_request", return_value=request), \
                 patch.object(recovery.subprocess, "run", return_value=result), \
                 self.assertRaises(recovery.DNSObservationRejected) as raised:
                recovery.verify_candidate({}, {}, "/test/verifier")
            self.assertEqual(raised.exception.category, "record_readiness")
            self.assertEqual(raised.exception.__notes__, ["snapshot ready at capture: "+expected])

    def test_private_receiver_discards_transient_read_but_not_bad_wire_or_signature(self):
        t=preview.transport
        for fault, retries in ((t.ObservationUnavailable("503"),True),(ValueError("wire failure"),False)):
            with patch.object(t,"_observe_private_receiver",side_effect=[fault,{"current":True}]) as fn,patch.object(t.time,"sleep"):
                call=lambda:t.observe_recovery_fallback({}, {}, {}, {}, {},policy_path=None,observer=None,verifier=None)
                if retries:
                    self.assertEqual(call(),{"current":True})
                else:
                    with self.assertRaises(ValueError):call()
                self.assertEqual(fn.call_count,2 if retries else 1)

    def test_preview_counter_is_not_a_live_transport_declaration(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/"intent.json"
            cfg=staged.FailedPredecessorTests();cfg.setUp()
            cfg.config["preview_attempt"]=2
            p.write_text(json.dumps(cfg.config))
            self.assertNotIn("preview_attempt",preview.transport.load_config(p,preview=True))
            with self.assertRaises(ValueError):preview.transport.load_config(p)
            cfg.config["preview_attempt"]=True
            p.write_text(json.dumps(cfg.config))
            with self.assertRaises(ValueError):preview.transport.load_config(p,preview=True)

    def test_sources(self):
        wire.validate_sources(sites())
        for key in ("node","node_uid","ip","pod","pod_uid","country"):
            spec=sites()
            spec["sources"][1][key]=spec["sources"][0][key]
            with self.subTest(key=key), self.assertRaises(ValueError): wire.validate_sources(spec)

    def test_control_checks_query_type_authority_and_sequence(self):
        q=wire.query_packet("example.test","A",32)
        raw=q[:2]+struct.pack("!5H",0x8400,1,1,0,0)+q[12:]+b"\xc0\x0c"+struct.pack("!HHIH",1,1,30,4)+b"\x08\x08\x08\x08"
        wire.positive(raw,q,False)
        wire.positive(struct.pack("!H",len(raw))+raw,q,True)
        for bad in (raw[:-1],raw[:2]+b"\x80\x00"+raw[4:],b"XX"+raw[2:],raw[:2]+b"\x84\x02"+raw[4:],
                    raw[:len(q)+2]+b"\x00\x1c"+raw[len(q)+4:],raw[:-4]+b"\x7f\0\0\1"):
            with self.assertRaises(ValueError): wire.positive(bad,q,False)

    def test_only_explicit_refusal_works_never_timeout_or_exec_error(self):
        addr="8.8.4.4"
        args=("default",sites()["sources"][0],addr,{"hostname":"example.test","type":"A"})
        for mode,error in (("tcp",f"nc: {addr} ({addr}:53): Connection refused"),("udp","nslookup: read: Connection refused")):
            for msg in (error,"","connection timed out","command terminated with exit code 1",error+"\npermission denied"):
                with self.subTest(mode=mode,msg=msg),patch.object(wire,"invoke",return_value=subprocess.CompletedProcess([],1,b"",msg.encode())):
                    if msg==error: self.assertEqual(wire.sample(*args,mode,False,b"12")["outcome"],"connection_refused")
                    else:
                        with self.assertRaises(ValueError): wire.sample(*args,mode,False,b"12")

    def test_report_binds_full_topology_all_eight_samples_and_time(self):
        svc,slices,pods,node,d=fixture()
        t=listener.explicit_unready_snapshot(svc,slices,pods,node,d)
        spec=sites()
        failure={"refused_listener":spec,"query":{"hostname":"example.test","type":"A"},"public_service_uid":t["service"][1]}
        stage={"address":t["address"],"failed_predecessor":failure}
        now=dt.datetime.now(dt.timezone.utc)
        witness={"failed_predecessor":{"empty_topology":t},"old_uid":t["pod"][1],"node_uid":t["node"][1]}
        rows=[{"pod_uid":s["pod_uid"],"country":s["country"],"server":ip,"transport":kind,"outcome":r}
              for s in spec["sources"] for ip,r in ((t["address"],"connection_refused"),(spec["control"],"authoritative_positive"))
              for kind in ("tcp","udp")]
        receipt={"schema":"fugue.dns.explicit-refusal/v1","query":failure["query"],"authorizes_traffic":False,"challenge":"0"*32,
                 "finished_at":now.isoformat(),"started_at":now.isoformat(),"topology":t,"public_topology":t,"samples":rows}
        listener.validate(receipt,stage,svc,witness,now)
        for changes in ({"started_at":(now-dt.timedelta(seconds=61)).isoformat()},{"samples":rows[:-1]},{"samples":rows[:-1]+[rows[0]]},
                        {"challenge":""},{"authorizes_traffic":True},{"topology":dict(t,selector={"app":"different"})}):
            with self.assertRaises(ValueError): listener.validate(dict(receipt,**changes),stage,svc,witness,now)

    def test_node_status_revision_requires_a_completely_new_two_site_sample(self):
        svc,slices,pods,node,d=fixture()
        t=listener.explicit_unready_snapshot(svc,slices,pods,node,d)
        def advanced(t):
            n=t["node"]
            return dict(t,node=(n[0],n[1],str(int(n[2])+1)))
        updated=advanced(t)
        policy={"servers":[d["address"],"1.1.1.1"],"queries":[{"hostname":"example.test","type":"A",
                "answer_source":"verified_serving_policy","minimum_answers":1}]}
        stage={"address":d["address"],"failed_predecessor":{"refused_listener":sites(),"query":{"hostname":"example.test","type":"A"}}}
        for snapshots,ok,nprobes in (([t,updated,updated,updated],True,2),([t,updated,updated,advanced(updated)],False,2),
                                     ([t,dict(updated,slice=("other",uid(31),"12"))],False,1)):
            with self.subTest(ok=ok,n=nprobes),patch.object(listener.transport,"read_json",return_value=svc), \
                    patch.object(listener.transport,"read_observation_declaration",return_value=policy), \
                    patch.object(listener,"empty",side_effect=[(x,None) for x in snapshots]), \
                    patch.object(wire,"observe",side_effect=[{"challenge":"a"*32},{"challenge":"b"*32}]) as probe:
                read=lambda:listener.observe({"listeners":[d],"namespace":d["namespace"]},stage,d,svc,None)
                if ok:
                    report=read()
                    self.assertEqual(report["challenge"],"b"*32)
                    self.assertEqual(report["public_topology"],updated)
                else:
                    with self.assertRaises(ValueError):read()
                self.assertEqual(probe.call_count,nprobes)

    def test_only_new_node_revision_can_rebind_the_verified_dry_run(self):
        svc,slices,pods,node,d=fixture()
        t=listener.explicit_unready_snapshot(svc,slices,pods,node,d)
        n=t["node"]
        before={"assignment":"signed","node_uid":n[1],"candidate_uid":"candidate","failed_predecessor":
                {"assignment":"old","empty_topology":t}}
        def changed(topology):
            return dict(before,failed_predecessor=dict(before["failed_predecessor"],empty_topology=topology))
        updated=dict(t,node=(n[0],n[1],str(int(n[2])+1)))
        fresh=changed(updated)
        self.assertTrue(listener.same_plan_after_node_update(before,fresh))
        for invalid in (dict(fresh,assignment="changed"),dict(fresh,candidate_uid="changed"),
                        changed(dict(updated,service=("new",uid(39),"100"))),changed(dict(updated,port=8053)),
                        changed(dict(updated,pod=("new",uid(38),"13"))),changed(dict(updated,node=(n[0],uid(18),"100"))),
                        changed(dict(updated,node=(n[0],n[1],"0")))):
            self.assertFalse(listener.same_plan_after_node_update(before,invalid))
        tr=preview.transport
        with patch.object(tr,"handoff_witness",return_value=fresh):
            with self.assertRaises(ValueError):tr.revalidate_handoff_witness({},d,svc,before)
            self.assertEqual(tr.revalidate_handoff_witness({},d,svc,before,fresh_refusal=True),fresh)

    def test_preview_failed_source_never_verifies_or_writes(self):
        stage={"address":"8.8.4.4","failed_predecessor":{"refused_listener":sites()}}
        config={"handoffs":[stage],"namespace":"test"}
        with patch.object(preview.transport,"stage_diagnostic",return_value={"name":"diagnostic"}),patch.object(preview.transport,"read_json"), \
             patch.object(listener,"observe",side_effect=ValueError("source changed")),patch.object(preview.transport,"handoff_witness") as hw, \
             patch.object(preview.transport,"kubectl") as write,self.assertRaisesRegex(ValueError,"source changed"):
            preview.preview(config,None,None,None)
        hw.assert_not_called()
        write.assert_not_called()

    def test_two_distinct_live_challenges_required_for_two_selector_writes(self):
        for mode in ("fail_first","fail_second","duplicate","success"):
            with self.subTest(mode=mode):
                fx=staged.FailedPredecessorTests()
                fx.setUp()
                fx.failure["refused_listener"]=sites()
                transport=preview.transport
                calls=[]
                def observe(config,stage,listener_,previous):
                    port=listener_["port"]
                    calls.append(("failure_probe",port))
                    if (mode,port) in (("fail_first",15353),("fail_second",53)):
                        raise ValueError("socket no longer refused")
                    return {"challenge":"1"*32 if port==15353 or mode=="duplicate" else "2"*32}
                def verify(config,stage,witness,port):
                    return {"verified_backend":dict(fx.witness,candidate_uid=witness["candidate_uid"])}
                def fallback(*args):
                    return dict(fx.witness,candidate_uid=fx.fallback["pod_uid"])
                with patch.object(transport,"kubectl",side_effect=lambda *args,**kw:calls.append(("write",args[1]))), \
                     patch.object(transport,"revalidate_handoff_witness",return_value=fx.witness), \
                     patch.object(listener,"validate"),patch.object(transport,"restore_staged_listener",return_value=None):
                    def execute():
                        transport.apply_transport_steps(fx.config,fx.operations,{"applied":[]},lambda:None,verify,fallback,observe)
                    if mode=="success": execute()
                    else:
                        with self.assertRaises(ValueError): execute()
                self.assertEqual(("write","diagnostic") in calls,mode!="fail_first")
                self.assertEqual(("write","public") in calls,mode=="success")

    def test_node_changes_after_wire_never_authorize_the_interrupted_sample(self):
        svc,slices,pods,node,d=fixture()
        old=listener.explicit_unready_snapshot(svc,slices,pods,node,d)
        n=old["node"]
        new=dict(old,node=(n[0],n[1],str(int(n[2])+1)))
        for scenario in ("stable_retry","changed_again","replayed"):
            fx=staged.FailedPredecessorTests();fx.setUp()
            fx.failure["refused_listener"]=sites()
            fresh=dict(fx.witness,failed_predecessor=dict(fx.witness["failed_predecessor"],empty_topology=new))
            obs=[{"challenge":"1"*32,"topology":old},{"challenge":("1" if scenario=="replayed" else "2")*32,
                "topology":old if scenario=="changed_again" else new}]
            def validate(report,stage,current,witness,now):
                if report["topology"]!=witness["failed_predecessor"]["empty_topology"]:raise ValueError("unbound")
            def stage_check(config,stage,witness,port):
                return {"verified_backend":dict(fresh,candidate_uid=fresh["candidate_uid"])}
            def fallback(*args):return dict(fresh,candidate_uid=fresh["fallback_uid"])
            with self.subTest(scenario=scenario),patch.object(listener,"validate",side_effect=validate), \
                    patch.object(listener.transport,"kubectl") as write,patch.object(listener.transport,"revalidate_handoff_witness",return_value=fresh), \
                    patch.object(wire,"observe",side_effect=obs) as observe:
                report={"applied":[]}
                apply=lambda:listener.transport.apply_transport_steps(fx.config,fx.operations[:1],report,
                    lambda:None,stage_check,fallback,lambda *a:observe())
                if scenario=="stable_retry":
                    apply()
                    self.assertEqual([r["challenge"] for r in report["predecessor_verifications"]],["2"*32])
                    self.assertEqual(write.call_count,1)
                else:
                    with self.assertRaises(ValueError):apply()
                    write.assert_not_called()
                self.assertEqual(observe.call_count,2)
