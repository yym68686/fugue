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
