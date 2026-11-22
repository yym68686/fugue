import copy
import datetime
import unittest
from unittest.mock import patch
from uuid import UUID

from scripts import inspect_dns_rollback_ledger as audit
from scripts import test_dns_transport_rollback_authority as reference_tests


class InspectionTests(unittest.TestCase):
    def setUp(self):
        self.snap, self.full, self.lkg, ref = reference_tests.DNSTransportRollbackAuthorityTests().inputs()
        self.lkg["expires_at"]=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=1)).isoformat()
        self.node={"metadata":{"name":"node-a","uid":str(UUID(int=9))}}
        self.backends=[{"component":"dns-"+c, "service_account":"reader-"+c, "pod_uid":str(UUID(int=i)),
                        "revision":c*40, "image":"registry.example.test/dns@sha256:"+c*64, "target_port":53}
                       for i,c in enumerate(("a","b"),1)]
        self.d={"schema":"fugue.dns.rollback-ledger-inspection/v1", "namespace":"test", "node":"node-a",
                "node_uid":self.node["metadata"]["uid"],"api_address":"8.8.8.8","rollback":ref,"backends":self.backends}

    def run_inspection(self, *, cold="", replace="", bad_image=False):
        meta = {b["component"]: {"name":b["component"],"uid":b["pod_uid"],"resourceVersion":"41",
                                "annotations":{"fugue.pro/oci-revision":b["revision"]}} for b in self.backends}
        def read(*args):
            self.assertEqual(args[0],"get")
            if args[1:3]==("node","node-a"): return self.node
            self.assertEqual(args[1],"pod")
            m=copy.deepcopy(meta[args[2]])
            if replace==m["name"]: m["resourceVersion"]="42"
            return {"metadata":m}
        def backend(ns,selector,node,port):
            name=selector["app.kubernetes.io/component"]
            b=next(x for x in self.backends if x["component"]==name)
            self.assertEqual((ns,node,port),("test","node-a",53))
            return {"pod":{"metadata":meta[name],"spec":{"serviceAccountName":b["service_account"],
                    "containers":[{"name":"dns","image":"wrong" if bad_image else b["image"]}]}},
                    "snapshot":dict(self.snap,ready=(name!=cold))}
        def api(cfg,path):
            self.assertEqual(cfg["origin"],"https://api.example.test")
            if path=="/v1/admin/artifacts/parent/lkg": return {"lkg":self.lkg}
            self.assertEqual(path,"/v1/platform-state/artifacts/release_set?scope_key=global&channel=full&include_current_rollback_message=true")
            return self.full
        with patch.object(audit.transport,"read_json",side_effect=read),patch.object(audit.transport,"handoff_backend",side_effect=backend),patch.object(audit,"read_recovery_api",side_effect=api):
            return audit.inspect(self.d,"api.example.test")

    def test_independent_bindings_never_authorize_traffic(self):
        r=self.run_inspection()
        self.assertFalse(r["authorizes_traffic"])
        self.assertEqual([x["exact_rollback_bound"] for x in r["recipients"]],[True,True])
        cold=self.run_inspection(cold="dns-b")
        self.assertEqual([x["exact_rollback_bound"] for x in cold["recipients"]],[True,True])
        self.assertEqual([x["ready_at_read"] for x in cold["recipients"]],[True,False])
        self.assertFalse(cold["authorizes_traffic"])
        del self.full["current_rollback_message"]
        self.assertFalse(any(x["exact_rollback_bound"] for x in self.run_inspection()["recipients"]))

    def test_rejects_resource_replacement_and_wrong_pins(self):
        for args in ({"replace":"dns-a"},{"bad_image":True}):
            with self.subTest(args=args),self.assertRaises(ValueError): self.run_inspection(**args)
        self.d["backends"][1]["pod_uid"]=self.backends[0]["pod_uid"]
        with self.assertRaises(ValueError): self.run_inspection()

    def test_declaration_cannot_choose_credential_origin(self):
        self.d["origin"]="https://other.example.test"
        with self.assertRaises(ValueError): self.run_inspection()
