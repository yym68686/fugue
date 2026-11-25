import copy
import unittest

from scripts import release_idle_front_standby as standby
from scripts import test_retire_front_hostport as hostport


class IdleFrontStandbyTests(unittest.TestCase):
    def fixture(self):
        origin, shared = hostport.declaration(), hostport.workload()
        origin["legacyTemplateDigest"] = standby.kube.digest(shared["spec"]["template"])
        _, live = standby.retirement.derive(origin, shared)
        live["metadata"].update(uid="daemon-uid", resourceVersion="123")
        config = {"daemonSetUID": "daemon-uid", "specDigest": standby.kube.digest(live["spec"])}
        return config, origin, live

    def test_only_exact_single_node_affinity_can_change(self):
        config, origin, live = self.fixture()
        patch = standby.workload(config, origin, live)
        self.assertEqual([item["path"] for item in patch], ["/metadata/uid", "/metadata/resourceVersion", "/spec",
                                                           "/metadata/annotations", "/spec/template/spec/affinity"])
        self.assertEqual(patch[-1]["value"], standby.retirement.node_affinity(origin["node"], "NotIn"))
        result = copy.deepcopy(live)
        result["metadata"]["annotations"] = patch[-2]["value"]
        result["spec"]["template"]["spec"]["affinity"] = patch[-1]["value"]
        self.assertEqual(standby.workload(config, origin, result), [])

    def test_rejects_uid_hostport_template_or_owner_drift(self):
        config, origin, live = self.fixture()
        for edit in (lambda d: d["metadata"].update(uid="foreign"),
                     lambda d: d["metadata"]["annotations"].update({standby.ANNOTATION: "foreign"}),
                     lambda d: d["spec"]["template"]["spec"]["containers"][0].update(image="foreign"),
                     lambda d: d["spec"]["template"]["spec"]["containers"][0]["ports"][0].update(hostPort=80)):
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                changed = copy.deepcopy(live)
                edit(changed)
                standby.workload(config, origin, changed)


if __name__ == "__main__":
    unittest.main()
