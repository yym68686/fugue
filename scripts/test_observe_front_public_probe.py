import unittest
from unittest.mock import patch
from scripts import observe_front_public_probe as external


class ExternalFrontTests(unittest.TestCase):
    def test_external_probe_requires_same_identity_and_publication(self):
        config = {"listeners": [{"name": "probe-one", "address": "8.8.8.8", "port": 15443}]}
        profiles = {"probe-one": {"node": "node-a", "group": "cell-a", "probes": [{"host": "api.example.test", "path": "/"}]}}
        for bad in [None, "publication", "node", "group", "unavailable"]:
            def proof(address, host, path, port=443):
                if bad == "unavailable" and port == 15443: raise TimeoutError("unavailable")
                return {"edge": "wrong" if bad == "node" else "node-a", "group": "wrong" if bad == "group" else "cell-a", "release": str(port) if bad == "publication" else "one"}
            with self.subTest(bad=bad), patch.object(external.front, "proof", side_effect=proof), patch.object(external.time, "sleep"):
                if bad:
                    with self.assertRaises((ValueError, TimeoutError)): external.observe(config, profiles)
                else:
                    evidence = external.observe(config, profiles)
                    self.assertFalse(evidence["authorizes_traffic"])
                    self.assertEqual(len(evidence["observations"]), 5)


if __name__ == "__main__": unittest.main()
