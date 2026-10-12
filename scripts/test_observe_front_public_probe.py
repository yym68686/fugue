import unittest
from unittest.mock import patch
from scripts import observe_front_public_probe as external


class ExternalFrontTests(unittest.TestCase):
    def test_failure_retains_exact_attempt_and_completed_observations(self):
        config = {'listeners': [{'name': 'selected', 'address': '8.8.8.8', 'port': 15443}]}
        profiles = {'selected': {'node': 'node-a', 'group': 'cell-a', 'probes': [{'host': 'api.example.test', 'path': '/'}]}}
        progress = {}
        results = [{'proof': {'edge': 'node-a', 'group': 'cell-a'}, 'publication_retries': 0}, TimeoutError('private transport diagnostic')]
        with patch.object(external.front, 'stable_proof_pair', side_effect=results):
            with self.assertRaises(TimeoutError):
                external.observe(config, profiles, samples=2, interval=0, listener_name='selected', progress=progress)
        self.assertEqual(len(progress['observations']), 1)
        self.assertEqual(progress['current_attempt']['listener'], 'selected')
        self.assertEqual(progress['current_attempt']['sample'], 1)
        self.assertEqual(progress['current_attempt']['host'], 'api.example.test')
        self.assertNotIn('private transport diagnostic', str(progress))

    def test_selected_release_does_not_probe_unrelated_offline_listener(self):
        config = {'listeners': [{'name': 'changed', 'address': '8.8.8.8', 'port': 15443},
                                {'name': 'unchanged', 'address': '9.9.9.9', 'port': 15443}]}
        profiles = {name: {'node': 'node-a', 'group': 'cell-a', 'probes': [{'host': 'api.example.test', 'path': '/'}]}
                    for name in ['changed', 'unchanged']}
        def proof(address, port, host, path, node, group):
            if address == '9.9.9.9':
                raise TimeoutError('unrelated endpoint offline')
            return {'proof': {'edge': node, 'group': group}, 'publication_retries': 0}
        with patch.object(external.front, 'stable_proof_pair', side_effect=proof):
            evidence = external.observe(config, profiles, samples=1, listener_name='changed')
            self.assertEqual(evidence['observed_listeners'], ['changed'])
            self.assertFalse(evidence['authorizes_traffic'])
            with self.assertRaises(TimeoutError):
                external.observe(config, profiles, samples=1)
            with self.assertRaises(ValueError):
                external.observe(config, profiles, samples=1, listener_name='missing')

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
