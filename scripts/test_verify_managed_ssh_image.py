import copy
import unittest
import json
import sys
import time
from unittest import mock

from scripts import verify_managed_ssh_image as artifact


class ManagedSSHImageTest(unittest.TestCase):
    def test_fixture_pull_avoids_exporting_partial_multiplatform_indexes(self):
        identity = 'sha256:'+'a'*64
        image = 'registry.example/fixture:1'
        digest = 'registry.example/fixture@sha256:'+'c'*64
        metadata = {'Id': 'sha256:'+'d'*64, 'Os': 'linux', 'Architecture': 'amd64', 'RepoDigests': [digest]}
        observed = {'status': {'id': identity, 'repoDigests': [digest]}, 'info': {'imageSpec': {'os': 'linux', 'architecture': 'amd64'}}}
        for family in ('artifact', 'transport'):
            node = 'fugue-ssh-'+family+'-0123456789-control-plane'
            output = [b'', json.dumps([metadata]).encode(), b'', json.dumps(observed).encode()]
            with self.subTest(family=family), mock.patch.object(artifact, 'run', side_effect=output) as execute:
                self.assertEqual(artifact.pull_fixture_into_test_node(node, image), identity)
                self.assertEqual(execute.call_args_list[2].args[0], ['docker', 'exec', node, 'crictl', 'pull', image])
                self.assertNotIn('docker-image', str(execute.call_args_list))
        observed['status']['repoDigests'] = ['registry.example/fixture@sha256:'+'b'*64]
        output[-1] = json.dumps(observed).encode()
        with mock.patch.object(artifact, 'run', side_effect=output):
            with self.assertRaisesRegex(ValueError, 'digest or native platform differs'):
                artifact.pull_fixture_into_test_node(node, image)

    def test_fixture_pull_never_targets_production_or_ambiguous_images(self):
        with mock.patch.object(artifact, 'run') as execute:
            for node, image in [('production-node', 'fixture:1'), ('fugue-ssh-artifact-0123456789-control-plane', 'fixture:1 --all-platforms')]:
                with self.assertRaises(ValueError):
                    artifact.pull_fixture_into_test_node(node, image)
            execute.assert_not_called()

    def test_native_acceptance_requires_cross_session_background_service(self):
        complete = b"--- PASS: TestManagedSSHIsolatedKubernetes\nnative detached bootstrap and cross-session Unix socket verified"
        artifact.validate_native_acceptance(complete)
        for incomplete in [b"", *complete.splitlines()]:
            with self.assertRaises(ValueError):
                artifact.validate_native_acceptance(incomplete)

    def test_timeout_cleans_descendants_holding_output_pipes(self):
        started=time.monotonic()
        child='import time; time.sleep(5)'
        parent='import subprocess,sys,time; subprocess.Popen([sys.executable,"-c",'+repr(child)+']); time.sleep(5)'
        with self.assertRaisesRegex(RuntimeError,'verification timed out'):
            artifact.run([sys.executable,'-c',parent],timeout=0.5)
        self.assertLess(time.monotonic()-started,3,'timeout waited on inherited child output pipes')

    def test_candidate_uses_cri_pull_and_checks_config_identity(self):
        image="registry.example/ssh@sha256:"+"a"*64
        node="fugue-ssh-artifact-0123456789-control-plane"
        metadata={"Id":"sha256:"+"b"*64, 'Os': 'linux', 'Architecture': 'amd64'}
        result={"status":{"id":"sha256:"+"c"*64,"repoDigests":[image]}, 'info': {'imageSpec': {'os': 'linux', 'architecture': 'amd64'}}}
        with mock.patch.object(artifact,"run",side_effect=[b"",json.dumps(result).encode()]) as execute:
            self.assertEqual(artifact.pull_candidate_into_test_node(node,image,metadata),result['status']['id'])
            self.assertEqual(execute.call_args_list[0].args[0],["docker","exec",node,"crictl","pull",image])
        result['info']['imageSpec']['architecture'] = 'arm64'
        with mock.patch.object(artifact,"run",side_effect=[b"",json.dumps(result).encode()]):
            with self.assertRaises(ValueError):artifact.pull_candidate_into_test_node(node,image,metadata)
        with mock.patch.object(artifact,"run") as execute:
            with self.assertRaises(ValueError):artifact.pull_candidate_into_test_node("production-node",image,metadata)
            execute.assert_not_called()

    def test_repository_digest_normalization_and_wrong_native_identity(self):
        digest = 'sha256:'+'a'*64
        self.assertEqual(artifact.canonical_repository_digest('fixture@'+digest), 'docker.io/library/fixture@'+digest)
        self.assertEqual(artifact.canonical_repository_digest('team/fixture@'+digest), 'docker.io/team/fixture@'+digest)
        for value in ('fixture:latest', 'fixture@sha256:bad'):
            with self.assertRaises(ValueError): artifact.canonical_repository_digest(value)
        metadata = {'Os': 'linux', 'Architecture': 'amd64'}
        observed = {'status': {'id': digest, 'repoDigests': ['docker.io/library/fixture@'+digest]}, 'info': {'imageSpec': {'os': 'linux', 'architecture': 'amd64'}}}
        self.assertEqual(artifact.verify_cri_identity(observed, metadata, {artifact.canonical_repository_digest('fixture@'+digest)}), digest)
        for field, value in [('os', 'windows'), ('architecture', 'arm64')]:
            wrong = copy.deepcopy(observed)
            wrong['info']['imageSpec'][field] = value
            with self.assertRaises(ValueError): artifact.verify_cri_identity(wrong, metadata, {'docker.io/library/fixture@'+digest})

    def test_rejects_unpinned_network_policy_code(self):
        with self.assertRaises(ValueError):
            artifact.policy_manifest(b"untrusted manifest")

    def test_requires_source_platform_and_entrypoint(self):
        source = "a"*40
        metadata = {"Os": "linux", "Architecture": "amd64", "Config": {"Labels": {"org.opencontainers.image.revision": source}, "Entrypoint": ["/usr/local/bin/fugue-managed-ssh"]}}
        self.assertEqual(artifact.validate_metadata(metadata, source), "amd64")
        for field, value in [("Os", "windows"), ("Architecture", "386"), ("Config", {})]:
            changed = copy.deepcopy(metadata)
            changed[field] = value
            with self.assertRaises(ValueError):
                artifact.validate_metadata(changed, source)
        with self.assertRaises(ValueError):
            artifact.validate_metadata(metadata, "b"*40)

    def test_rejects_mutable_image_before_docker_access(self):
        for image in ["example/ssh:latest", "example/ssh@sha256:bad"]:
            with self.assertRaises(ValueError):
                artifact.verify(image, "a"*40)


if __name__ == "__main__":
    unittest.main()
