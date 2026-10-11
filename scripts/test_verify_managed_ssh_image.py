import copy
import unittest
import json
import sys
import time
from unittest import mock

from scripts import verify_managed_ssh_image as artifact


class ManagedSSHImageTest(unittest.TestCase):
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
        metadata={"Id":"sha256:"+"b"*64}
        result={"status":{"id":metadata["Id"],"repoDigests":[image]}}
        with mock.patch.object(artifact,"run",side_effect=[b"",json.dumps(result).encode()]) as execute:
            self.assertEqual(artifact.pull_candidate_into_test_node(node,image,metadata),metadata["Id"])
            self.assertEqual(execute.call_args_list[0].args[0],["docker","exec",node,"crictl","pull",image])
        result["status"]["id"]="sha256:"+"c"*64
        with mock.patch.object(artifact,"run",side_effect=[b"",json.dumps(result).encode()]):
            with self.assertRaises(ValueError):artifact.pull_candidate_into_test_node(node,image,metadata)
        with mock.patch.object(artifact,"run") as execute:
            with self.assertRaises(ValueError):artifact.pull_candidate_into_test_node("production-node",image,metadata)
            execute.assert_not_called()

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
