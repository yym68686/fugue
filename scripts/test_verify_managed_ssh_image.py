import copy
import unittest

from scripts import verify_managed_ssh_image as artifact


class ManagedSSHImageTest(unittest.TestCase):
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
