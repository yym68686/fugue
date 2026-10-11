import copy
import unittest
from unittest import mock

from scripts import verify_ssh_front_image as artifact


class SSHFrontArtifactTest(unittest.TestCase):
    def test_requires_exact_front_entrypoint_source_and_platform(self):
        source='a'*40
        metadata={'Os':'linux','Architecture':'amd64','Config':{'Labels':{'org.opencontainers.image.revision':source},'Entrypoint':['/usr/local/bin/fugue-ssh-front']}}
        self.assertEqual(artifact.validate_metadata(metadata,source),'amd64')
        for field,value in [('Os','darwin'),('Architecture','386'),('Config',{})]:
            changed=copy.deepcopy(metadata)
            changed[field]=value
            with self.assertRaises(ValueError):artifact.validate_metadata(changed,source)
        with self.assertRaises(ValueError):artifact.validate_metadata(metadata,'b'*40)
        image='registry.example/front@sha256:'+'b'*64
        with self.assertRaises(ValueError):artifact.validate_metadata(metadata,source,image)
        metadata['RepoDigests']=[image]
        self.assertEqual(artifact.validate_metadata(metadata,source,image),'amd64')

    def test_refuses_mutable_or_local_only_artifacts_before_execution(self):
        with mock.patch.object(artifact,'run') as execute:
            for image in ['example/ssh:latest','sha256:'+'a'*64,'example/ssh@sha256:bad']:
                with self.assertRaises(ValueError):artifact.verify(image,'b'*40)
            execute.assert_not_called()


if __name__=='__main__':
    unittest.main()
