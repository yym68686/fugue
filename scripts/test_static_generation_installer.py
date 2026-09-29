import importlib.util
import pathlib
import tempfile
import unittest
from unittest import mock

source=pathlib.Path(__file__).resolve().parents[1]/'internal/cli/static_edge_generation.py'
spec=importlib.util.spec_from_file_location('generation_installer',source)
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class GenerationIsolation(unittest.TestCase):
    def test_create_only_files_refuse_overwrite_and_symlinks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp).resolve()
            p=root/'asset'
            with mock.patch.object(module.os,'chown'):
                module.put(p,b'original',0,0,0o600)
                module.put(p,b'original',0,0,0o600)
                with self.assertRaises(ValueError):module.put(p,b'replacement',0,0,0o600)
                self.assertEqual(p.read_bytes(),b'original')
                link=root/'link';link.symlink_to(p)
                with self.assertRaises(ValueError):module.put(link,b'original',0,0,0o600)

    def test_generation_lifecycle_has_no_forced_retirement(self):
        text=module.unit_text(pathlib.Path('/opt/fugue-static-generations/fixture-g1'),'caddy','caddy.json','caddy','512M','100%').decode()
        self.assertIn('TimeoutStopSec=infinity',text)
        self.assertIn('SendSIGKILL=no',text)
        self.assertNotIn('ExecStop=',text)
        self.assertNotIn('RuntimeMaxSec=',text)

    def test_predecessor_change_prevents_installation(self):
        plan={'preserve':[{'unit':'previous.service','pid':9,'config':'/etc/fixture.json','sha256':'a'*64}]}
        with mock.patch.object(module,'command',return_value='10'):
            with self.assertRaisesRegex(ValueError,'predecessor process changed'):module.fence(plan)

if __name__=='__main__':unittest.main()
