import copy
import unittest
from reconcile_postgres_protection import validate


class ProtectionSafetyTest(unittest.TestCase):
    def setUp(self):
        self.config = {'expectedSystemID': '123', 'spec': {
            'primaryUpdateStrategy': 'supervised',
            'postgresql': {'synchronous': {'method': 'any', 'number': 1, 'dataDurability': 'required'}},
            'backup': {'target': 'prefer-standby', 'barmanObjectStore': {'destinationPath': 's3://test/archive'}}}}
        self.live = {'status': {'systemID': '123', 'readyInstances': 2, 'currentPrimary': 'db-1', 'targetPrimary': 'db-1'}, 'spec': {}}

    def test_accepts_protected_existing_database(self):
        validate(self.config, self.live)

    def test_refuses_wrong_database_or_degraded_quorum(self):
        for field, value in [('systemID', '456'), ('readyInstances', 1), ('targetPrimary', 'db-2')]:
            live = copy.deepcopy(self.live)
            live['status'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate(self.config, live)

    def test_refuses_destructive_spec_or_archive_replacement(self):
        config = copy.deepcopy(self.config)
        config['spec']['storage'] = {'size': '1Gi'}
        with self.assertRaises(ValueError):
            validate(config, self.live)
        self.live['spec']['backup'] = {'barmanObjectStore': {'destinationPath': 's3://other/archive'}}
        with self.assertRaises(ValueError):
            validate(self.config, self.live)


if __name__ == '__main__':
    unittest.main()
