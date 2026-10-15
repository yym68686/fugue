import unittest
from scripts.select_dns_configuration import select


class SelectionTests(unittest.TestCase):
    def test_unrelated_push_never_competes_for_dns_transport(self):
        self.assertEqual(select(['internal/managedssh/server.go', 'docs/notes.md', '.github/workflows/ci.yml']),
                         {'transport': False, 'ports': False, 'slots': False})

    def test_each_independent_declaration_selects_only_its_writer(self):
        for path, writer in [('deploy/environments/production/dns-transport/transport.json', 'transport'),
                             ('deploy/environments/production/dns-port-retirement/region.json', 'ports'),
                             ('deploy/environments/production/dns-slot-retirement/intent.json', 'slots')]:
            self.assertEqual(select([path]), {key: key == writer for key in ['transport', 'ports', 'slots']})

    def test_executor_changes_do_not_replay_historical_retirements(self):
        self.assertEqual(select(['scripts/retire_dns_slots.py', 'scripts/retire_dns_hostport.py']),
                         {'transport': False, 'ports': False, 'slots': False})
        self.assertTrue(select(['scripts/recover_dns_transport.py'])['transport'])
