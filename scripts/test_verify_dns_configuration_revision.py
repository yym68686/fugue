import unittest
from scripts.verify_dns_configuration_revision import workflow_projection


class RevisionTests(unittest.TestCase):
    source = "name: ci\non: push\npermissions: {contents: read}\njobs:\n  unrelated:\n    run: old\n  dns_configuration_plan:\n    run: select\n  dns_transport:\n    run: verify\n  dns_slot_retirement:\n    run: retire\n"

    def test_unrelated_job_changes_do_not_supersede_configuration(self):
        names = ['dns_configuration_plan', 'dns_transport']
        self.assertEqual(workflow_projection(self.source, names), workflow_projection(self.source.replace('run: old', 'run: new'), names))

    def test_global_authority_and_selected_job_changes_are_fenced(self):
        names = ['dns_configuration_plan', 'dns_transport', 'dns_slot_retirement']
        for old, new in [('contents: read', 'contents: write'), ('on: push', 'on: schedule'), ('run: select', 'run: other'), ('run: verify', 'run: bypass'), ('run: retire', 'run: other')]:
            self.assertNotEqual(workflow_projection(self.source, names), workflow_projection(self.source.replace(old, new), names))

    def test_missing_and_duplicate_jobs_fail_closed(self):
        for source in [self.source.replace('dns_transport:', 'other:'), self.source + '  dns_transport:\n    run: duplicate\n', 'name: no-jobs\n']:
            with self.assertRaises(ValueError):
                workflow_projection(source, ['dns_transport'])
