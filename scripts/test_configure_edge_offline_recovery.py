import copy
import unittest
from scripts import configure_edge_offline as offline
from scripts import test_configure_edge_offline as fixtures

class OfflineRecoveryConfigurationTests(unittest.TestCase):
    def test_recovery_policy_is_bounded_and_explicit(self):
        config, _ = fixtures.OfflineConfigurationTests().fixture()
        hold = config['projection_policy']['offline_edges'][0]
        hold['recovery'] = {'hostname':'api.example.test','path':'/','probe_interval_seconds':300,'probe_timeout_seconds':5,'success_threshold':3,'failure_threshold':1,'freshness_seconds':1800}
        offline.validate(config)
        for key,value in [('probe_interval_seconds',299),('success_threshold',1),('freshness_seconds',600)]:
            bad = copy.deepcopy(config); bad['projection_policy']['offline_edges'][0]['recovery'][key] = value
            with self.assertRaises(ValueError): offline.validate(bad)
        for path in ['//x','/x?bad=1','']:
            bad = copy.deepcopy(config); bad['projection_policy']['offline_edges'][0]['recovery']['path'] = path
            with self.assertRaises(ValueError): offline.validate(bad)

if __name__ == '__main__': unittest.main()
