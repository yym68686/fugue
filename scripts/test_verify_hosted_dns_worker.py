import unittest

from scripts.provision_observation_worker_database import ProvisionError
from scripts.verify_hosted_dns_worker import verify_standby_metrics


STANDBY = """# TYPE fugue_hosted_dns_owner gauge
fugue_hosted_dns_owner{owner="api"} 1.000000
fugue_hosted_dns_worker_storage_available 1.000000
"""


class HostedDNSStandbyMetricsTest(unittest.TestCase):
    def test_accepts_go_prometheus_gauges(self):
        verify_standby_metrics(STANDBY)
        verify_standby_metrics(STANDBY.replace("1.000000", "1"))

    def test_refuses_wrong_owner_unavailable_storage_or_completed_work(self):
        for metrics in (
            STANDBY.replace('owner="api"', 'owner="worker"'),
            STANDBY.replace("storage_available 1", "storage_available 0"),
            STANDBY + "fugue_hosted_dns_last_completed_at_seconds 42.000000\n",
        ):
            with self.subTest(metrics=metrics), self.assertRaises(ProvisionError):
                verify_standby_metrics(metrics)
