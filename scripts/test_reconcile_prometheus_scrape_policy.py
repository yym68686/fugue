import json
from pathlib import Path
import re
import unittest
from scripts.reconcile_prometheus_scrape_policy import project


class ScrapePolicyTest(unittest.TestCase):
    def test_exact_component_port_contract_and_idempotence(self):
        policy = json.loads(Path("deploy/environments/production/observability/scrape-policy.json").read_text())
        old = '''scrape_configs:
  - job_name: fugue-control-plane-pods
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
        regex: "api|dns"
        action: keep
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: "http|health|metrics"
        action: keep
      - target_label: namespace
        replacement: fugue-system
  - job_name: unrelated
    metrics_path: /custom
'''
        new = project(old, policy)
        self.assertEqual(project(new, policy), new)
        self.assertIn("job_name: unrelated\n    metrics_path: /custom", new)
        regex = "|".join(policy["componentPorts"])
        for target in ["api;metrics", "dns-country-de;http", "edge-country-de-worker-a;health", "edge-worker-b;health"]:
            self.assertIsNotNone(re.fullmatch(regex, target), target)
        for target in ["api;http", "caddy;https-slot", "tenant-app;metrics", "dns;dns-udp", "edge-worker-a;https-slot"]:
            self.assertIsNone(re.fullmatch(regex, target), target)


if __name__ == "__main__":
    unittest.main()
