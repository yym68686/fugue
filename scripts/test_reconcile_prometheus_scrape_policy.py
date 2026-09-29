import json
from pathlib import Path
import re
import unittest
from unittest.mock import patch
from scripts.reconcile_prometheus_scrape_policy import additional_scrapes, project, verify_queries, public_dns_targets_match


class ScrapePolicyTest(unittest.TestCase):
    def test_public_dns_verification_rejects_old_or_missing_backends(self):
        check = {"job": "public-dns", "serviceNamePattern": "dns-public-[a-z]+"}
        services = [{"metadata": {"name": name}} for name in ("dns-public-east", "dns-public-west")]
        endpoints = [{"metadata": service["metadata"], "subsets": [{"addresses": [{"targetRef": {"kind": "Pod", "name": pod}}]}]}
                     for service, pod in zip(services, ("east-slot-b", "west-slot-a"))]
        targets = [{"health": "up", "labels": {"job": "public-dns", "service": service["metadata"]["name"], "pod": pod}}
                   for service, pod in zip(services, ("east-slot-b", "west-slot-a"))]
        self.assertTrue(public_dns_targets_match(services, endpoints, targets, check))
        self.assertFalse(public_dns_targets_match(services, endpoints, targets[:1], check))
        endpoints[0]["subsets"][0]["addresses"][0]["targetRef"]["name"] = "east-slot-c"
        self.assertFalse(public_dns_targets_match(services, endpoints, targets, check))
        targets[0]["labels"]["pod"] = "east-slot-c"
        self.assertTrue(public_dns_targets_match(services, endpoints, targets, check))
        targets[0]["health"] = "down"
        self.assertFalse(public_dns_targets_match(services, endpoints, targets, check))
        self.assertFalse(public_dns_targets_match([], [], [], check))

    def test_public_dns_discovery_follows_service_backends(self):
        policy = json.loads(Path("deploy/environments/production/observability/scrape-policy.json").read_text())
        job = next(job for job in policy["additionalScrapeConfigs"] if job["job_name"] == "fugue-public-dns")
        self.assertEqual(job["kubernetes_sd_configs"][0]["role"], "endpoints")
        self.assertNotIn("static_configs", job)
        gates = [rule for rule in job["relabel_configs"] if rule.get("action") == "keep"]

        def included(service, owner="fugue-dns-transport", container="dns", port="http", protocol="TCP"):
            labels = {"__meta_kubernetes_service_name": service,
                      "__meta_kubernetes_service_label_app_kubernetes_io_managed_by": owner,
                      "__meta_kubernetes_pod_container_name": container,
                      "__meta_kubernetes_pod_container_port_name": port,
                      "__meta_kubernetes_pod_container_port_protocol": protocol}
            return all(re.fullmatch(rule["regex"], ";".join(labels.get(key, "") for key in rule["source_labels"])) for rule in gates)

        # Public identity is the Service, not a country, slot, or Pod name.
        # Endpoints discovery includes the selected Pod's extra HTTP port.
        self.assertTrue(included("fugue-dns-public-pool-one"))
        self.assertTrue(included("fugue-dns-public-pool-two", port="health"))
        self.assertFalse(included("fugue-dns-transport-pool-one"))
        self.assertFalse(included("fugue-dns-public-pool-one", owner="unrelated"))
        self.assertFalse(included("fugue-dns-public-pool-one", container="sidecar"))
        self.assertFalse(included("fugue-dns-public-pool-one", port="dns-udp", protocol="UDP"))
        self.assertFalse(included("fugue-dns-public-pool-one", port="dns-tcp"))
        labels = {r["target_label"]: r for r in job["relabel_configs"] if "target_label" in r}
        self.assertEqual(labels["service"]["source_labels"], ["__meta_kubernetes_service_name"])
        self.assertEqual(labels["pod"]["source_labels"], ["__meta_kubernetes_pod_name"])
        self.assertEqual(labels["serving_role"]["replacement"], "public")

    def test_verification_does_not_accept_absent_metrics(self):
        with patch("scripts.reconcile_prometheus_scrape_policy.query_prometheus", return_value=[]), \
             patch("scripts.reconcile_prometheus_scrape_policy.time.monotonic", side_effect=[0, 121]):
            with self.assertRaisesRegex(RuntimeError, "metric verification failed"):
                verify_queries("/proxy", ["up{job=\"storage\"} == 1"])

    def test_verification_waits_for_all_predicates_together(self):
        with patch("scripts.reconcile_prometheus_scrape_policy.query_prometheus", side_effect=[
                [{"value": [1, "1"]}], [], [], [{"value": [2, "1"]}],
                [{"value": [3, "1"]}], [{"value": [3, "1"]}]]) as query, \
             patch("scripts.reconcile_prometheus_scrape_policy.time.sleep") as sleep:
            verify_queries("/proxy", ["healthy", "fresh"])
            self.assertEqual(query.call_count, 6)
            self.assertEqual(sleep.call_count, 2)

    def test_additional_job_lifecycle_preserves_unrelated_configuration(self):
        original = '''global:
  scrape_interval: 30s
scrape_configs:
  - job_name: unrelated
    static_configs: [{targets: ["localhost:9090"]}]
remote_write:
  - url: http://metrics.example.test/api/v1/write
'''
        policy = {"additionalScrapeConfigs": [{"job_name": "node-storage", "scrape_interval": "30s"}]}
        added = additional_scrapes(original, policy)
        self.assertEqual(added, additional_scrapes(added, policy))
        self.assertLess(added.index("job_name: node-storage"), added.index("remote_write:"))
        changed = additional_scrapes(added, {"additionalScrapeConfigs": [{"job_name": "node-storage", "scrape_interval": "60s"}]})
        self.assertIn('scrape_interval: "60s"', changed)
        self.assertEqual(original, additional_scrapes(changed, {"additionalScrapeConfigs": []}))

    def test_conflicting_jobs_and_broken_ownership_fail_closed(self):
        for name in ['node-storage', '"node-storage"', "'node-storage'"]:
            with self.assertRaises(ValueError):
                additional_scrapes("scrape_configs:\n  - job_name: " + name + "\n", {
                    "additionalScrapeConfigs": [{"job_name": "node-storage"}]})
        for jobs in [[{"job_name": "same"}, {"job_name": "same"}], [{"job_name": "bad\nname"}], "invalid"]:
            with self.assertRaises(ValueError):
                additional_scrapes("scrape_configs:\n", {"additionalScrapeConfigs": jobs})
        with self.assertRaises(ValueError):
            additional_scrapes("scrape_configs:\n  # BEGIN Fugue additional scrape configs\n", {})

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
