package api

import (
	"io"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/store"
)

func (s *Server) writeRobustnessMetrics(w io.Writer) {
	for _, guardian := range []string{"route-dns", "edge-tls", "node-health", "bundle-rollout", "traffic-safety", "release-guard", "request-attribution", "runtime-continuity", "platform-state-release"} {
		observability.WriteGaugeMetric(w, "fugue_robustness_guardian_enabled", "Robustness guardian classes available in the control plane.", map[string]string{"guardian": guardian}, 1)
	}

	now := time.Now().UTC()
	if edgeNodes, _, err := s.store.ListEdgeNodes(""); err == nil {
		expectedByGroup := mostCommonNonEmptyEdgeRouteGenerationByGroup(edgeNodes)
		observability.WriteMetricHeader(w, "fugue_robustness_node_generation_drift_seconds", "Seconds since a node was last observed with a generation that differs from the current majority generation.", "gauge")
		observability.WriteMetricHeader(w, "fugue_robustness_lkg_serving", "Whether a node reports serving an LKG or degraded cache generation.", "gauge")
		for _, node := range edgeNodes {
			expected := expectedByGroup[strings.TrimSpace(node.EdgeGroupID)]
			labels := map[string]string{"kind": "edge", "node_id": node.ID, "edge_group_id": node.EdgeGroupID}
			observability.WriteMetricSample(w, "fugue_robustness_node_generation_drift_seconds", labels, robustnessGenerationDriftSeconds(now, expected, firstNonEmpty(node.RouteBundleVersion, node.ServingGeneration), node.LastHeartbeatAt, node.UpdatedAt))
			observability.WriteMetricSample(w, "fugue_robustness_lkg_serving", labels, boolMetric(robustnessNodeServingLKG(node.CacheStatus, node.RouteBundleVersion, node.ServingGeneration, node.LKGGeneration)))
		}
	}
	if dnsNodes, err := s.store.ListDNSNodes(""); err == nil {
		expectedByScope := mostCommonNonEmptyDNSGenerationByScope(dnsNodes)
		observability.WriteMetricHeader(w, "fugue_robustness_dns_query_errors", "DNS node query errors reported by authoritative DNS nodes.", "gauge")
		for _, node := range dnsNodes {
			expected := expectedByScope[dnsGenerationScopeKey(node)]
			labels := map[string]string{"kind": "dns", "node_id": node.ID, "edge_group_id": node.EdgeGroupID, "zone": node.Zone}
			observability.WriteMetricSample(w, "fugue_robustness_node_generation_drift_seconds", labels, robustnessGenerationDriftSeconds(now, expected, firstNonEmpty(node.DNSBundleVersion, node.ServingGeneration), node.LastHeartbeatAt, node.UpdatedAt))
			observability.WriteMetricSample(w, "fugue_robustness_lkg_serving", labels, boolMetric(robustnessNodeServingLKG(node.CacheStatus, node.DNSBundleVersion, node.ServingGeneration, node.LKGGeneration)))
			observability.WriteMetricSample(w, "fugue_robustness_dns_query_errors", map[string]string{"node_id": node.ID, "edge_group_id": node.EdgeGroupID, "zone": node.Zone}, float64(node.QueryErrorCount))
		}
	}
	if nodeHealth, err := s.store.ListNodeDeepHealthResults(); err == nil {
		observability.WriteMetricHeader(w, "fugue_node_deep_health_pass", "Whether the latest node deep health report passed.", "gauge")
		observability.WriteMetricHeader(w, "fugue_node_quarantine_active", "Whether a node is currently quarantined by deep health.", "gauge")
		observability.WriteMetricHeader(w, "fugue_node_managed_iptables_stale_rule_count", "Suspicious stale Fugue managed iptables rules reported by node deep health.", "gauge")
		for _, result := range nodeHealth {
			labels := map[string]string{
				"node_updater_id": result.NodeUpdaterID,
				"cluster_node":    result.ClusterNodeName,
				"runtime_id":      result.RuntimeID,
				"reason":          result.QuarantineReason,
			}
			observability.WriteMetricSample(w, "fugue_node_deep_health_pass", labels, boolMetric(result.OverallStatus == model.NodeDeepHealthStatusPass))
			observability.WriteMetricSample(w, "fugue_node_quarantine_active", labels, boolMetric(nodeQuarantineActive(result, now)))
			staleRules := 0.0
			for _, check := range result.Checks {
				if check.Name != model.NodeDeepHealthCheckManagedIptablesStale {
					continue
				}
				if value := strings.TrimSpace(check.Evidence["suspect_rules"]); value != "" {
					if parsed, err := strconv.ParseFloat(value, 64); err == nil {
						staleRules = parsed
					}
				}
			}
			observability.WriteMetricSample(w, "fugue_node_managed_iptables_stale_rule_count", labels, staleRules)
		}
	}

	if policies, err := s.store.ListBackupPolicies(store.BackupPolicyFilter{IncludeDisabled: true, PlatformAdmin: true, Limit: 500}); err == nil {
		observability.WriteMetricHeader(w, "fugue_robustness_backup_last_success_age_seconds", "Age of the last successful backup per policy.", "gauge")
		for _, policy := range policies {
			age := -1.0
			if policy.LastSuccessfulAt != nil && !policy.LastSuccessfulAt.IsZero() {
				age = now.Sub(policy.LastSuccessfulAt.UTC()).Seconds()
			}
			observability.WriteMetricSample(w, "fugue_robustness_backup_last_success_age_seconds", map[string]string{
				"policy_id":   policy.ID,
				"status":      policy.Status,
				"scope":       policy.Scope,
				"target_type": policy.Target.Type,
			}, age)
		}
	}

	if events, err := s.store.ListAuditEvents("", true, 1000); err == nil {
		repairCounts := map[string]float64{}
		bundleRejections := 0.0
		for _, event := range events {
			action := strings.TrimSpace(event.Action)
			if strings.HasPrefix(action, "robustness.repair.") {
				repairCounts[strings.TrimPrefix(action, "robustness.repair.")]++
			}
			if action == "robustness.bundle_publish.rejected" {
				bundleRejections++
			}
		}
		observability.WriteMetricHeader(w, "fugue_robustness_repair_events_total", "Recent robustness repair audit events by outcome.", "counter")
		for _, outcome := range []string{"dry_run", "blocked", "disabled"} {
			observability.WriteMetricSample(w, "fugue_robustness_repair_events_total", map[string]string{"outcome": outcome}, repairCounts[outcome])
		}
		observability.WriteGaugeMetric(w, "fugue_robustness_bundle_publish_rejections_recent", "Recent structured bundle publish rejection audit events.", nil, bundleRejections)
	}
	if artifacts, err := s.store.ListPlatformArtifacts(model.PlatformArtifactFilter{Limit: 500}); err == nil {
		seen := map[string]bool{}
		drift := 0.0
		lkgExpired := 0.0
		for _, artifact := range artifacts {
			key := artifact.ArtifactKind + "\x00" + artifact.ScopeKey
			if seen[key] {
				continue
			}
			seen[key] = true
			consumers, err := s.store.ListPlatformConsumers(artifact.ArtifactKind, artifact.ScopeKey)
			if err != nil {
				continue
			}
			for _, consumer := range consumers {
				if consumer.DesiredGeneration != "" && consumer.ActualGeneration != "" && consumer.DesiredGeneration != consumer.ActualGeneration {
					drift++
				}
				if consumer.LKGExpired {
					lkgExpired++
				}
			}
		}
		observability.WriteGaugeMetric(w, "fugue_platform_consumer_generation_drift_total", "Platform state consumers whose actual generation differs from desired generation.", nil, drift)
		observability.WriteGaugeMetric(w, "fugue_platform_lkg_expired_total", "Platform state consumers reporting expired LKG.", nil, lkgExpired)
		observability.WriteGaugeMetric(w, "fugue_release_guard_block_total", "Current release guard block signal derived from consumer drift and LKG expiry.", nil, boolMetric(drift > 0 || lkgExpired > 0))
	}
}

func robustnessGenerationDriftSeconds(now time.Time, expected, observed string, lastHeartbeat *time.Time, updatedAt time.Time) float64 {
	expected = strings.TrimSpace(expected)
	observed = strings.TrimSpace(observed)
	if expected == "" || observed == "" || expected == observed {
		return 0
	}
	since := updatedAt.UTC()
	if lastHeartbeat != nil && !lastHeartbeat.IsZero() {
		since = lastHeartbeat.UTC()
	}
	if since.IsZero() || since.After(now) {
		return 0
	}
	return now.Sub(since).Seconds()
}

func robustnessNodeServingLKG(cacheStatus, bundleGeneration, servingGeneration, lkgGeneration string) bool {
	cacheStatus = strings.ToLower(strings.TrimSpace(cacheStatus))
	if strings.Contains(cacheStatus, "lkg") || strings.Contains(cacheStatus, "degraded") || strings.Contains(cacheStatus, "stale") {
		return true
	}
	bundleGeneration = strings.TrimSpace(bundleGeneration)
	servingGeneration = strings.TrimSpace(servingGeneration)
	lkgGeneration = strings.TrimSpace(lkgGeneration)
	return servingGeneration != "" && lkgGeneration != "" && servingGeneration == lkgGeneration && bundleGeneration != "" && servingGeneration != bundleGeneration
}
