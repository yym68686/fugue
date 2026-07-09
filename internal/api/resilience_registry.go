package api

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func resilienceInvariantRegistry() []model.ResilienceInvariant {
	return []model.ResilienceInvariant{
		{ID: "node.pod_dns_to_kube_dns_service", Category: "node", Description: "workload pods can resolve through the kube-dns Service IP", HardGate: true, EvidenceSource: "node_deep_health", RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "node.pod_dns_to_coredns_pod", Category: "node", Description: "workload pods can resolve through a CoreDNS pod IP for service-vip comparison", HardGate: true, EvidenceSource: "node_deep_health", RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "node.kubernetes_default_dns", Category: "node", Description: "workload pods can resolve kubernetes.default.svc", HardGate: true, EvidenceSource: "node_deep_health", RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "node.same_namespace_service_dns_tcp", Category: "node", Description: "workload pods can resolve and connect to a same-namespace service", HardGate: true, EvidenceSource: "node_deep_health", RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "node.managed_iptables_provenance", Category: "node", Description: "Fugue managed iptables rules are identifiable and stale rules are detected before repair", HardGate: true, EvidenceSource: "node_deep_health", RunbookRef: "docs/runbooks/stale-iptables-managed-rule.md"},
		{ID: "node.podcidr_matches_kubernetes", Category: "node", Description: "actual route table still contains the Kubernetes Node PodCIDR", HardGate: true, EvidenceSource: "node_deep_health", RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "node.conntrack_safe", Category: "node", Description: "conntrack saturation stays below the hard fail threshold", HardGate: true, EvidenceSource: "node_deep_health", RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "runtime.no_quarantined_node_placement", Category: "tenant_workload", Description: "new runtime placement excludes quarantined nodes when tenant workload failover is enabled", HardGate: false, EvidenceSource: "runtime_continuity", RunbookRef: "docs/runbooks/stateless-runtime-migration.md"},
		{ID: "runtime.stateless_replacement_ready_before_route", Category: "tenant_workload", Description: "stateless replacement plan must become ready before tenant workload route cutover", HardGate: false, EvidenceSource: "runtime_continuity", RunbookRef: "docs/runbooks/stateless-runtime-migration.md"},
		{ID: "runtime.stateful_fence_before_failover", Category: "tenant_workload", Description: "stateful tenant workload failover requires lease, fence, backup, and restore evidence", HardGate: false, EvidenceSource: "runtime_continuity", RunbookRef: "docs/runbooks/stateful-app-preflight.md"},
		{ID: "edge.eligible_set_hard_gates", Category: "edge", Description: "DNS answers only contain online, healthy, route-ready, TLS-ready, non-excluded, non-draining, non-quarantined edges", HardGate: true, EvidenceSource: "traffic_safety", RunbookRef: "docs/runbooks/edge-quarantine.md"},
		{ID: "edge.service_min_healthy_edge_count", Category: "edge", Description: "service-level exclusion or rollout must not leave a hostname with zero healthy eligible edges", HardGate: true, EvidenceSource: "traffic_safety", RunbookRef: "docs/runbooks/traffic-safety-zero-eligible-edge.md"},
		{ID: "dns.answer_policy_generation_visible", Category: "dns", Description: "DNS answers include ranking version, scope, route generation, and selected edge group evidence", HardGate: false, EvidenceSource: "dns_answer_policy", RunbookRef: "docs/runbooks/request-attribution.md"},
		{ID: "release.no_block_rollout_incident", Category: "release", Description: "normal releases are blocked while block-rollout incidents exist", HardGate: true, EvidenceSource: "release_guard", RunbookRef: "docs/runbooks/release-guard-blocked.md"},
		{ID: "release.artifact_shadow_validated", Category: "release", Description: "edge, DNS, route, and Caddy state changes must have validated shadow artifacts before expansion", HardGate: true, EvidenceSource: "platform_state_release_system", RunbookRef: "docs/runbooks/platform-artifact-release.md"},
		{ID: "request.error_class_attributed", Category: "request", Description: "503 and 5xx requests are attributed to auth, quota, business, edge, origin, DNS, connect, TLS, timeout, or origin-unavailable classes", HardGate: false, EvidenceSource: "request_explain", RunbookRef: "docs/runbooks/request-attribution.md"},
		{ID: "platform_state.consumer_generation_visible", Category: "release", Description: "data-plane consumers report desired, actual, LKG, apply, and probe generation state", HardGate: true, EvidenceSource: "platform_consumer_heartbeat", RunbookRef: "docs/runbooks/consumer-generation-drift.md"},
		{ID: "platform_state.lkg_hash_and_expiry_checked", Category: "release", Description: "LKG snapshots are content-addressed, hash-checked, atomically written, and expire into degraded state", HardGate: true, EvidenceSource: "platform_lkg", RunbookRef: "docs/runbooks/lkg-expired.md"},
	}
}

func resilienceDashboards() []model.ResilienceDashboard {
	return []model.ResilienceDashboard{
		{Name: "node deep health", Scope: "node", Metrics: []string{"fugue_node_deep_health_pass", "fugue_node_pod_dns_lookup_success", "fugue_node_managed_iptables_stale_rule_count", "fugue_node_quarantine_active"}, Command: "fugue admin node-updater health ls", Description: "node DNS, CNI, iptables, PodCIDR, conntrack, and updater generation health"},
		{Name: "traffic safety", Scope: "hostname", Metrics: []string{"fugue_service_eligible_edge_count", "fugue_service_healthy_edge_count", "fugue_service_edge_exclusion_active", "fugue_dns_answer_rejected_total"}, Command: "fugue admin traffic-safety explain <hostname>", Description: "eligible edge set, hard-gated edges, service exclusions, exploration state, and route generation readiness"},
		{Name: "release guard", Scope: "release", Metrics: []string{"fugue_release_guard_block_total", "fugue_release_artifact_validation_failure_total", "fugue_platform_consumer_generation_drift_seconds", "fugue_platform_artifact_rollback_total"}, Command: "fugue admin release guard status", Description: "pre/post rollout robustness baseline, platform artifact validation, consumer drift, and rollback readiness"},
		{Name: "request attribution", Scope: "request", Metrics: []string{"fugue_request_error_class_total", "fugue_request_origin_dns_error_total", "fugue_request_origin_connect_error_total", "fugue_request_body_read_slow_total", "fugue_request_upstream_unavailable_total"}, Command: "fugue admin request explain <request-id>", Description: "request-level edge, origin, DNS, runtime, body-read, TCP, cache, and route generation evidence"},
	}
}

func resilienceAlertRules() []model.ResilienceAlertRule {
	return []model.ResilienceAlertRule{
		{Name: "node.pod_dns_failed", Severity: model.RobustnessSeverityBlockPublish, Expression: "fugue_node_pod_dns_lookup_success == 0", IncidentClass: "node_dns_failure", ExplainCommand: "fugue admin robustness check node <node-name>", RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{Name: "service.healthy_edge_count_zero", Severity: model.RobustnessSeverityBlockPublish, Expression: "fugue_service_healthy_edge_count == 0", IncidentClass: "traffic_safety_zero_eligible_edge", ExplainCommand: "fugue admin traffic-safety explain <hostname>", RunbookRef: "docs/runbooks/traffic-safety-zero-eligible-edge.md"},
		{Name: "edge.origin_dns_error_spike", Severity: model.RobustnessSeverityDegraded, Expression: "increase(fugue_request_origin_dns_error_total[5m]) > 0", IncidentClass: "edge_origin_dns_error_spike", ExplainCommand: "fugue admin request explain <request-id>", RunbookRef: "docs/runbooks/request-attribution.md"},
		{Name: "edge.body_read_slow_spike", Severity: model.RobustnessSeverityDegraded, Expression: "increase(fugue_request_body_read_slow_total[5m]) > 0", IncidentClass: "edge_body_read_slow_spike", ExplainCommand: "fugue admin edge quality-rank <hostname> --request-size-class body_1m_16m", RunbookRef: "docs/runbooks/request-attribution.md"},
		{Name: "release.guard_blocked", Severity: model.RobustnessSeverityBlockPublish, Expression: "fugue_release_guard_block_total > 0", IncidentClass: "release_guard_blocked", ExplainCommand: "fugue admin release guard status", RunbookRef: "docs/runbooks/release-guard-blocked.md"},
		{Name: "request.upstream_unavailable_spike", Severity: model.RobustnessSeverityDegraded, Expression: "increase(fugue_request_upstream_unavailable_total[5m]) > 0", IncidentClass: "upstream_unavailable_spike", ExplainCommand: "fugue admin request explain <request-id>", RunbookRef: "docs/runbooks/request-attribution.md"},
	}
}

func resilienceChaosDrills() []model.ResilienceChaosDrill {
	return []model.ResilienceChaosDrill{
		{ID: "stale-iptables-dnat", FailureMode: "Fugue managed DNAT points at a stale PodCIDR", Detection: "managed_iptables_stale_rule deep health hard fail", Quarantine: "node quarantine and edge eligible-set removal", RepairOrRollback: "repair-managed-iptables dry-run, then provenance-scoped deletion and deep-health reprobe", ExplainCommand: "fugue admin node-updater health show <node-updater-id>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/stale-iptables-managed-rule.md"},
		{ID: "pod-dns-timeout", FailureMode: "pod DNS to kube-dns Service times out", Detection: "pod_dns_to_kube_dns_service hard fail", Quarantine: "node quarantine and runtime scheduler hard gate", RepairOrRollback: "refresh desired state or repair managed iptables, then reprobe", ExplainCommand: "fugue admin robustness check node <node-name>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "coredns-pod-loss", FailureMode: "CoreDNS pod unavailable or bypass probe fails", Detection: "pod_dns_to_coredns_pod hard fail", Quarantine: "node degraded/quarantined when service DNS also fails", RepairOrRollback: "serve LKG and hold rollout until DNS probe passes", ExplainCommand: "fugue admin robustness status --json", ReleaseReadiness: true, RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "service-dns-failure", FailureMode: "same namespace service DNS/TCP unavailable", Detection: "same_namespace_service_dns or same_namespace_service_tcp hard fail", Quarantine: "node/runtime placement gate", RepairOrRollback: "stateless replacement plan or stateful preflight only", ExplainCommand: "fugue admin robustness check service <hostname>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/stateless-runtime-migration.md"},
		{ID: "edge-origin-connect-timeout", FailureMode: "edge cannot connect to origin", Detection: "request attribution origin_connect/timeout class", Quarantine: "hostname-edge hard gate in traffic safety", RepairOrRollback: "route bundle LKG or scoped edge exclusion", ExplainCommand: "fugue admin request explain <request-id>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/edge-quarantine.md"},
		{ID: "edge-slow-body-read", FailureMode: "client-to-edge request body read is slow", Detection: "body read bps, min-window bps, max read gap, TCP retrans/RTO", Quarantine: "scoped quality score demotes edge inside eligible set", RepairOrRollback: "pause exploration and fall back to better scoped edge", ExplainCommand: "fugue admin edge quality-rank <hostname> --request-size-class body_1m_16m", ReleaseReadiness: true, RunbookRef: "docs/runbooks/request-attribution.md"},
		{ID: "bad-route-bundle", FailureMode: "generated route bundle is empty or invalid", Detection: "route bundle invariant validation", Quarantine: "fail closed and keep LKG route bundle", RepairOrRollback: "rollback platform artifact release", ExplainCommand: "fugue admin artifact validate <artifact-id> --dry-run", ReleaseReadiness: true, RunbookRef: "docs/runbooks/platform-artifact-rollback.md"},
		{ID: "service-edge-exclusion-zero", FailureMode: "service edge exclusion leaves zero eligible edge", Detection: "traffic safety min healthy edge count", Quarantine: "reject release or exclusion expansion", RepairOrRollback: "remove exclusion or bring another edge healthy", ExplainCommand: "fugue admin traffic-safety explain <hostname>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/traffic-safety-zero-eligible-edge.md"},
		{ID: "control-plane-api-pod-kill", FailureMode: "control-plane API pod is killed or crashloops", Detection: "external watchdog API probe and Kubernetes readiness fail", Quarantine: "hold control-plane rollout and keep data-plane consumers on validated LKG", RepairOrRollback: "Kubernetes restarts healthy replica or deploy workflow rolls back bad image", ExplainCommand: "fugue admin release guard status", ReleaseReadiness: true, RunbookRef: "docs/runbooks/control-plane-outage-watchdog.md"},
		{ID: "single-control-plane-vm-powerdown", FailureMode: "single control-plane VM is powered down by provider or hypervisor", Detection: "external watchdog API/Kubernetes/runner probes fail while edge/DNS probes may still pass", Quarantine: "data plane serves validated LKG; watchdog opens evidence-only incident", RepairOrRollback: "provider power action only with explicit configured target and recorded action id", ExplainCommand: "fugue admin robustness status --json", ReleaseReadiness: true, RunbookRef: "docs/runbooks/control-plane-outage-watchdog.md"},
		{ID: "all-control-plane-unavailable-edge-alive", FailureMode: "all control-plane endpoints are unavailable while public edge/DNS remains reachable", Detection: "watchdog control-plane probes fail and edge/DNS probes pass", Quarantine: "edge/DNS keep validated LKG and only TTL-bound temporary filters are allowed", RepairOrRollback: "restore control plane, then replay local WAL and reconcile consumer generations", ExplainCommand: "fugue admin artifact consumers <artifact-id>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/data-plane-lkg-autonomy.md"},
		{ID: "edge-caddy-bad-config", FailureMode: "new Caddy route config fails to load", Detection: "edge apply/probe failure before LKG promotion", Quarantine: "reject new route bundle and keep previous LKG", RepairOrRollback: "reload LKG Caddy config and fix generated artifact before release expansion", ExplainCommand: "fugue admin request explain <request-id>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/edge-quarantine.md"},
		{ID: "edge-worker-crash", FailureMode: "edge worker exits or stops reporting heartbeat", Detection: "edge heartbeat, public probe, and DNS answer audit detect unavailable edge", Quarantine: "DNS answer-time filtering removes non-serving edge without changing authoritative policy", RepairOrRollback: "restart stateless edge component with cooldown or keep edge self-quarantined", ExplainCommand: "fugue admin edge nodes", ReleaseReadiness: true, RunbookRef: "docs/runbooks/automatic-repair-safety.md"},
		{ID: "control-plane-bad-image-rollout", FailureMode: "new control-plane image fails readiness", Detection: "canary/API readiness/DB readiness/smoke/post-deploy robustness", Quarantine: "stop rollout and rollback Helm revision", RepairOrRollback: "helm rollback through deploy workflow", ExplainCommand: "fugue admin release guard status", ReleaseReadiness: true, RunbookRef: "docs/runbooks/release-guard-blocked.md"},
		{ID: "cluster-ip-connect-failure", FailureMode: "edge resolves service DNS but cannot connect to ClusterIP", Detection: "edge request sample origin_cluster_ip_connect timing and upstream unavailable class", Quarantine: "traffic safety hard-gates affected hostname-edge path", RepairOrRollback: "repair node DNS/CNI or use endpoint fallback when stateless policy permits", ExplainCommand: "fugue admin request explain <request-id>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/request-attribution.md"},
		{ID: "endpoint-fallback", FailureMode: "service DNS or ClusterIP path fails but verified endpoint LKG can serve stateless HTTP route", Detection: "origin DNS/ClusterIP failure plus valid endpoint LKG identity and TTL", Quarantine: "short TTL endpoint fallback only for safe routes and WAL record for every hit", RepairOrRollback: "expire fallback after TTL and reconcile endpoints when control plane returns", ExplainCommand: "fugue admin request explain <request-id>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/data-plane-lkg-autonomy.md"},
		{ID: "k3s-agent-local-api-timeout", FailureMode: "node local k3s agent API or remotedialer becomes unavailable", Detection: "node guardian local apiserver/remotedialer hard check", Quarantine: "node enters suspect/quarantine and stops receiving new workload placement", RepairOrRollback: "guarded k3s-agent restart only after preflight and cooldown", ExplainCommand: "fugue admin node-updater health show <node-updater-id>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "dns-stale-answer", FailureMode: "DNS would answer a stale or locally unhealthy edge", Detection: "DNS answer audit filtered_edge_ids/filter_reasons and stale edge public probe", Quarantine: "answer-time filtering removes stale edge for a bounded TTL", RepairOrRollback: "resync DNS bundle or keep serving previous validated DNS LKG", ExplainCommand: "fugue admin robustness status --json", ReleaseReadiness: true, RunbookRef: "docs/runbooks/data-plane-lkg-autonomy.md"},
		{ID: "peer-false-positive", FailureMode: "peer health overlay marks a healthy edge suspect", Detection: "peer evidence hash/signature and local probe disagree", Quarantine: "single-peer suspect only lowers weight and expires automatically", RepairOrRollback: "discard expired peer signal and require multi-failure-domain evidence for temporary filter", ExplainCommand: "fugue admin robustness status --json", ReleaseReadiness: true, RunbookRef: "docs/runbooks/edge-quarantine.md"},
		{ID: "lkg-expired", FailureMode: "edge or DNS LKG expires before a newer validated generation is available", Detection: "consumer heartbeat lkg_expired and robustness incident", Quarantine: "fail closed or degraded serving according to component failure contract", RepairOrRollback: "re-pull validated artifact or rollback to a fresh LKG", ExplainCommand: "fugue admin artifact consumers <artifact-id>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/lkg-expired.md"},
		{ID: "lkg-corruption", FailureMode: "local LKG file hash or schema validation fails", Detection: "LKG content hash/schema verification rejects current file", Quarantine: "try previous validated LKG or fail closed", RepairOrRollback: "replace corrupted local cache from control plane artifact after validation", ExplainCommand: "fugue admin robustness status --json", ReleaseReadiness: true, RunbookRef: "docs/runbooks/lkg-expired.md"},
		{ID: "provider-power-event-attribution", FailureMode: "VM power event cause is ambiguous", Detection: "guest logs and provider activity log disagree or provider evidence is missing", Quarantine: "classify as unknown_power_loss until provider evidence is imported", RepairOrRollback: "attach provider action id and timestamps before closing incident", ExplainCommand: "fugue admin robustness incidents ls", ReleaseReadiness: true, RunbookRef: "docs/runbooks/provider-power-event-attribution.md"},
		{ID: "node-loss", FailureMode: "node heartbeat disappears or node becomes unhealthy", Detection: "node updater heartbeat and node deep health staleness", Quarantine: "scheduler/edge hard gate and runtime continuity replacement plan", RepairOrRollback: "stateless replacement plan; stateful preflight only", ExplainCommand: "fugue admin robustness check node <node-name>", ReleaseReadiness: true, RunbookRef: "docs/runbooks/stateless-runtime-migration.md"},
	}
}

func resilienceRunbooks() []model.RunbookReference {
	return []model.RunbookReference{
		{Name: "node DNS failure", Path: "docs/runbooks/node-dns-failure.md", IncidentClass: "node_dns_failure"},
		{Name: "stale managed iptables rule", Path: "docs/runbooks/stale-iptables-managed-rule.md", IncidentClass: "iptables_hard_fail"},
		{Name: "edge quarantine", Path: "docs/runbooks/edge-quarantine.md", IncidentClass: "edge_quarantine"},
		{Name: "traffic safety zero eligible edge", Path: "docs/runbooks/traffic-safety-zero-eligible-edge.md", IncidentClass: "traffic_safety_zero_eligible_edge"},
		{Name: "release guard blocked", Path: "docs/runbooks/release-guard-blocked.md", IncidentClass: "release_guard_blocked"},
		{Name: "request attribution", Path: "docs/runbooks/request-attribution.md", IncidentClass: "request_error_attributed"},
		{Name: "stateless runtime migration", Path: "docs/runbooks/stateless-runtime-migration.md", IncidentClass: "runtime_continuity"},
		{Name: "stateful app preflight", Path: "docs/runbooks/stateful-app-preflight.md", IncidentClass: "stateful_preflight"},
		{Name: "platform artifact release", Path: "docs/runbooks/platform-artifact-release.md", IncidentClass: "platform_artifact_release"},
		{Name: "platform artifact rollback", Path: "docs/runbooks/platform-artifact-rollback.md", IncidentClass: "platform_artifact_rollback"},
		{Name: "consumer generation drift", Path: "docs/runbooks/consumer-generation-drift.md", IncidentClass: "platform_consumer_generation_drift"},
		{Name: "LKG expired", Path: "docs/runbooks/lkg-expired.md", IncidentClass: "platform_lkg_expired"},
		{Name: "control-plane outage watchdog", Path: "docs/runbooks/control-plane-outage-watchdog.md", IncidentClass: "control_plane_outage"},
		{Name: "data-plane LKG autonomy", Path: "docs/runbooks/data-plane-lkg-autonomy.md", IncidentClass: "data_plane_lkg_autonomy"},
		{Name: "provider power event attribution", Path: "docs/runbooks/provider-power-event-attribution.md", IncidentClass: "provider_power_event"},
		{Name: "subsystem failure contract", Path: "docs/runbooks/subsystem-failure-contract.md", IncidentClass: "subsystem_failure_contract"},
		{Name: "automatic repair safety", Path: "docs/runbooks/automatic-repair-safety.md", IncidentClass: "automatic_repair"},
		{Name: "emergency disable switch", Path: "docs/runbooks/emergency-disable-switch.md", IncidentClass: "emergency_disable"},
	}
}

type platformStateConsumerContract struct {
	Component     string
	ArtifactKinds []string
	Scope         string
}

func platformStateConsumerContracts() []platformStateConsumerContract {
	return []platformStateConsumerContract{
		{Component: "node-updater", ArtifactKinds: []string{model.PlatformArtifactKindNodeDesiredState, model.PlatformArtifactKindDiscoveryBundle, model.PlatformArtifactKindNodeGuardianPolicy}, Scope: "node"},
		{Component: "edge-worker", ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindEdgeRankingPolicy, model.PlatformArtifactKindTrafficSafetyPolicy}, Scope: "edge-group/hostname"},
		{Component: "dns-server", ArtifactKinds: []string{model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindEdgeRankingPolicy, model.PlatformArtifactKindTrafficSafetyPolicy}, Scope: "zone/edge-group/hostname"},
		{Component: "caddy-edge-front", ArtifactKinds: []string{model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindEdgeRouteBundle}, Scope: "edge-node"},
		{Component: "runtime-agent", ArtifactKinds: []string{model.PlatformArtifactKindRuntimePlacementPlan, model.PlatformArtifactKindRuntimeContinuityPlan, model.PlatformArtifactKindNodeDesiredState}, Scope: "runtime/node"},
	}
}

func (s *Server) buildResilienceInventory(rSubject string) []model.ResilienceInventoryItem {
	now := time.Now().UTC()
	out := []model.ResilienceInventoryItem{
		{Category: "documentation", Subject: "docs/self-healing-robustness-plan.md", Status: "audited", Summary: "baseline robustness document is now superseded by self-organization inventory and checklist", UpdatedAt: now},
		{Category: "request_attribution", Subject: "edge_performance_sample", Status: "partial", Summary: "request id, hostname, route generation, edge, body read, origin phases, TCP, cache, and error class fields are captured; runtime pod/node propagation remains a gap", UpdatedAt: now},
		{Category: "release_pipeline", Subject: "scripts/upgrade_fugue_control_plane.sh", Status: "guarded", Summary: "pre-deploy robustness baseline, smoke probe, post-deploy robustness gate, and Helm rollback path are present", UpdatedAt: now},
	}
	for _, contract := range platformStateConsumerContracts() {
		out = append(out, model.ResilienceInventoryItem{
			Category: "platform_state_consumer",
			Subject:  contract.Component,
			Status:   "contracted",
			Summary:  fmt.Sprintf("scope=%s artifact_kinds=%s", contract.Scope, strings.Join(contract.ArtifactKinds, ",")),
			Evidence: map[string]string{
				"long_poll":      "required",
				"periodic_pull":  "required",
				"load_lkg_first": "required",
				"atomic_apply":   "required",
				"local_probe":    "required",
			},
			UpdatedAt: now,
		})
	}
	if updaters, err := s.store.ListNodeUpdaters("", true); err == nil {
		out = append(out, model.ResilienceInventoryItem{
			Category:  "node_actual_state",
			Subject:   "node_updaters",
			Status:    "visible",
			Summary:   fmt.Sprintf("node_updaters=%d", len(updaters)),
			Evidence:  map[string]string{"source": "store.ListNodeUpdaters"},
			UpdatedAt: now,
		})
	}
	if health, err := s.store.ListNodeDeepHealthResults(); err == nil {
		quarantined := 0
		for _, result := range health {
			if nodeQuarantineActive(result, now) {
				quarantined++
			}
		}
		out = append(out, model.ResilienceInventoryItem{
			Category:  "node_actual_state",
			Subject:   "node_deep_health",
			Status:    "visible",
			Summary:   fmt.Sprintf("reports=%d active_quarantine=%d", len(health), quarantined),
			Evidence:  map[string]string{"source": "store.ListNodeDeepHealthResults"},
			UpdatedAt: now,
		})
	}
	if edgeNodes, _, err := s.store.ListEdgeNodes(""); err == nil {
		out = append(out, model.ResilienceInventoryItem{
			Category:  "service_route",
			Subject:   "edge_nodes",
			Status:    "visible",
			Summary:   fmt.Sprintf("edge_nodes=%d", len(edgeNodes)),
			Evidence:  map[string]string{"source": "store.ListEdgeNodes"},
			UpdatedAt: now,
		})
	}
	if artifacts, err := s.store.ListPlatformArtifacts(model.PlatformArtifactFilter{Limit: 500}); err == nil {
		kinds := platformArtifactKinds(artifacts)
		out = append(out, model.ResilienceInventoryItem{
			Category:  "release_pipeline",
			Subject:   "platform_artifacts",
			Status:    "visible",
			Summary:   fmt.Sprintf("artifacts=%d kinds=%s", len(artifacts), strings.Join(kinds, ",")),
			Evidence:  map[string]string{"source": "store.ListPlatformArtifacts"},
			UpdatedAt: now,
		})
	}
	if strings.TrimSpace(rSubject) != "" {
		out = append(out, model.ResilienceInventoryItem{
			Category:  "service_route",
			Subject:   normalizeExternalAppDomain(rSubject),
			Status:    "scoped",
			Summary:   "scoped route inventory is attached in route_explain",
			UpdatedAt: now,
		})
	}
	return out
}

func resilienceGapReport() []model.ResilienceGap {
	return []model.ResilienceGap{
		{ID: "request.runtime_node_propagation", Category: "request", Severity: model.RobustnessSeverityWarning, Description: "edge samples record edge and route generation, but runtime pod/node propagation is only available when downstream services forward it", ImplementationPath: "edge-front/origin proxy headers and runtime middleware"},
		{ID: "stateful.automatic_failover_boundary", Category: "runtime", Severity: model.RobustnessSeverityInfo, Description: "stateful app failover intentionally stops at lease/fence/backup/restore preflight without automatic destructive migration", ImplementationPath: "stateful failover policy and operator approval"},
		{ID: "repair.non_dry_run_guard", Category: "repair", Severity: model.RobustnessSeverityInfo, Description: "new repair actions are rate-limited, idempotent, provenance-scoped, and dry-run first; broad automatic repair remains blocked unless safety class allows it", ImplementationPath: "node update task execution and robustness repair endpoint"},
	}
}

func (s *Server) buildRuntimeContinuityStatuses() ([]model.RuntimeContinuityStatus, error) {
	apps, err := s.store.ListApps("", true)
	if err != nil {
		return nil, err
	}
	updaters, err := s.store.ListNodeUpdaters("", true)
	if err != nil {
		return nil, err
	}
	health, err := s.store.ListNodeDeepHealthResults()
	if err != nil {
		return nil, err
	}
	updaterByRuntime := map[string]model.NodeUpdater{}
	for _, updater := range updaters {
		if runtimeID := strings.TrimSpace(updater.RuntimeID); runtimeID != "" {
			updaterByRuntime[runtimeID] = updater
		}
	}
	quarantineByRuntime := map[string]model.NodeDeepHealthResult{}
	for _, result := range health {
		if !nodeQuarantineActive(result, time.Now().UTC()) {
			continue
		}
		if runtimeID := strings.TrimSpace(result.RuntimeID); runtimeID != "" {
			quarantineByRuntime[runtimeID] = result
		}
	}
	statuses := make([]model.RuntimeContinuityStatus, 0, len(apps))
	for _, app := range apps {
		runtimeID := firstNonEmpty(strings.TrimSpace(app.Status.CurrentRuntimeID), strings.TrimSpace(app.Spec.RuntimeID))
		desired := app.Spec.Replicas
		ready := app.Status.CurrentReplicas
		stateless := appContinuityStateless(app)
		status := model.RuntimeContinuityStatus{
			AppID:           app.ID,
			AppName:         app.Name,
			State:           "healthy",
			Strategy:        "stateless_replacement",
			DesiredReplicas: desired,
			ReadyReplicas:   ready,
			RuntimeID:       runtimeID,
			Evidence: map[string]string{
				"phase": app.Status.Phase,
			},
		}
		if app.Route != nil {
			status.Hostname = normalizeExternalAppDomain(app.Route.Hostname)
		}
		if updater, ok := updaterByRuntime[runtimeID]; ok {
			status.RuntimeNode = updater.ClusterNodeName
		}
		if !stateless {
			status.Strategy = "stateful_preflight_only"
			status.StatefulPreflight = []string{"lease present", "fence evidence present", "fresh backup available", "restore plan available"}
		}
		if desired > 0 && ready < desired {
			status.Blockers = append(status.Blockers, fmt.Sprintf("ready replicas %d below desired %d", ready, desired))
		}
		if quarantine, ok := quarantineByRuntime[runtimeID]; ok {
			status.NodeQuarantine = quarantine.QuarantineReason
			status.Blockers = append(status.Blockers, "runtime node quarantined: "+firstNonEmpty(quarantine.QuarantineReason, quarantine.QuarantineState))
			status.Attribution = append(status.Attribution, "node_quarantine")
		}
		if len(status.Blockers) > 0 {
			if stateless {
				status.State = "degraded"
				status.ReplacementPlan = "create replacement pod on non-quarantined runtime node; only switch route after replacement readiness and service DNS/TCP probes pass"
			} else {
				status.State = "blocked"
				status.ReplacementPlan = "stateful app requires lease/fence/backup/restore preflight before failover"
			}
		}
		if stateless {
			status.Attribution = append(status.Attribution, "stateless")
		} else {
			status.Attribution = append(status.Attribution, "stateful")
		}
		statuses = append(statuses, status)
	}
	sort.SliceStable(statuses, func(i, j int) bool {
		if statuses[i].State != statuses[j].State {
			return statuses[i].State < statuses[j].State
		}
		return statuses[i].AppName < statuses[j].AppName
	})
	return statuses, nil
}

func appContinuityStateless(app model.App) bool {
	if app.Spec.Postgres != nil || app.Spec.Workspace != nil || app.Spec.PersistentStorage != nil {
		return false
	}
	if len(app.Bindings) > 0 || len(app.BackingServices) > 0 {
		return false
	}
	return true
}

func nodeQuarantineActive(result model.NodeDeepHealthResult, now time.Time) bool {
	if result.ObservedOnly {
		return false
	}
	if strings.TrimSpace(result.QuarantineState) == "" || result.QuarantineState == model.NodeQuarantineStateClear {
		return false
	}
	if result.QuarantineExpiresAt != nil && !result.QuarantineExpiresAt.IsZero() && now.After(result.QuarantineExpiresAt.UTC()) {
		return false
	}
	return true
}

func (s *Server) activeNodeQuarantineByName() map[string]model.NodeDeepHealthResult {
	out := map[string]model.NodeDeepHealthResult{}
	if s == nil || s.store == nil {
		return out
	}
	results, err := s.store.ListNodeDeepHealthResults()
	if err != nil {
		return out
	}
	now := time.Now().UTC()
	for _, result := range results {
		if !nodeQuarantineActive(result, now) {
			continue
		}
		for _, key := range []string{result.NodeUpdaterID, result.ClusterNodeName, result.RuntimeID, result.MachineID} {
			key = strings.TrimSpace(key)
			if key != "" {
				out[key] = result
			}
		}
	}
	return out
}

func (s *Server) robustnessNodeDeepHealthChecks() ([]model.RobustnessCheck, error) {
	results, err := s.store.ListNodeDeepHealthResults()
	if err != nil {
		return nil, err
	}
	checks := []model.RobustnessCheck{{
		Name:     "node_deep_health_inventory",
		Pass:     true,
		Severity: model.RobustnessSeverityInfo,
		Subject:  "node_deep_health",
		Expected: "node guardians report observe-only deep health",
		Observed: fmt.Sprintf("reports=%d", len(results)),
		Evidence: map[string]string{"guardian": "node-health", "observed_only": "true"},
	}}
	now := time.Now().UTC()
	for _, result := range results {
		subject := "node:" + firstNonEmpty(result.ClusterNodeName, result.NodeUpdaterID)
		activeQuarantine := nodeQuarantineActive(result, now)
		severity := model.RobustnessSeverityInfo
		if activeQuarantine {
			severity = model.RobustnessSeverityBlockPublish
		} else if result.QuarantineState == model.NodeQuarantineStateDegraded || result.OverallStatus == model.NodeDeepHealthStatusWarning {
			severity = model.RobustnessSeverityWarning
		}
		checks = append(checks, model.RobustnessCheck{
			Name:       "node_deep_health_quarantine",
			Pass:       !activeQuarantine,
			Severity:   severity,
			Subject:    subject,
			Expected:   "quarantine_state=clear",
			Observed:   fmt.Sprintf("overall=%s quarantine=%s reason=%s", result.OverallStatus, result.QuarantineState, result.QuarantineReason),
			Message:    strings.Join(result.RecoveryConditions, "; "),
			Evidence:   map[string]string{"guardian": "node-health", "node_updater_id": result.NodeUpdaterID, "runtime_id": result.RuntimeID, "observed_only": fmt.Sprintf("%t", result.ObservedOnly)},
			RepairHint: "keep the node out of new scheduling and edge DNS answers until a fresh deep-health report clears all hard-fail checks",
		})
		for _, deep := range result.Checks {
			if deep.Status != model.NodeDeepHealthStatusFail {
				continue
			}
			checks = append(checks, model.RobustnessCheck{
				Name:       "node_deep_health_" + deep.Name,
				Pass:       false,
				Severity:   robustnessSeverityForNodeDeepHealthCheck(deep, result.ObservedOnly),
				Subject:    subject,
				Expected:   deep.Expected,
				Observed:   deep.Observed,
				Message:    deep.Message,
				Evidence:   deep.Evidence,
				RepairHint: firstNonEmpty(deep.RepairAction, "run fugue admin node-updater health show and choose a dry-run repair plan before any repair execution"),
			})
		}
	}
	return checks, nil
}

func (s *Server) robustnessControlPlaneTopologyChecks(principal model.Principal) ([]model.RobustnessCheck, error) {
	updaters, err := s.store.ListNodeUpdaters(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return nil, err
	}
	topology := buildControlPlaneTopologyFromNodeUpdaters(updaters, time.Now().UTC())
	check := controlPlaneTopologyCheck(topology)
	if check.Evidence == nil {
		check.Evidence = map[string]string{}
	}
	check.Evidence["source"] = "node_updater_heartbeat"
	return []model.RobustnessCheck{check}, nil
}

func robustnessSeverityForNodeDeepHealthCheck(check model.NodeDeepHealthCheck, observedOnly bool) string {
	if observedOnly {
		return model.RobustnessSeverityWarning
	}
	if check.HardFail {
		return model.RobustnessSeverityBlockPublish
	}
	return model.RobustnessSeverityWarning
}

func robustnessChecksFromRuntimeContinuity(statuses []model.RuntimeContinuityStatus) []model.RobustnessCheck {
	checks := []model.RobustnessCheck{{
		Name:     "app_continuity_inventory",
		Pass:     true,
		Severity: model.RobustnessSeverityInfo,
		Subject:  "apps",
		Expected: "app continuity invariant is evaluated",
		Observed: fmt.Sprintf("apps=%d", len(statuses)),
		Evidence: map[string]string{"guardian": "runtime-continuity"},
	}}
	for _, status := range statuses {
		pass := len(status.Blockers) == 0
		severity := model.RobustnessSeverityInfo
		if !pass {
			severity = model.RobustnessSeverityDegraded
		}
		evidence := cloneStringMap(status.Evidence)
		if evidence == nil {
			evidence = map[string]string{}
		}
		evidence["guardian"] = "runtime-continuity"
		evidence["release_gate_scope"] = "tenant_workload"
		evidence["report_only"] = "true"
		evidence["app_id"] = strings.TrimSpace(status.AppID)
		evidence["app_name"] = strings.TrimSpace(status.AppName)
		evidence["hostname"] = normalizeExternalAppDomain(status.Hostname)
		checks = append(checks, model.RobustnessCheck{
			Name:       "app_continuity_invariant",
			Pass:       pass,
			Severity:   severity,
			Subject:    "app:" + firstNonEmpty(status.AppName, status.AppID),
			Expected:   "ready replicas satisfy desired replicas and runtime node is not quarantined",
			Observed:   fmt.Sprintf("state=%s strategy=%s ready=%d desired=%d runtime=%s node=%s", status.State, status.Strategy, status.ReadyReplicas, status.DesiredReplicas, status.RuntimeID, status.RuntimeNode),
			Message:    strings.Join(status.Blockers, "; "),
			Evidence:   evidence,
			RepairHint: firstNonEmpty(status.ReplacementPlan, "tenant workload continuity is report-only unless the tenant has enabled managed failover"),
		})
	}
	return checks
}

func (s *Server) robustnessPlatformConsumerChecks() ([]model.RobustnessCheck, error) {
	artifacts, err := s.store.ListPlatformArtifacts(model.PlatformArtifactFilter{Limit: 500})
	if err != nil {
		return nil, err
	}
	checks := []model.RobustnessCheck{}
	seenScope := map[string]bool{}
	drift := 0
	lkgExpired := 0
	for _, artifact := range artifacts {
		scopeKey := artifact.ArtifactKind + "\x00" + artifact.ScopeKey
		if seenScope[scopeKey] {
			continue
		}
		seenScope[scopeKey] = true
		consumers, err := s.store.ListPlatformConsumers(artifact.ArtifactKind, artifact.ScopeKey)
		if err != nil {
			return nil, err
		}
		for _, consumer := range consumers {
			if consumer.DesiredGeneration != "" && consumer.ActualGeneration != "" && consumer.DesiredGeneration != consumer.ActualGeneration {
				drift++
				checks = append(checks, model.RobustnessCheck{
					Name:       "platform_consumer_generation_drift",
					Pass:       false,
					Severity:   model.RobustnessSeverityBlockPublish,
					Subject:    "consumer:" + consumer.ConsumerID,
					Expected:   "actual_generation equals desired_generation",
					Observed:   fmt.Sprintf("kind=%s scope=%s desired=%s actual=%s lkg=%s", consumer.ArtifactKind, consumer.ScopeKey, consumer.DesiredGeneration, consumer.ActualGeneration, consumer.LKGGeneration),
					Message:    consumer.LastError,
					Evidence:   map[string]string{"component": consumer.Component, "node_id": consumer.NodeID, "apply_status": consumer.ApplyStatus, "probe_status": consumer.ProbeStatus},
					RepairHint: "consumer must periodic-pull the active generation, apply atomically, probe locally, and report convergence before release expansion",
				})
			}
			if consumer.LKGExpired {
				lkgExpired++
				checks = append(checks, model.RobustnessCheck{
					Name:       "platform_consumer_lkg_expired",
					Pass:       false,
					Severity:   model.RobustnessSeverityBlockPublish,
					Subject:    "consumer:" + consumer.ConsumerID,
					Expected:   "LKG is fresh and hash-verified before serving",
					Observed:   fmt.Sprintf("kind=%s scope=%s lkg=%s serving_lkg=%t", consumer.ArtifactKind, consumer.ScopeKey, consumer.LKGGeneration, consumer.ServingLKG),
					Message:    consumer.LastError,
					Evidence:   map[string]string{"component": consumer.Component, "node_id": consumer.NodeID},
					RepairHint: "fail closed or re-pull a validated artifact; do not silently serve expired LKG",
				})
			}
		}
	}
	checks = append(checks, model.RobustnessCheck{
		Name:       "platform_consumer_generation_inventory",
		Pass:       drift == 0 && lkgExpired == 0,
		Severity:   model.RobustnessSeverityBlockPublish,
		Subject:    "platform_consumers",
		Expected:   "no generation drift and no expired LKG",
		Observed:   fmt.Sprintf("drift=%d lkg_expired=%d", drift, lkgExpired),
		Evidence:   map[string]string{"guardian": "platform-state-release"},
		RepairHint: "inspect fugue admin artifact consumers and force consumers to periodic-pull before rollout expansion",
	})
	return checks, nil
}

func (s *Server) robustnessPlatformConsumers() ([]model.PlatformConsumerInstance, error) {
	artifacts, err := s.store.ListPlatformArtifacts(model.PlatformArtifactFilter{Limit: 500})
	if err != nil {
		return nil, err
	}
	seenScope := map[string]bool{}
	seenConsumer := map[string]bool{}
	consumers := []model.PlatformConsumerInstance{}
	for _, artifact := range artifacts {
		scopeKey := artifact.ArtifactKind + "\x00" + artifact.ScopeKey
		if seenScope[scopeKey] {
			continue
		}
		seenScope[scopeKey] = true
		scopeConsumers, err := s.store.ListPlatformConsumers(artifact.ArtifactKind, artifact.ScopeKey)
		if err != nil {
			return nil, err
		}
		for _, consumer := range scopeConsumers {
			key := strings.Join([]string{
				consumer.ConsumerID,
				consumer.ArtifactKind,
				consumer.ScopeKey,
				consumer.NodeID,
			}, "\x00")
			if seenConsumer[key] {
				continue
			}
			seenConsumer[key] = true
			consumers = append(consumers, consumer)
		}
	}
	sort.SliceStable(consumers, func(i, j int) bool {
		left := strings.Join([]string{consumers[i].Component, consumers[i].NodeID, consumers[i].ArtifactKind, consumers[i].ScopeKey, consumers[i].ConsumerID}, "\x00")
		right := strings.Join([]string{consumers[j].Component, consumers[j].NodeID, consumers[j].ArtifactKind, consumers[j].ScopeKey, consumers[j].ConsumerID}, "\x00")
		return left < right
	})
	return consumers, nil
}
