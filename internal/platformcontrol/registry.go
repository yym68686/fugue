package platformcontrol

import (
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

const (
	ActionContractNodeQuarantine        = "node.quarantine"
	ActionContractEdgeQuarantine        = "edge.quarantine"
	ActionContractDNSAnswerFilter       = "dns.answer_filter"
	ActionContractEdgeRouteFilter       = "edge.route_filter"
	ActionContractNodeRepairTaskClaim   = "node.repair_task_claim"
	ActionContractLKGReload             = "lkg.reload"
	ActionContractEndpointFallback      = "endpoint.fallback"
	ActionContractReleaseRollback       = "release.rollback"
	ActionContractStatelessRestart      = "component.stateless_restart"
	ActionContractManagedIptablesRepair = "node.managed_iptables_repair"
	ActionContractFreezeNodeGeneration  = "node_updater.freeze_generation"
)

func InvariantDefinitions() []model.InvariantDefinition {
	definitions := []model.InvariantDefinition{
		invariant(
			"release.no_new_block_publish",
			"release",
			model.GatePolicyScopeCluster,
			"release",
			"new releases must not introduce or quantitatively expand block_publish incidents",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"release_guard",
			"release_guard.block_rollout",
			"",
			"docs/runbooks/release-guard-blocked.md",
			"5m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"release.public_synthetic_hard_rollback",
			"release",
			model.GatePolicyScopeCluster,
			"public-data-plane",
			"public synthetic probes must not enter a hard rollback class after a release",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"public_synthetic",
			"public.synthetic_503",
			ActionContractReleaseRollback,
			"docs/runbooks/bad-control-plane-release-rollback.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"platform_state.consumer_generation_visible",
			"consumer",
			model.GatePolicyScopeCluster,
			"required-platform-consumers",
			"required consumers report desired, actual, LKG, apply, and probe generation evidence",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"platform_consumer_heartbeat",
			"release_guard.block_rollout",
			"",
			"docs/runbooks/consumer-generation-drift.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"platform_state.expected_consumer_present",
			"consumer",
			model.GatePolicyScopeCluster,
			"expected-consumer-set",
			"every required expected consumer must have a fresh authenticated heartbeat",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"expected_consumer_set,platform_consumer_heartbeat",
			"release_guard.block_rollout",
			"",
			"docs/runbooks/consumer-generation-drift.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"platform_state.consumer_identity_trusted",
			"consumer",
			model.GatePolicyScopeCluster,
			"platform-component-identity",
			"consumer component, node, and scope identity must be derived from trusted credentials",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"platform_consumer_identity",
			"release_guard.block_rollout",
			"",
			"docs/runbooks/consumer-generation-drift.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			true,
		),
		invariant(
			"edge.eligible_set_hard_gates",
			"edge",
			model.GatePolicyScopeHostname,
			"edge-eligible-set",
			"eligible edges are online, route-ready, TLS-ready, non-draining, and non-quarantined",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"traffic_safety,edge_heartbeat",
			"edge.route_inventory_quarantine",
			ActionContractEdgeQuarantine,
			"docs/runbooks/edge-quarantine.md",
			"90s",
			model.InvariantEvidenceBehaviorPreserveLKG,
			model.InvariantEvidenceBehaviorPreserveLKG,
			false,
		),
		invariant(
			"edge.service_min_healthy_edge_count",
			"edge",
			model.GatePolicyScopeHostname,
			"hostname",
			"edge filtering and exclusions must preserve at least one eligible edge unless fail-closed is explicit",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"traffic_safety,dns_answer_audit",
			"dns.answer_route_ready",
			ActionContractEdgeRouteFilter,
			"docs/runbooks/traffic-safety-zero-eligible-edge.md",
			"90s",
			model.InvariantEvidenceBehaviorPreserveLKG,
			model.InvariantEvidenceBehaviorPreserveLKG,
			true,
		),
		invariant(
			"edge.route_bundle_nonempty",
			"edge",
			model.GatePolicyScopeEdgeGroup,
			"edge-route-bundle",
			"published edge route bundles must be non-empty for scopes with active routes",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"platform_artifact_validation",
			"edge.route_inventory_quarantine",
			"",
			"docs/runbooks/platform-artifact-rollback.md",
			"10m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			true,
		),
		invariant(
			"edge.caddy_preserves_previous_config",
			"edge",
			model.GatePolicyScopeEdgeNode,
			"caddy-edge-front",
			"a failed Caddy reload must keep the previous verified configuration serving",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"caddy_apply_probe,platform_consumer_heartbeat",
			"edge.route_inventory_quarantine",
			ActionContractLKGReload,
			"docs/runbooks/data-plane-lkg-autonomy.md",
			"2m",
			model.InvariantEvidenceBehaviorPreserveLKG,
			model.InvariantEvidenceBehaviorPreserveLKG,
			false,
		),
		invariant(
			"dns.answer_route_ready",
			"dns",
			model.GatePolicyScopeHostname,
			"dns-answer",
			"authoritative DNS answers must not include a known non-route-ready edge",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"dns_answer_audit,edge_heartbeat",
			"dns.answer_route_ready",
			ActionContractDNSAnswerFilter,
			"docs/runbooks/edge-route-all-unhealthy.md",
			"90s",
			model.InvariantEvidenceBehaviorPreserveLKG,
			model.InvariantEvidenceBehaviorPreserveLKG,
			false,
		),
		invariant(
			"dns.answer_decision_audited",
			"dns",
			model.GatePolicyScopeHostname,
			"dns-answer-audit",
			"DNS answer decisions record selected and filtered edges with generation and reason",
			model.RobustnessSeverityWarning,
			model.GatePolicyModeShadow,
			"dns_answer_audit",
			"dns.answer_route_ready",
			"",
			"docs/runbooks/request-attribution.md",
			"5m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"node.deep_health_shadow_first",
			"node",
			model.GatePolicyScopeNode,
			"node-deep-health",
			"new node deep-health checks cannot directly enter fleet-wide enforcement",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"gate_policy_registry,node_deep_health",
			"node.kubernetes_service_dns",
			"",
			"docs/runbooks/node-updater-canary-restore.md",
			"10m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			true,
		),
		invariant(
			"node.pod_dns_to_kube_dns_service",
			"node",
			model.GatePolicyScopeNode,
			"pod-network-dns",
			"workload pods can resolve through the kube-dns Service IP",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"node_deep_health",
			"node.kubernetes_service_dns",
			"",
			"docs/runbooks/node-dns-failure.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"node.pod_dns_to_coredns_pod",
			"node",
			model.GatePolicyScopeNode,
			"pod-network-dns",
			"workload pods can resolve through a CoreDNS pod IP for service-vip comparison",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"node_deep_health",
			"node.kubernetes_service_dns",
			"",
			"docs/runbooks/node-dns-failure.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"node.kubernetes_default_dns",
			"node",
			model.GatePolicyScopeNode,
			"pod-network-dns",
			"workload pods can resolve kubernetes.default.svc",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"node_deep_health",
			"node.kubernetes_service_dns",
			"",
			"docs/runbooks/node-dns-failure.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"node.same_namespace_service_dns_tcp",
			"node",
			model.GatePolicyScopeNode,
			"pod-network-service",
			"workload pods can resolve and connect to a same-namespace service",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"node_deep_health",
			"node.kubernetes_service_dns",
			"",
			"docs/runbooks/node-dns-failure.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"node.managed_iptables_provenance",
			"node",
			model.GatePolicyScopeNode,
			"managed-iptables",
			"Fugue-managed iptables rules are attributable and stale rules are detected before repair",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"node_deep_health",
			"node.kube_proxy_rules",
			"",
			"docs/runbooks/stale-iptables-managed-rule.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"node.podcidr_matches_kubernetes",
			"node",
			model.GatePolicyScopeNode,
			"podcidr-routing",
			"the node route table still contains the Kubernetes Node PodCIDR",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"node_deep_health",
			"node.cni_bridge",
			"",
			"docs/runbooks/node-dns-failure.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"node.conntrack_safe",
			"node",
			model.GatePolicyScopeNode,
			"conntrack",
			"conntrack saturation stays below the hard-fail threshold",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"node_deep_health",
			"node.conntrack_saturation",
			"",
			"docs/runbooks/node-dns-failure.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"node.repair_safety_contract",
			"node",
			model.GatePolicyScopeNode,
			"node-guardian",
			"node repair requires cooldown, attempt budget, WAL, audit, and a registered action contract",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"node_guardian_wal,node_repair_audit",
			"node.guardian_repair",
			ActionContractNodeRepairTaskClaim,
			"docs/runbooks/automatic-repair-safety.md",
			"10m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			true,
		),
		invariant(
			"node.quarantine_blast_radius",
			"node",
			model.GatePolicyScopeNode,
			"node-quarantine",
			"node quarantine has TTL, recovery conditions, scoped evidence, and a one-node blast-radius cap",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeShadow,
			"node_deep_health,action_audit",
			"scheduler.node_quarantine",
			ActionContractNodeQuarantine,
			"docs/runbooks/quarantine-blast-radius-exceeded.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			true,
		),
		invariant(
			"runtime.no_quarantined_node_placement",
			"tenant_workload",
			model.GatePolicyScopeRuntime,
			"runtime-placement",
			"new runtime placement excludes quarantined nodes when tenant workload failover is enabled",
			model.RobustnessSeverityWarning,
			model.GatePolicyModeShadow,
			"runtime_continuity,node_deep_health",
			"scheduler.node_quarantine",
			"",
			"docs/runbooks/stateless-runtime-migration.md",
			"2m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"runtime.stateless_replacement_ready_before_route",
			"tenant_workload",
			model.GatePolicyScopeRuntime,
			"runtime-continuity",
			"a stateless replacement plan becomes ready before tenant workload route cutover",
			model.RobustnessSeverityWarning,
			model.GatePolicyModeShadow,
			"runtime_continuity",
			"",
			"",
			"docs/runbooks/stateless-runtime-migration.md",
			"5m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"runtime.stateful_fence_before_failover",
			"tenant_workload",
			model.GatePolicyScopeRuntime,
			"stateful-runtime-continuity",
			"stateful tenant workload failover requires lease, fence, backup, and restore evidence",
			model.RobustnessSeverityWarning,
			model.GatePolicyModeShadow,
			"runtime_continuity,backup_preflight",
			"",
			"",
			"docs/runbooks/stateful-app-preflight.md",
			"5m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"platform_api.readiness",
			"platform_api",
			model.GatePolicyScopeCluster,
			"api",
			"control-plane API and database readiness remain independently observable",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"healthz,readyz,database_readiness",
			"public.synthetic_503",
			ActionContractReleaseRollback,
			"docs/runbooks/bad-control-plane-release-rollback.md",
			"1m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"platform_api.error_attribution",
			"platform_api",
			model.GatePolicyScopeCluster,
			"api-errors",
			"platform API 5xx responses are attributable to a concrete platform failure plane",
			model.RobustnessSeverityWarning,
			model.GatePolicyModeShadow,
			"request_explain,operation_diagnosis",
			"public.synthetic_503",
			"",
			"docs/runbooks/request-attribution.md",
			"15m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"dns.answer_policy_generation_visible",
			"dns",
			model.GatePolicyScopeHostname,
			"dns-answer-audit",
			"DNS answer evidence includes ranking version, scope, route generation, and selected edge group",
			model.RobustnessSeverityWarning,
			model.GatePolicyModeShadow,
			"dns_answer_audit",
			"dns.answer_route_ready",
			"",
			"docs/runbooks/request-attribution.md",
			"5m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"release.no_block_rollout_incident",
			"release",
			model.GatePolicyScopeCluster,
			"release",
			"normal releases are blocked while block-rollout incidents exist",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"release_guard",
			"release_guard.block_rollout",
			"",
			"docs/runbooks/release-guard-blocked.md",
			"5m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"release.artifact_shadow_validated",
			"release",
			model.GatePolicyScopeCluster,
			"platform-artifact-release",
			"edge, DNS, route, and Caddy state changes have validated shadow artifacts before expansion",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"platform_artifact_validation",
			"release_guard.block_rollout",
			"",
			"docs/runbooks/platform-artifact-release.md",
			"10m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"request.error_class_attributed",
			"request",
			model.GatePolicyScopeService,
			"request-attribution",
			"5xx requests are attributed to auth, quota, business, edge, origin, DNS, connect, TLS, timeout, or origin-unavailable classes",
			model.RobustnessSeverityWarning,
			model.GatePolicyModeShadow,
			"request_explain",
			"",
			"",
			"docs/runbooks/request-attribution.md",
			"15m",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			false,
		),
		invariant(
			"platform_state.lkg_hash_and_expiry_checked",
			"release",
			model.GatePolicyScopeCluster,
			"verified-lkg",
			"LKG snapshots are content-addressed, hash-checked, atomically written, and expire into degraded state",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"platform_lkg",
			"release_guard.block_rollout",
			"",
			"docs/runbooks/lkg-expired.md",
			"5m",
			model.InvariantEvidenceBehaviorFailClosed,
			model.InvariantEvidenceBehaviorFailClosed,
			false,
		),
		invariant(
			"release.single_active_lane",
			"release",
			model.GatePolicyScopeCluster,
			"release-lane",
			"one release lane cannot have two active releases or coordinators",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"release_lane_store",
			"release_guard.block_rollout",
			ActionContractReleaseRollback,
			"docs/runbooks/platform-artifact-release.md",
			"30s",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			true,
		),
		invariant(
			"release.stale_coordinator_rejected",
			"release",
			model.GatePolicyScopeCluster,
			"release-coordinator",
			"stale fencing tokens and stale coordinators cannot overwrite newer release state",
			model.RobustnessSeverityBlockPublish,
			model.GatePolicyModeEnforced,
			"release_lane_store",
			"release_guard.block_rollout",
			"",
			"docs/runbooks/platform-artifact-release.md",
			"30s",
			model.InvariantEvidenceBehaviorHold,
			model.InvariantEvidenceBehaviorHold,
			true,
		),
	}

	safety := map[string]model.InvariantDefinition{}
	for _, definition := range platformSafetyInvariantDefinitions() {
		safety[definition.ID] = definition
	}
	for _, invariantID := range platformsafety.ImmutableInvariantIDs() {
		if definition, ok := safety[invariantID]; ok {
			definitions = append(definitions, definition)
		}
	}
	sort.SliceStable(definitions, func(i, j int) bool { return definitions[i].ID < definitions[j].ID })
	return definitions
}

func InvariantDefinitionByID(id string) (model.InvariantDefinition, bool) {
	id = strings.TrimSpace(id)
	for _, definition := range InvariantDefinitions() {
		if definition.ID == id {
			return definition, true
		}
	}
	return model.InvariantDefinition{}, false
}

func AutomaticActionContracts() []model.AutomaticActionContract {
	contracts := []model.AutomaticActionContract{
		actionContract(ActionContractNodeQuarantine, "quarantine_node", model.GatePolicyScopeNode, "node.quarantine_blast_radius", "scheduler.node_quarantine", "FUGUE_AUTONOMY_QUARANTINE_ENABLED", "FUGUE_AUTONOMY_NODE_QUARANTINE_KILL_SWITCH", "15m", 3, 1, model.GateBlastRadiusPolicy{MaxNodes: 1}, "clear quarantine after consecutive healthy reports", "clear node quarantine", "docs/runbooks/quarantine-blast-radius-exceeded.md", true, true, true, false),
		actionContract(ActionContractEdgeQuarantine, "quarantine_edge", model.GatePolicyScopeEdgeNode, "edge.eligible_set_hard_gates", "edge.route_inventory_quarantine", "FUGUE_AUTONOMY_QUARANTINE_ENABLED", "FUGUE_AUTONOMY_EDGE_QUARANTINE_KILL_SWITCH", "15m", 3, 2, model.GateBlastRadiusPolicy{MaxEdgesPerGroup: 1, PreserveMinHealthyEdgeGroups: 1}, "restore edge after local and public probes pass", "remove temporary edge quarantine", "docs/runbooks/edge-quarantine.md", true, true, true, false),
		actionContract(ActionContractDNSAnswerFilter, "dns_answer_filter", model.GatePolicyScopeHostname, "dns.answer_route_ready", "dns.answer_route_ready", "FUGUE_AUTONOMY_DNS_FILTERING_ENABLED", "FUGUE_AUTONOMY_DNS_FILTERING_KILL_SWITCH", "5m", 3, 2, model.GateBlastRadiusPolicy{PreserveMinEligibleEdgesPerHost: 1}, "restore previous verified answer after recovery threshold", "expire filter and restore previous verified answer", "docs/runbooks/edge-route-all-unhealthy.md", true, true, true, false),
		actionContract(ActionContractEdgeRouteFilter, "edge_route_filter", model.GatePolicyScopeHostname, "edge.service_min_healthy_edge_count", "edge.route_inventory_quarantine", "FUGUE_AUTONOMY_QUARANTINE_ENABLED", "FUGUE_AUTONOMY_EDGE_ROUTE_FILTER_KILL_SWITCH", "5m", 3, 2, model.GateBlastRadiusPolicy{MaxEdgesPerGroup: 1, PreserveMinHealthyEdgeGroups: 1, PreserveMinEligibleEdgesPerHost: 1}, "restore route eligibility after convergence and probes pass", "restore prior verified route eligibility", "docs/runbooks/traffic-safety-zero-eligible-edge.md", true, true, true, false),
		actionContract(ActionContractNodeRepairTaskClaim, "node_repair_task_claim", model.GatePolicyScopeNode, "node.repair_safety_contract", "node.guardian_repair", "FUGUE_AUTONOMY_REPAIR_ENABLED", "FUGUE_AUTONOMY_NODE_REPAIR_KILL_SWITCH", "10m", 3, 1, model.GateBlastRadiusPolicy{MaxNodes: 1}, "repair verification passes and cooldown completes", "reconcile desired state or stop the task", "docs/runbooks/automatic-repair-safety.md", true, true, true, true),
		actionContract(ActionContractLKGReload, "reload_lkg_bundle", model.GatePolicyScopeEdgeNode, platformsafety.InvariantLKGContentIntegrity, "platform.lkg_reload", "FUGUE_AUTONOMY_REPAIR_ENABLED", "FUGUE_AUTONOMY_LKG_RELOAD_KILL_SWITCH", "10m", 1, 1, model.GateBlastRadiusPolicy{MaxEdgesPerGroup: 1, PreserveMinHealthyEdgeGroups: 1}, "fresh verified generation applies and probes pass", "reload the prior verified LKG", "docs/runbooks/data-plane-lkg-autonomy.md", true, true, true, true),
		actionContract(ActionContractEndpointFallback, "endpoint_fallback", model.GatePolicyScopeHostname, "platform_api.error_attribution", "edge.peer_fallback", "FUGUE_AUTONOMY_ENDPOINT_FALLBACK_ENABLED", "FUGUE_AUTONOMY_ENDPOINT_FALLBACK_KILL_SWITCH", "2m", 2, 1, model.GateBlastRadiusPolicy{PreserveMinEligibleEdgesPerHost: 1}, "service DNS and ClusterIP path recover", "expire endpoint fallback and resume normal routing", "docs/runbooks/data-plane-lkg-autonomy.md", true, true, true, false),
		actionContract(ActionContractReleaseRollback, "release_rollback", model.GatePolicyScopeCluster, "release.public_synthetic_hard_rollback", "release.automatic_rollback", "FUGUE_AUTONOMY_RELEASE_ROLLBACK_ENABLED", "FUGUE_AUTONOMY_RELEASE_ROLLBACK_KILL_SWITCH", "15m", 1, 1, model.GateBlastRadiusPolicy{PreserveMinHealthyEdgeGroups: 1}, "rollback vector converges and public probes pass", "hold and freeze the release lane", "docs/runbooks/bad-control-plane-release-rollback.md", true, false, true, true),
		actionContract(ActionContractStatelessRestart, "restart_stateless_component", model.GatePolicyScopeNode, "node.repair_safety_contract", "node.guardian_repair", "FUGUE_AUTONOMY_REPAIR_ENABLED", "FUGUE_AUTONOMY_STATELESS_RESTART_KILL_SWITCH", "10m", 3, 1, model.GateBlastRadiusPolicy{MaxNodes: 1}, "component readiness and local probes pass", "stop restart loop and serve LKG", "docs/runbooks/automatic-repair-safety.md", true, true, true, true),
		actionContract(ActionContractManagedIptablesRepair, "managed_iptables_repair", model.GatePolicyScopeNode, "node.repair_safety_contract", "node.managed_iptables_repair", "FUGUE_AUTONOMY_REPAIR_ENABLED", "FUGUE_AUTONOMY_MANAGED_IPTABLES_REPAIR_KILL_SWITCH", "10m", 3, 1, model.GateBlastRadiusPolicy{MaxNodes: 1}, "managed rules match current PodCIDR and probes pass", "restore captured managed-rule snapshot", "docs/runbooks/stale-iptables-managed-rule.md", true, true, true, true),
		actionContract(ActionContractFreezeNodeGeneration, "freeze_node_updater_generation", model.GatePolicyScopeNode, "platform_state.consumer_generation_visible", "node_updater.generation_rollout", "FUGUE_AUTONOMY_REPAIR_ENABLED", "FUGUE_AUTONOMY_NODE_GENERATION_FREEZE_KILL_SWITCH", "1h", 1, 1, model.GateBlastRadiusPolicy{MaxNodes: 1}, "desired generation is validated and canary converges", "restore previous desired generation", "docs/runbooks/node-updater-canary-restore.md", true, true, true, true),
	}
	sort.SliceStable(contracts, func(i, j int) bool { return contracts[i].ID < contracts[j].ID })
	return contracts
}

func AutomaticActionContractByID(id string) (model.AutomaticActionContract, bool) {
	id = strings.TrimSpace(id)
	for _, contract := range AutomaticActionContracts() {
		if contract.ID == id {
			return contract, true
		}
	}
	return model.AutomaticActionContract{}, false
}

func ConsumerContracts() []model.PlatformConsumerContractDefinition {
	return []model.PlatformConsumerContractDefinition{
		{Component: "node-updater", ArtifactKinds: []string{model.PlatformArtifactKindNodeDesiredState, model.PlatformArtifactKindDiscoveryBundle, model.PlatformArtifactKindNodeGuardianPolicy}, Scope: "node", IdentityKind: "node-updater-token", ProtocolVersion: "v1", SchemaVersion: "v1", Required: true, LoadLKGFirst: true, AtomicApply: true, LocalProbe: true, HeartbeatGeneration: true, HeartbeatFreshness: "2m", CompatibilityFloor: "v1", ExpectedConsumerSource: "server-side node topology"},
		{Component: "edge-worker", ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindEdgeRankingPolicy, model.PlatformArtifactKindTrafficSafetyPolicy}, Scope: "edge-group/hostname", IdentityKind: "edge-token", ProtocolVersion: "v1", SchemaVersion: "v1", Required: true, LoadLKGFirst: true, AtomicApply: true, LocalProbe: true, HeartbeatGeneration: true, HeartbeatFreshness: "90s", CompatibilityFloor: "v1", ExpectedConsumerSource: "server-side active edge topology"},
		{Component: "dns-server", ArtifactKinds: []string{model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindEdgeRankingPolicy, model.PlatformArtifactKindTrafficSafetyPolicy}, Scope: "zone/edge-group/hostname", IdentityKind: "dns-token", ProtocolVersion: "v1", SchemaVersion: "v1", Required: true, LoadLKGFirst: true, AtomicApply: true, LocalProbe: true, HeartbeatGeneration: true, HeartbeatFreshness: "90s", CompatibilityFloor: "v1", ExpectedConsumerSource: "server-side active DNS topology"},
		{Component: "caddy-edge-front", ArtifactKinds: []string{model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindEdgeRouteBundle}, Scope: "edge-node", IdentityKind: "edge-token", ProtocolVersion: "v1", SchemaVersion: "v1", Required: true, LoadLKGFirst: true, AtomicApply: true, LocalProbe: true, HeartbeatGeneration: true, HeartbeatFreshness: "90s", CompatibilityFloor: "v1", ExpectedConsumerSource: "server-side edge-front topology"},
		{Component: "runtime-agent", ArtifactKinds: []string{model.PlatformArtifactKindRuntimePlacementPlan, model.PlatformArtifactKindRuntimeContinuityPlan, model.PlatformArtifactKindNodeDesiredState}, Scope: "runtime/node", IdentityKind: "agent-token", ProtocolVersion: "v1", SchemaVersion: "v1", Required: true, LoadLKGFirst: true, AtomicApply: true, LocalProbe: true, HeartbeatGeneration: true, HeartbeatFreshness: "2m", CompatibilityFloor: "v1", ExpectedConsumerSource: "server-side runtime topology"},
	}
}

func SyntheticProbes() []model.PlatformSyntheticProbeDefinition {
	return []model.PlatformSyntheticProbeDefinition{
		{ID: "control-plane.healthz", Scope: "control-plane", Description: "public control-plane liveness", HardGate: true, Timeout: "10s"},
		{ID: "control-plane.readyz", Scope: "control-plane", Description: "public control-plane and database readiness", HardGate: true, Timeout: "10s"},
		{ID: "platform.representative-service", Scope: "public-data-plane", Description: "representative platform service request", HardGate: true, Timeout: "15s"},
		{ID: "edge.direct-resolve", Scope: "edge-node", Description: "direct TLS/HTTP probe against every active edge while preserving Host and SNI", HardGate: true, Timeout: "15s"},
		{ID: "dns.authoritative-answer", Scope: "dns-node", Description: "authoritative answer contains an eligible route-ready edge", HardGate: true, Timeout: "5s"},
	}
}

func LKGPolicies() []model.PlatformLKGPolicyDefinition {
	policies := []model.PlatformLKGPolicyDefinition{
		{ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, StorageLocation: "filesystem:/var/lib/fugue/edge/routes-cache.json", CachePathEnv: "FUGUE_EDGE_ROUTES_CACHE_PATH", MaxAge: "24h", MaxStale: "72h", MinimumGenerations: 3, ArchiveLimit: 5, ExpiryBehavior: "degraded_preserve_previous"},
		{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, StorageLocation: "filesystem:/var/lib/fugue/dns/dns-cache.json (per-zone path derived for extra zones)", CachePathEnv: "FUGUE_DNS_CACHE_PATH", MaxAge: "24h", MaxStale: "72h", MinimumGenerations: 3, ArchiveLimit: 5, ExpiryBehavior: "degraded_or_explicit_fail_closed"},
		{ArtifactKind: model.PlatformArtifactKindCaddyRouteConfig, StorageLocation: "derived:edge_route_bundle plus active Caddy config", MaxAge: "24h", MaxStale: "72h", MinimumGenerations: 3, ExpiryBehavior: "degraded_preserve_previous"},
		{ArtifactKind: model.PlatformArtifactKindDiscoveryBundle, StorageLocation: "control-plane store:fugue_platform_lkg_snapshots", MaxAge: "24h", MaxStale: "168h", MinimumGenerations: 3, ExpiryBehavior: "degraded_preserve_previous"},
		{ArtifactKind: model.PlatformArtifactKindNodeDesiredState, StorageLocation: "control-plane store:fugue_platform_lkg_snapshots", MaxAge: "24h", MaxStale: "168h", MinimumGenerations: 3, ExpiryBehavior: "hold_new_actions"},
		{ArtifactKind: model.PlatformArtifactKindRuntimePlacementPlan, StorageLocation: "control-plane store:fugue_platform_lkg_snapshots", MaxAge: "6h", MaxStale: "24h", MinimumGenerations: 3, ExpiryBehavior: "hold_new_placement"},
		{ArtifactKind: model.PlatformArtifactKindRuntimeContinuityPlan, StorageLocation: "control-plane store:fugue_platform_lkg_snapshots", MaxAge: "6h", MaxStale: "24h", MinimumGenerations: 3, ExpiryBehavior: "hold_new_failover"},
		{ArtifactKind: model.PlatformArtifactKindNodeGuardianPolicy, StorageLocation: "control-plane store:fugue_platform_lkg_snapshots", MaxAge: "24h", MaxStale: "168h", MinimumGenerations: 3, ExpiryBehavior: "observe_only"},
		{ArtifactKind: model.PlatformArtifactKindReleaseGuardPolicy, StorageLocation: "control-plane store:fugue_platform_lkg_snapshots", MaxAge: "24h", MaxStale: "168h", MinimumGenerations: 3, ExpiryBehavior: "compiled_defaults"},
		{ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy, StorageLocation: "control-plane store:fugue_platform_lkg_snapshots", MaxAge: "24h", MaxStale: "168h", MinimumGenerations: 3, ExpiryBehavior: "compiled_defaults"},
		{ArtifactKind: model.PlatformArtifactKindTrafficSafetyPolicy, StorageLocation: "control-plane store:fugue_platform_lkg_snapshots", MaxAge: "24h", MaxStale: "168h", MinimumGenerations: 3, ExpiryBehavior: "compiled_defaults"},
		{ArtifactKind: model.PlatformArtifactKindSubsystemFailureContracts, StorageLocation: "control-plane store:fugue_platform_lkg_snapshots", MaxAge: "24h", MaxStale: "168h", MinimumGenerations: 3, ExpiryBehavior: "compiled_defaults"},
		{ArtifactKind: model.PlatformArtifactKindGatePolicyRegistry, StorageLocation: "control-plane store:fugue_platform_lkg_snapshots", MaxAge: "24h", MaxStale: "168h", MinimumGenerations: 3, ExpiryBehavior: "compiled_defaults"},
		{ArtifactKind: model.PlatformArtifactKindAutomaticActionContracts, StorageLocation: "control-plane store:fugue_platform_lkg_snapshots", MaxAge: "24h", MaxStale: "168h", MinimumGenerations: 3, ExpiryBehavior: "compiled_defaults"},
	}
	sort.SliceStable(policies, func(i, j int) bool { return policies[i].ArtifactKind < policies[j].ArtifactKind })
	return policies
}

func DefaultMechanisms() []model.PlatformControlMechanism {
	return []model.PlatformControlMechanism{
		{ID: "platform_safety_kernel", Category: "release", Status: model.PlatformControlMechanismEnforced, Mode: model.GatePolicyModeEnforced, ImplementationRef: "internal/platformsafety", Summary: "compiled non-bypassable artifact, release, LKG, fencing, and override boundaries"},
		{ID: "verified_lkg", Category: "release", Status: model.PlatformControlMechanismEnforced, Mode: model.GatePolicyModeEnforced, ImplementationRef: "internal/store/platform_state*", Summary: "full release remains serving_unverified until evidence verification"},
		{ID: "gate_policy_registry", Category: "policy", Status: model.PlatformControlMechanismEnforced, Mode: model.GatePolicyModeEnforced, ImplementationRef: "internal/api/gate_registry.go", Summary: "configuration can only tighten compiled defaults"},
		{ID: "invariant_registry", Category: "policy", Status: model.PlatformControlMechanismEnforced, Mode: model.GatePolicyModeEnforced, ImplementationRef: "internal/platformcontrol/registry.go", Summary: "single definitions for release guard, robustness, node, edge, DNS, consumer, and artifact invariants"},
		{ID: "action_safety_evaluator", Category: "automatic_action", Status: model.PlatformControlMechanismShadow, Mode: model.GatePolicyModeShadow, ImplementationRef: "internal/platformcontrol/action_safety.go", Summary: "central contract evaluator; production action sites are migrated individually"},
		{ID: "node_deep_health", Category: "node", Status: model.PlatformControlMechanismShadow, Mode: model.GatePolicyModeShadow, ImplementationRef: "internal/store/node_deep_health.go", Summary: "reports are observed-only until explicit canary promotion"},
		{ID: "release_guard", Category: "release", Status: model.PlatformControlMechanismEnforced, Mode: model.GatePolicyModeEnforced, ImplementationRef: "internal/api/resilience_explain.go", Summary: "hard release gate over robustness, artifact validity, consumer drift, and gate policy validity"},
		{ID: "service_diagnosis", Category: "diagnosis", Status: model.PlatformControlMechanismDesigned, ImplementationRef: "docs/fugue-platform-resilience-control-loop-plan.md", Summary: "existing diagnosis surfaces are present; unified server-side service diagnosis is pending"},
	}
}

func platformSafetyInvariantDefinitions() []model.InvariantDefinition {
	const runbook = "docs/runbooks/platform-safety-kernel.md"
	return []model.InvariantDefinition{
		safetyInvariant(platformsafety.InvariantArtifactValidated, "artifact", "artifact", "artifact validation status is required unless a bounded override permits it", false, runbook),
		safetyInvariant(platformsafety.InvariantArtifactSchema, "artifact", "artifact", "artifact schema and positive generation sequence are valid", true, runbook),
		safetyInvariant(platformsafety.InvariantArtifactContentHash, "artifact", "artifact", "canonical artifact content matches the content hash", true, runbook),
		safetyInvariant(platformsafety.InvariantArtifactSignature, "artifact", "artifact", "artifact provenance signature is trusted and valid", true, runbook),
		safetyInvariant(platformsafety.InvariantGenerationMonotonic, "release", "release-lane", "ordinary publication moves generation sequence forward", false, runbook),
		safetyInvariant(platformsafety.InvariantShadowNoProductionImpact, "release", "release-lane", "shadow publication never changes production serving state", true, runbook),
		safetyInvariant(platformsafety.InvariantCanaryScopeIsolation, "release", "release-lane", "canary publication cannot escape its bounded scope", true, runbook),
		safetyInvariant(platformsafety.InvariantBlastRadiusHardCap, "automatic_action", "action", "compiled blast-radius maximum cannot be relaxed by configuration", true, runbook),
		safetyInvariant(platformsafety.InvariantKillSwitchPrecedence, "automatic_action", "action", "kill switches take precedence over configured policy", true, runbook),
		safetyInvariant(platformsafety.InvariantFullPinnedRollback, "release", "release-lane", "full publication has a readable verified pinned rollback target", false, runbook),
		safetyInvariant(platformsafety.InvariantFencingTokenCurrent, "release", "release-lane", "release mutations use the current fencing token", true, runbook),
		safetyInvariant(platformsafety.InvariantVerificationEvidencePassed, "lkg", "verified-lkg", "verified LKG promotion requires complete verification evidence", true, runbook),
		safetyInvariant(platformsafety.InvariantLKGNotExpired, "lkg", "verified-lkg", "expired LKG is not reported healthy", true, runbook),
		safetyInvariant(platformsafety.InvariantLKGContentIntegrity, "lkg", "verified-lkg", "LKG content remains hash-valid and matches its artifact", true, runbook),
		safetyInvariant(platformsafety.InvariantLKGSignature, "lkg", "verified-lkg", "LKG signature is trusted and not revoked", true, runbook),
	}
}

func invariant(
	id, category, scope, subject, description, severity, defaultMode, evidenceSources,
	gatePolicyID, actionContractID, runbookRef, maxAge, unknownBehavior, staleBehavior string,
	nonBypassable bool,
) model.InvariantDefinition {
	sources := splitCSV(evidenceSources)
	evidenceSource := ""
	if len(sources) > 0 {
		evidenceSource = sources[0]
	}
	return model.InvariantDefinition{
		ID:                        id,
		Category:                  category,
		Scope:                     scope,
		Subject:                   subject,
		Owner:                     "platform",
		Description:               description,
		Severity:                  severity,
		DefaultMode:               defaultMode,
		HardGate:                  severity == model.RobustnessSeverityBlockPublish,
		EvidenceSource:            evidenceSource,
		EvidenceSources:           sources,
		GatePolicyID:              gatePolicyID,
		AutomaticActionContractID: actionContractID,
		RollbackSignal:            firstString("release_guard_block_rollout", severity == model.RobustnessSeverityBlockPublish),
		RunbookRef:                runbookRef,
		EvidenceFreshnessPolicy: model.InvariantEvidencePolicy{
			MaxAge:                maxAge,
			MinimumSources:        1,
			MinimumFailureDomains: firstInt(2, scope == model.GatePolicyScopeEdgeGroup || scope == model.GatePolicyScopeHostname),
			AllowLKGEvidence:      unknownBehavior == model.InvariantEvidenceBehaviorPreserveLKG,
		},
		UnknownBehavior:        unknownBehavior,
		StaleBehavior:          staleBehavior,
		NonBypassable:          nonBypassable,
		ClockUncertaintyBudget: "5s",
	}
}

func safetyInvariant(id, category, subject, description string, nonBypassable bool, runbookRef string) model.InvariantDefinition {
	definition := invariant(
		id,
		category,
		model.GatePolicyScopeCluster,
		subject,
		description,
		model.RobustnessSeverityBlockPublish,
		model.GatePolicyModeEnforced,
		"platform_safety_kernel",
		"release_guard.block_rollout",
		"",
		runbookRef,
		"0s",
		model.InvariantEvidenceBehaviorFailClosed,
		model.InvariantEvidenceBehaviorFailClosed,
		nonBypassable,
	)
	return definition
}

func actionContract(
	id, actionType, scope, triggerInvariant, gatePolicyID, enableEnv, killSwitchEnv, ttl string,
	minimumSamples, minimumFailureDomains int,
	blastRadius model.GateBlastRadiusPolicy,
	recoveryCondition, rollbackAction, runbookRef string,
	requiresAudit, requiresWAL, requiresIdempotencyKey, requiresFencingToken bool,
) model.AutomaticActionContract {
	return model.AutomaticActionContract{
		ID:                     id,
		ActionType:             actionType,
		Scope:                  scope,
		TriggerInvariant:       triggerInvariant,
		EvidenceSource:         triggerInvariant,
		RequiredEvidence:       []string{triggerInvariant},
		GatePolicyID:           gatePolicyID,
		MaxBlastRadius:         "compiled",
		BlastRadius:            blastRadius,
		TTL:                    ttl,
		MinimumSamples:         minimumSamples,
		MinimumFailureDomains:  minimumFailureDomains,
		SoakMinDuration:        "0s",
		RecoveryCondition:      recoveryCondition,
		RollbackAction:         rollbackAction,
		DryRunOutput:           "action safety decision and would_action audit",
		AuditLogLocation:       "fugue_audit_events",
		EnableEnv:              enableEnv,
		KillSwitchEnv:          killSwitchEnv,
		RunbookRef:             runbookRef,
		AllowedModes:           []string{model.GatePolicyModeShadow, model.GatePolicyModeCanary, model.GatePolicyModeEnforced},
		RequiresRollbackTarget: id == ActionContractReleaseRollback,
		RequiresAudit:          requiresAudit,
		RequiresWAL:            requiresWAL,
		RequiresIdempotencyKey: requiresIdempotencyKey,
		RequiresFencingToken:   requiresFencingToken,
	}
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func firstString(value string, include bool) string {
	if include {
		return value
	}
	return ""
}

func firstInt(value int, include bool) int {
	if include {
		return value
	}
	return 1
}
