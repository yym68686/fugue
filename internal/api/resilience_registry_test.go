package api

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestResilienceRegistriesWireRunbooksAndReleaseReadiness(t *testing.T) {
	t.Parallel()

	runbookPaths := map[string]struct{}{}
	for _, runbook := range resilienceRunbooks() {
		if strings.TrimSpace(runbook.Name) == "" || strings.TrimSpace(runbook.Path) == "" || strings.TrimSpace(runbook.IncidentClass) == "" {
			t.Fatalf("runbook reference must include name/path/incident class: %+v", runbook)
		}
		runbookPaths[runbook.Path] = struct{}{}
		if _, err := os.Stat(filepath.Join("..", "..", filepath.FromSlash(runbook.Path))); err != nil {
			t.Fatalf("runbook path %s must exist: %v", runbook.Path, err)
		}
	}
	for _, invariant := range resilienceInvariantRegistry() {
		if strings.TrimSpace(invariant.ID) == "" || strings.TrimSpace(invariant.Category) == "" || strings.TrimSpace(invariant.Description) == "" {
			t.Fatalf("invariant must include id/category/description: %+v", invariant)
		}
		if invariant.RunbookRef != "" {
			if _, ok := runbookPaths[invariant.RunbookRef]; !ok {
				t.Fatalf("invariant %s references unknown runbook %s", invariant.ID, invariant.RunbookRef)
			}
		}
	}
	for _, alert := range resilienceAlertRules() {
		if strings.TrimSpace(alert.Name) == "" || strings.TrimSpace(alert.Expression) == "" || strings.TrimSpace(alert.IncidentClass) == "" || strings.TrimSpace(alert.ExplainCommand) == "" {
			t.Fatalf("alert must include name/expression/incident/explain: %+v", alert)
		}
		if _, ok := runbookPaths[alert.RunbookRef]; !ok {
			t.Fatalf("alert %s references unknown runbook %s", alert.Name, alert.RunbookRef)
		}
	}
	for _, drill := range resilienceChaosDrills() {
		if strings.TrimSpace(drill.ID) == "" || strings.TrimSpace(drill.Detection) == "" || strings.TrimSpace(drill.Quarantine) == "" || strings.TrimSpace(drill.RepairOrRollback) == "" || strings.TrimSpace(drill.ExplainCommand) == "" {
			t.Fatalf("drill must verify detection/quarantine/repair-or-rollback/explain: %+v", drill)
		}
		if !drill.ReleaseReadiness {
			t.Fatalf("drill %s must feed release readiness", drill.ID)
		}
		if _, ok := runbookPaths[drill.RunbookRef]; !ok {
			t.Fatalf("drill %s references unknown runbook %s", drill.ID, drill.RunbookRef)
		}
	}
}

func TestPlatformStateConsumerContractsCoverDataPlaneConsumers(t *testing.T) {
	t.Parallel()

	contracts := platformStateConsumerContracts()
	if len(contracts) != 5 {
		t.Fatalf("expected five platform-state consumer contracts, got %+v", contracts)
	}
	byComponent := map[string]platformStateConsumerContract{}
	for _, contract := range contracts {
		if strings.TrimSpace(contract.Component) == "" || strings.TrimSpace(contract.Scope) == "" || len(contract.ArtifactKinds) == 0 {
			t.Fatalf("consumer contract must include component/scope/artifact kinds: %+v", contract)
		}
		byComponent[contract.Component] = contract
	}
	for _, component := range []string{"node-updater", "edge-worker", "dns-server", "caddy-edge-front", "runtime-agent"} {
		if _, ok := byComponent[component]; !ok {
			t.Fatalf("missing platform-state consumer contract for %s", component)
		}
	}
}

func TestTrafficSafetyHardGateReasonsCoverRouteExclusionAndNodeHealth(t *testing.T) {
	t.Parallel()

	explain := model.RouteExplainResponse{
		Hostname: "api.fugue.pro",
		HealthyEdgeGroups: map[string]bool{
			"edge-group-drained":    false,
			"edge-group-route":      true,
			"edge-group-exclusion":  true,
			"edge-group-tls":        true,
			"edge-group-generation": true,
		},
		Routes: []model.EdgeRouteBinding{
			{
				Hostname:          "api.fugue.pro",
				SelectedEdgeGroup: "edge-group-route",
				Status:            "building",
				RouteGeneration:   "route_gen_pending",
			},
			{
				Hostname:             "api.fugue.pro",
				SelectedEdgeGroup:    "edge-group-exclusion",
				Status:               "ready",
				ExcludedEdgeIDs:      []string{"edge-bad"},
				ExcludedEdgeGroupIDs: []string{"edge-group-bad"},
				ExclusionReason:      "service-specific bad edge",
				RouteGeneration:      "route_gen_ready",
			},
			{
				Hostname:          "api.fugue.pro",
				SelectedEdgeGroup: "edge-group-tls",
				Status:            "ready",
				TLSPolicy:         "blocked: cert missing",
				RouteGeneration:   "route_gen_tls",
			},
			{
				Hostname:          "api.fugue.pro",
				SelectedEdgeGroup: "edge-group-generation",
				Status:            "ready",
			},
		},
		GeneratedAt: time.Now().UTC(),
	}

	eligible, gated := trafficSafetyEdgeGroups(explain)
	if !stringSliceContains(gated, "edge-group-drained") || !stringSliceContains(gated, "edge-group-route") || !stringSliceContains(gated, "edge-group-tls") {
		t.Fatalf("expected unhealthy/route/tls groups to be gated, eligible=%v gated=%v", eligible, gated)
	}
	reasons := trafficSafetyHardGateReasons(explain)
	for group, want := range map[string]string{
		"edge-group-drained":   "not healthy",
		"edge-group-route":     "route generation is not ready",
		"edge-group-exclusion": "service edge exclusion active",
		"edge-group-tls":       "TLS policy blocks",
	} {
		if !strings.Contains(reasons[group], want) {
			t.Fatalf("expected hard gate reason for %s to contain %q, got %q", group, want, reasons[group])
		}
	}
	blockers := trafficSafetyRouteBlockers(explain)
	if !stringSliceContains(blockers, "route api.fugue.pro status is building") || !stringSliceContains(blockers, "route api.fugue.pro has no route generation") {
		t.Fatalf("expected route generation blockers, got %+v", blockers)
	}
}

func TestRequestExplainSplitsUpstreamUnavailableAndFailureContracts(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		sample     model.EdgePerformanceSample
		wantClass  string
		wantCause  string
		wantSystem string
	}{
		{
			name:       "dns",
			sample:     model.EdgePerformanceSample{StatusCode: http.StatusServiceUnavailable, OriginDNSMS: 12},
			wantClass:  "edge.upstream_unavailable.origin_dns",
			wantCause:  "edge_to_origin_dns",
			wantSystem: "kubernetes_cni_dns",
		},
		{
			name:       "stored-dns-class",
			sample:     model.EdgePerformanceSample{StatusCode: http.StatusServiceUnavailable, OriginFailureClass: "origin_dns_failed"},
			wantClass:  "edge.upstream_unavailable.origin_dns",
			wantCause:  "edge_observed_request",
			wantSystem: "observability_metrics",
		},
		{
			name:       "connect",
			sample:     model.EdgePerformanceSample{StatusCode: http.StatusServiceUnavailable, OriginConnectMS: 50},
			wantClass:  "edge.upstream_unavailable.origin_connect",
			wantCause:  "edge_to_origin_connect",
			wantSystem: "kubernetes_cni_dns",
		},
		{
			name:       "endpoint-connect",
			sample:     model.EdgePerformanceSample{StatusCode: http.StatusServiceUnavailable, OriginEndpointConnectMS: 35},
			wantClass:  "edge.upstream_unavailable.origin_endpoint_connect",
			wantCause:  "edge_to_origin_endpoint_connect",
			wantSystem: "runtime_scheduler",
		},
		{
			name:       "timeout",
			sample:     model.EdgePerformanceSample{StatusCode: http.StatusServiceUnavailable, OriginResponseWaitMS: 5000},
			wantClass:  "edge.upstream_unavailable.timeout",
			wantCause:  "origin_response_wait",
			wantSystem: "runtime_scheduler",
		},
		{
			name:       "origin",
			sample:     model.EdgePerformanceSample{StatusCode: http.StatusServiceUnavailable},
			wantClass:  "edge.upstream_unavailable.origin_unavailable",
			wantCause:  "edge_observed_request",
			wantSystem: "observability_metrics",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.sample.ID = "req_" + tt.name
			tt.sample.SampledAt = time.Now().UTC()
			gotClass := requestErrorClassFromSample(tt.sample)
			attribution := requestAttributionFromSample(tt.sample)
			contracts := requestFailureContractsFromAttribution(attribution)
			plane := requestFailurePlane(gotClass, tt.sample)
			if gotClass != tt.wantClass {
				t.Fatalf("expected class %s, got %s", tt.wantClass, gotClass)
			}
			if plane != "data_plane" {
				t.Fatalf("expected data plane failure for %s, got %s", gotClass, plane)
			}
			if !stringSliceContains(attribution, tt.wantCause) || !stringSliceContains(contracts, tt.wantSystem) {
				t.Fatalf("expected attribution %q and contract %q, got attribution=%v contracts=%v", tt.wantCause, tt.wantSystem, attribution, contracts)
			}
		})
	}
}

func TestRuntimeContinuityReportsTenantWorkloadRisksWithoutPlatformHardGate(t *testing.T) {
	t.Parallel()

	statelessChecks := robustnessChecksFromRuntimeContinuity([]model.RuntimeContinuityStatus{{
		AppID:           "app_stateless",
		AppName:         "stateless",
		State:           "degraded",
		Strategy:        "stateless_replacement",
		DesiredReplicas: 1,
		ReadyReplicas:   0,
		NodeQuarantine:  "pod_dns_failed",
		Blockers:        []string{"runtime node quarantined: pod_dns_failed"},
		ReplacementPlan: "create replacement pod on non-quarantined runtime node",
	}})
	if len(statelessChecks) != 2 || statelessChecks[1].Pass || statelessChecks[1].Severity != model.RobustnessSeverityDegraded {
		t.Fatalf("expected stateless quarantined runtime to be degraded but not a platform hard gate, got %+v", statelessChecks)
	}
	if statelessChecks[1].Evidence["release_gate_scope"] != "tenant_workload" || statelessChecks[1].Evidence["report_only"] != "true" {
		t.Fatalf("expected stateless continuity to be tenant workload report-only evidence, got %+v", statelessChecks[1].Evidence)
	}

	statefulChecks := robustnessChecksFromRuntimeContinuity([]model.RuntimeContinuityStatus{{
		AppID:             "app_stateful",
		AppName:           "stateful",
		State:             "blocked",
		Strategy:          "stateful_preflight_only",
		DesiredReplicas:   1,
		ReadyReplicas:     0,
		Blockers:          []string{"ready replicas 0 below desired 1"},
		StatefulPreflight: []string{"lease present", "fence evidence present", "fresh backup available", "restore plan available"},
		ReplacementPlan:   "stateful app requires lease/fence/backup/restore preflight before failover",
	}})
	if len(statefulChecks) != 2 || statefulChecks[1].Pass || statefulChecks[1].Severity != model.RobustnessSeverityDegraded {
		t.Fatalf("expected stateful failover without readiness to be tenant workload risk, got %+v", statefulChecks)
	}
	if statefulChecks[1].Evidence["release_gate_scope"] != "tenant_workload" || statefulChecks[1].Evidence["report_only"] != "true" {
		t.Fatalf("expected stateful continuity to be tenant workload report-only evidence, got %+v", statefulChecks[1].Evidence)
	}
}

func TestExplicitReleaseSignalPromotesTenantWorkloadContinuityToControlPlaneGate(t *testing.T) {
	t.Parallel()

	checks := robustnessChecksFromRuntimeContinuity([]model.RuntimeContinuityStatus{{
		AppID:           "app_signal",
		AppName:         "signal-app",
		Hostname:        "signal.example.com",
		State:           "degraded",
		Strategy:        "stateless_replacement",
		DesiredReplicas: 1,
		ReadyReplicas:   0,
		Blockers:        []string{"ready replicas 0 below desired 1"},
	}})
	checks = applyReleaseSignalsToRobustnessChecks(checks, []model.ReleaseSignal{{
		ID:         "sig_signal_app",
		Enabled:    true,
		OwnerScope: model.ReleaseSignalOwnerScopeTenantWorkload,
		GateScope:  model.ReleaseSignalGateScopeControlPlane,
		Mode:       model.ReleaseSignalModeHardGate,
		Subject:    "app:app_signal",
		CheckName:  "app_continuity_invariant",
		Reason:     "admin opted this workload into control-plane release success",
	}})

	if len(checks) != 2 || checks[1].Pass || checks[1].Severity != model.RobustnessSeverityBlockPublish {
		t.Fatalf("expected explicit release signal to promote failing workload to block_publish, got %+v", checks)
	}
	evidence := checks[1].Evidence
	if evidence["release_signal_id"] != "sig_signal_app" || evidence["release_gate_scope"] != model.ReleaseSignalGateScopeControlPlane || evidence["report_only"] != "false" {
		t.Fatalf("expected explicit control-plane release signal evidence, got %+v", evidence)
	}
}

func TestExplicitReleaseSignalFailsWhenConfiguredWorkloadSignalIsMissing(t *testing.T) {
	t.Parallel()

	checks := applyReleaseSignalsToRobustnessChecks(nil, []model.ReleaseSignal{{
		ID:         "sig_missing",
		Enabled:    true,
		OwnerScope: model.ReleaseSignalOwnerScopeTenantWorkload,
		GateScope:  model.ReleaseSignalGateScopeControlPlane,
		Mode:       model.ReleaseSignalModeHardGate,
		Subject:    "app:missing",
		CheckName:  "app_continuity_invariant",
		Reason:     "admin opted this workload into control-plane release success",
	}})
	if len(checks) != 1 || checks[0].Name != "release_signal_observed" || checks[0].Severity != model.RobustnessSeverityBlockPublish {
		t.Fatalf("expected missing explicit release signal to block rollout, got %+v", checks)
	}
}
