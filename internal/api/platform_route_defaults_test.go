package api

import (
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"reflect"
	"testing"
	"time"
)

func TestPinnedRouteDefaultsPreserveMigrationThenChangeCompilerBehavior(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	configured := []model.PlatformRoute{{Hostname: "control.example.test", Kind: model.EdgeRouteKindControlPlaneAPI, UpstreamURL: "http://control:8080", RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive}, {Hostname: "worker.example.test", Kind: model.EdgeRouteKindPlatform, UpstreamURL: "http://worker:8080", RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive}}
	snapshot := model.EdgeRouteIntentSnapshot{GeneratedAt: now}
	for _, r := range configured {
		snapshot.Routes = append(snapshot.Routes, edgeRouteIntentFromPlatformRoute(r))
	}
	legacy, err := projectBusinessRouteDraft(snapshot, nil, nil, configured, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defaults := platformconfig.NormalizePolicySnapshot(legacy.Policy)
	before, _ := json.Marshal(defaults)
	pinned, err := projectBusinessRouteDraftWithPolicy(snapshot, nil, nil, configured, nil, nil, nil, nil, nil, nil, &defaults)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(legacy, pinned) {
		t.Fatal("pinned route defaults changed migration output")
	}
	defaults.MinimumHealthyEdges = 3
	defaults.MaxStaleSeconds = 10
	defaults.RouteConstraints[0].MinHealthyEdgeNodes = 4
	defaults.DNSRouteStateConstraints = []platformconfig.DNSRouteStateConstraint{}
	changed, err := projectBusinessRouteDraftWithPolicy(snapshot, nil, nil, configured, nil, nil, nil, nil, nil, nil, &defaults)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(changed.Intent, pinned.Intent) || changed.Policy.Generation == pinned.Policy.Generation {
		t.Fatal("policy update mutated intent or lost identity")
	}
	// Complete compilation must carry the global threshold and hostname override.
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: changed.Intent, Policy: changed.Policy, RuntimeSnapshot: changed.RuntimeSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	projected, err := projectPlatformRouteArtifact(compiled.RouteArtifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range projected.Routes {
		want := 3
		if r.Hostname == "control.example.test" {
			want = 4
		}
		if r.MinHealthyEdgeNodes != want {
			t.Fatal("compiled route threshold ignored", r.Hostname, r.MinHealthyEdgeNodes)
		}
	}
	// The same frozen fact becomes stale under the new signed freshness window.
	originRoute := platformconfig.RouteIntent{Hostname: "origin.example.test", UpstreamURL: "http://origin:80", Enabled: true, OriginRef: "origin", RuntimeID: "runtime"}
	facts := platformconfig.RuntimeSnapshot{CapturedAt: &now, Origins: []platformconfig.OriginObservation{{Ref: "origin", RuntimeID: "runtime", ObservedAt: now.Add(-20 * time.Second), Status: model.EdgeRouteStatusActive}}}
	if _, err = platformconfig.ResolveRouteOrigins([]platformconfig.RouteIntent{originRoute}, facts, pinned.Policy); err != nil {
		t.Fatal(err)
	}
	if _, err = platformconfig.ResolveRouteOrigins([]platformconfig.RouteIntent{originRoute}, facts, changed.Policy); err == nil {
		t.Fatal("new freshness policy did not reject stale fact")
	}
	record := platformconfig.DNSIntent{RecordKind: model.EdgeDNSRecordKindCustomDomainTarget}
	inactive := platformconfig.CompiledRoute{RouteIntent: platformconfig.RouteIntent{RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusDisabled}}
	if !platformconfig.DNSRouteStateAllowed(record, inactive, pinned.Policy) || platformconfig.DNSRouteStateAllowed(record, inactive, changed.Policy) {
		t.Fatal("signed DNS inactive behavior ignored")
	}
	restored, err := projectBusinessRouteDraftWithPolicy(snapshot, nil, nil, configured, nil, nil, nil, nil, nil, nil, &pinned.Policy)
	if err != nil || !reflect.DeepEqual(restored.Policy, pinned.Policy) {
		t.Fatal("prior policy not replayable", err)
	}
	after, _ := json.Marshal(pinned.Policy)
	if string(before) != string(after) {
		t.Fatal("source policy mutated")
	}
}

func TestPinnedRouteDefaultsExplicitBusinessOverrideAndOwnership(t *testing.T) {
	now := time.Now().UTC()
	route := model.EdgeRouteIntent{Hostname: "app.example.test", AppID: "app", TenantID: "tenant", RuntimeID: "runtime", UpstreamURL: "http://app:80", RoutePolicy: model.EdgeRoutePolicyEnabled}
	apps := map[string]model.App{"app": {ID: "app", TenantID: "tenant", Spec: model.AppSpec{RuntimeID: "runtime", Replicas: 1}}}
	snap := model.EdgeRouteIntentSnapshot{GeneratedAt: now, Routes: []model.EdgeRouteIntent{route}}
	defaults := platformconfig.NormalizePolicySnapshot(platformconfig.PolicySnapshot{Generation: "base", MinimumHealthyEdges: 2, MaxStaleSeconds: 60, RouteConstraints: []platformconfig.RoutePolicyConstraint{{ID: "base-host", Hostname: route.Hostname, RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true, MinHealthyEdgeNodes: 3}}})
	explicit := []model.EdgeRoutePolicy{{ID: "business", Hostname: route.Hostname, TenantID: "tenant", AppID: "app", RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true, MinHealthyEdgeNodes: 4}}
	result, err := projectBusinessRouteDraftWithPolicy(snap, apps, apps, nil, explicit, nil, nil, nil, nil, nil, &defaults)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Policy.RouteConstraints) != 1 || result.Policy.RouteConstraints[0].ID != "business" || result.Policy.RouteConstraints[0].MinHealthyEdgeNodes != 4 {
		t.Fatal("business override lost", result.Policy.RouteConstraints)
	}
	defaults.RouteConstraints[0].Hostname = "absent.example.test"
	if _, err = projectBusinessRouteDraftWithPolicy(snap, apps, apps, nil, explicit, nil, nil, nil, nil, nil, &defaults); err == nil {
		t.Fatal("unreferenced base rule silently ignored")
	}
	defaults.RouteConstraints[0].Hostname = route.Hostname
	defaults.RouteConstraints[0].TenantID = "foreign"
	result, err = projectBusinessRouteDraftWithPolicy(snap, apps, apps, nil, nil, nil, nil, nil, nil, nil, &defaults)
	if err != nil {
		t.Fatal(err)
	}
	_, err = platformconfig.ApplyRoutePolicyConstraints([]platformconfig.CompiledRoute{{RouteIntent: result.Intent.Routes[0]}}, result.Policy, &now)
	if err == nil {
		t.Fatal("foreign policy owner granted authority")
	}
}
