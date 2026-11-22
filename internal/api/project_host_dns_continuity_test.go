package api

import (
	"context"
	"errors"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

func TestAutomaticHostedPathChangeRequiresTheRollbackDNSCheck(t *testing.T) {
	record := platformconfig.DNSIntent{Hostname: "service.example.test", Type: "A", RecordKind: "hosted", AppID: "owner", TenantID: "tenant"}
	graph := func(paths ...string) model.PlatformArtifact {
		plan := platformconfig.DNSReadinessPlan{}
		for _, path := range paths {
			plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{Hostname: record.Hostname, Path: path})
		}
		return model.PlatformArtifact{Content: map[string]any{"records": []platformconfig.DNSIntent{record}, "readiness_plan": plan}}
	}
	unavailable := errors.New("required cell receipt is stale even though the global release is verified")
	single, shared := graph("/"), graph("/", "/api")
	for _, tc := range []struct {
		name          string
		before, after model.PlatformArtifact
		fail, checked bool
	}{
		{"forward blocked", single, shared, true, true}, {"revert blocked", shared, single, true, true},
		{"forward verified", single, shared, false, true}, {"no path change during DNS recovery", single, single, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := guardProducedHostedDNSPaths(context.Background(), tc.before, tc.after, func(context.Context) error {
				calls++
				if tc.fail {
					return unavailable
				}
				return nil
			})
			if (calls == 1) != tc.checked || (err != nil) != (tc.checked && tc.fail) || err != nil && !errors.Is(err, unavailable) {
				t.Fatalf("checks=%d err=%v", calls, err)
			}
		})
	}
}

func TestProjectHostDNSContinuityRequiresAllLiveConsumers(t *testing.T) {
	for _, name := range []string{"both_ready", "old_binary", "legacy_continuity", "stale", "no_receipt", "unexpected", "missing"} {
		t.Run(name, func(t *testing.T) {
			fact := &model.PlatformConsumerInstance{CompatibilityCapabilities: []string{platformconfig.DNSManagedHostContinuityCapabilityV1}}
			status := model.PlatformConsumerConvergenceStatus{Pass: true, RequiredExpected: 2, RequiredPassing: 2, Assessments: []model.PlatformConsumerEvidenceAssessment{
				{Required: true, State: model.InvariantEvidenceStatePass, Observed: fact},
				{Required: true, State: model.InvariantEvidenceStatePass, Observed: fact},
			}}
			switch name {
			case "old_binary":
				status.Assessments[1].Observed = &model.PlatformConsumerInstance{}
			case "stale":
				status.Assessments[1].State = model.InvariantEvidenceStateUnknown
			case "no_receipt":
				status.Assessments[1].Observed = nil
			case "unexpected":
				status.UnexpectedConsumers = []string{"obsolete-dns"}
			case "missing":
				status.Assessments = status.Assessments[:1]
			}
			if ready := projectHostConsumersReady(status, platformconfig.DNSManagedHostContinuityCapabilityV1); ready != (name == "both_ready" || name == "legacy_continuity") {
				t.Fatalf("ready=%t for %s", ready, name)
			}
			if projectHostConsumersReady(status, platformconfig.DNSHostedPathTransitionCapabilityV1) {
				t.Fatal("standby or legacy continuity was mistaken for global path-transition support")
			}
			if name == "both_ready" {
				fact.CompatibilityCapabilities = append(fact.CompatibilityCapabilities, platformconfig.DNSHostedPathTransitionCapabilityV1)
				if !projectHostConsumersReady(status, platformconfig.DNSHostedPathTransitionCapabilityV1) {
					t.Fatal("two fresh required witnesses did not admit global path-transition support")
				}
			}
			if projectHostConsumersReady(status, platformcontrol.SharedCustomHostTLSCapabilityV1) {
				t.Fatal("DNS continuity cannot substitute for the Worker TLS capability")
			}
			fact.CompatibilityCapabilities = append(fact.CompatibilityCapabilities, platformcontrol.SharedCustomHostTLSCapabilityV1)
			if ready := projectHostConsumersReady(status, platformcontrol.SharedCustomHostTLSCapabilityV1); ready != (name == "both_ready" || name == "legacy_continuity") {
				t.Fatalf("worker ready=%t for %s", ready, name)
			}
		})
	}
}

func TestHostedDNSProbePathGuardIgnoresUnrelatedPublicationChanges(t *testing.T) {
	host := "app.example.test"
	dnsArtifact := func(name, owner, tenant, kind, digest string, paths ...string) model.PlatformArtifact {
		plan := platformconfig.DNSReadinessPlan{}
		for _, path := range paths {
			plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{Hostname: name, Path: path, RouteDigest: digest})
		}
		return model.PlatformArtifact{Content: map[string]any{
			"records":        []platformconfig.DNSIntent{{Hostname: name, AppID: owner, TenantID: tenant, Type: "A", RecordKind: kind}},
			"readiness_plan": plan,
		}}
	}
	single := dnsArtifact(host, "api", "tenant", "hosted", "old", "/")
	shared := dnsArtifact(host, "api", "tenant", "hosted", "new", "/", "/api")
	for _, tc := range []struct {
		name          string
		before, after model.PlatformArtifact
		want          bool
	}{
		{"split", single, shared, true},
		{"rollback merge", shared, single, true},
		{"change shared path", shared, dnsArtifact(host, "api", "tenant", "hosted", "new", "/", "/next"), true},
		{"digest and membership only", shared, dnsArtifact(host, "api", "tenant", "hosted", "newer", "/api", "/", "/"), false},
		{"new or removed hostname", single, dnsArtifact("other.example.test", "api", "tenant", "hosted", "new", "/", "/api"), false},
		{"external DNS", single, dnsArtifact(host, "api", "tenant", "static", "new", "/", "/api"), false},
		{"separate owner", single, dnsArtifact(host, "new", "tenant", "hosted", "new", "/", "/api"), false},
		{"separate tenant", single, dnsArtifact(host, "api", "other", "hosted", "new", "/", "/api"), false},
		{"complete route withdrawal", shared, dnsArtifact(host, "api", "tenant", "hosted", "new"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hostedDNSProbePathsChanged(tc.before, tc.after)
			if err != nil || got != tc.want {
				t.Fatalf("transition=%t, want %t; err=%v", got, tc.want, err)
			}
		})
	}
}

func TestHostedDNSPathChangeCannotAlsoChangeAddressAuthority(t *testing.T) {
	const hostname = "api.example.test"
	build := func(pathSplit bool, addresses []string, ttl, quorum int) model.PlatformArtifact {
		plan := platformconfig.DNSReadinessPlan{}
		record := platformconfig.DNSReadinessRecord{Hostname: hostname, MinimumHealthyEdges: quorum}
		for _, address := range addresses {
			target := platformconfig.DNSReadinessTarget{EdgeID: "edge-" + address, Address: address, Family: "A", ProbeIDs: []string{"root-" + address}}
			plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{Hostname: hostname, Path: "/", Address: address, ID: target.ProbeIDs[0]})
			if pathSplit {
				id := "api-" + address
				target.ProbeIDs = append(target.ProbeIDs, id)
				plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{Hostname: hostname, Path: "/v1", Address: address, ID: id})
			}
			record.Targets = append(record.Targets, target)
		}
		plan.Records = []platformconfig.DNSReadinessRecord{record}
		return model.PlatformArtifact{Content: map[string]any{
			"records":        []platformconfig.DNSIntent{{Hostname: hostname, AppID: "api", TenantID: "tenant", RecordKind: "hosted", Type: "A", TTL: ttl, Values: addresses}},
			"readiness_plan": plan,
		}}
	}
	oldAddrs := []string{"192.0.2.1", "192.0.2.2"}
	before := build(false, oldAddrs, 60, 1)
	for _, after := range []model.PlatformArtifact{
		build(true, oldAddrs[:1], 60, 1),
		build(true, append(append([]string(nil), oldAddrs...), "192.0.2.3"), 60, 1),
		build(true, oldAddrs, 90, 1),
		build(true, oldAddrs, 60, 2),
	} {
		if changed, err := hostedDNSProbePathsChanged(before, after); changed || !errors.Is(err, errHostedDNSPathTopology) {
			t.Fatalf("combined path and address authority changes were admitted: changed=%t err=%v", changed, err)
		}
	}
	if changed, err := hostedDNSProbePathsChanged(before, build(true, oldAddrs, 60, 1)); !changed || err != nil {
		t.Fatalf("changed proof membership alone must still reach the capability gate: changed=%t err=%v", changed, err)
	}
	if changed, err := hostedDNSProbePathsChanged(before, build(false, oldAddrs[:1], 60, 1)); changed || err != nil {
		t.Fatalf("standalone endpoint withdrawal must keep its own path: changed=%t err=%v", changed, err)
	}
}

func TestManagedProjectHostPreflightBlocksUnreadyWorkersOnlyForSharedDestination(t *testing.T) {
	single := map[string]string{"/": "api"}
	shared := map[string]string{"/": "web", "/api": "api"}
	for _, test := range []struct {
		name          string
		before, after map[string]string
		dns, worker   error
		want          error
	}{
		{"enable with old Worker", single, shared, nil, errProjectHostWorkerTLS, errProjectHostWorkerTLS},
		{"keep with old Worker", shared, shared, nil, errProjectHostWorkerTLS, errProjectHostWorkerTLS},
		{"revert without Worker", shared, single, nil, errProjectHostWorkerTLS, nil},
		{"revert without DNS continuity", shared, single, errProjectHostDNSContinuity, nil, errProjectHostDNSContinuity},
		{"retain one application", single, single, errProjectHostDNSContinuity, errProjectHostWorkerTLS, nil},
		{"enable with both ready", single, shared, nil, nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := model.ProjectHostRoutePlan{Before: test.before, After: test.after, HostnameOwnerAppID: "api"}
			if err := projectHostManagedPreflight(plan, test.dns, test.worker); !errors.Is(err, test.want) {
				t.Fatalf("preflight returned %v, want %v", err, test.want)
			}
		})
	}
}
