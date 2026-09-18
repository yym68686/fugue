package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"fugue/internal/config"
	"fugue/internal/model"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

func readinessTestPlan() (platformconfig.DNSReadinessPlan, platformconfig.DNSReadinessPolicy) {
	policy := platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 1, FactFreshnessSeconds: 60, MaxConcurrency: 2, MaxProbes: 10}
	plan := platformconfig.DNSReadinessPlan{Records: []platformconfig.DNSReadinessRecord{{Hostname: "target.example.test", MinimumHealthyEdges: 2}}}
	for _, edge := range []string{"a", "b"} {
		target := platformconfig.DNSReadinessTarget{EdgeID: "edge-" + edge, EdgeGroupID: "edge-group-" + edge, Address: "8.8.8.8", Family: "A"}
		if edge == "b" {
			target.Address = "9.9.9.9"
		}
		for _, path := range []string{"/", "/api"} {
			p := platformconfig.DNSReadinessProbe{EdgeID: target.EdgeID, EdgeGroupID: target.EdgeGroupID, Address: target.Address, Hostname: "app.example.test", Path: path, RouteDigest: "sha256:" + strings.Repeat(edge, 64)}
			p.ID, _ = platformconfig.DNSReadinessProbeID(p)
			plan.Probes = append(plan.Probes, p)
			target.ProbeIDs = append(target.ProbeIDs, p.ID)
		}
		plan.Records[0].Targets = append(plan.Records[0].Targets, target)
	}
	return plan, policy
}

func TestDNSReadinessFreshFactsExpireAndRequireEveryPathAndQuorum(t *testing.T) {
	plan, policy := readinessTestPlan()
	now := time.Now().UTC()
	originalExpiry := now.Add(30 * time.Second)
	probe := func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
		for _, p := range plan.Probes {
			if p.Address == address && p.Path == path {
				return routeprobe.Proof{Digest: p.RouteDigest, Version: "actual-bundle", EdgeID: p.EdgeID, GroupID: p.EdgeGroupID, CheckedAt: now, ValidUntil: originalExpiry}, nil
			}
		}
		return routeprobe.Proof{}, errors.New("unexpected target")
	}
	facts := collectDNSReadinessFacts(context.Background(), &plan, &policy, probe)
	status := summarizeDNSReadiness(&plan, &policy, facts, "digest", now, now)
	if status.ReadyProbes != 4 || status.ReadyRecords != 1 || status.Serving || !status.FreshUntil.Equal(originalExpiry) {
		t.Fatalf("invalid readiness: %+v", status)
	}
	expired := summarizeDNSReadiness(&plan, &policy, facts, "digest", now, originalExpiry.Add(time.Second))
	if expired.ReadyProbes != 0 || expired.ReadyRecords != 0 {
		t.Fatal("cached facts renewed")
	}
	facts[0].Ready = false
	partial := summarizeDNSReadiness(&plan, &policy, facts, "digest", now, now)
	if partial.ReadyRecords != 0 {
		t.Fatal("one path or one edge satisfied all dependencies")
	}
	facts[0].Ready = true
	facts[0].Proof.Digest = "wrong"
	if status := summarizeDNSReadiness(&plan, &policy, facts, "digest", now, now); status.ReadyRecords != 0 {
		t.Fatal("cached proof binding ignored")
	}
}

func TestDNSReadinessRejectsWrongIdentityFutureStateAndBoundsFreshness(t *testing.T) {
	plan, policy := readinessTestPlan()
	for name, mutate := range map[string]func(*routeprobe.Proof){
		"identity": func(p *routeprobe.Proof) { p.EdgeID = "other" },
		"digest":   func(p *routeprobe.Proof) { p.Digest = "other" },
		"state":    func(p *routeprobe.Proof) { p.State = "disabled" },
		"future":   func(p *routeprobe.Proof) { p.CheckedAt = time.Now().Add(time.Hour) },
		"expired":  func(p *routeprobe.Proof) { p.ValidUntil = time.Now().Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			facts := collectDNSReadinessFacts(context.Background(), &plan, &policy, func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
				for _, requirement := range plan.Probes {
					if requirement.Address == address && requirement.Path == path {
						now := time.Now().UTC()
						p := routeprobe.Proof{Digest: requirement.RouteDigest, Version: "bundle", EdgeID: requirement.EdgeID, GroupID: requirement.EdgeGroupID, CheckedAt: now, ValidUntil: now.Add(time.Hour)}
						mutate(&p)
						return p, nil
					}
				}
				return routeprobe.Proof{}, errors.New("unexpected target")
			})
			for _, fact := range facts {
				if fact.Ready {
					t.Fatal("bad proof became ready")
				}
			}
		})
	}
	facts := collectDNSReadinessFacts(context.Background(), &plan, &policy, func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
		for _, r := range plan.Probes {
			if r.Address == address && r.Path == path {
				now := time.Now().UTC()
				return routeprobe.Proof{Digest: r.RouteDigest, Version: "bundle", EdgeID: r.EdgeID, GroupID: r.EdgeGroupID, CheckedAt: now, ValidUntil: now.Add(time.Hour)}, nil
			}
		}
		return routeprobe.Proof{}, errors.New("unexpected target")
	})
	for _, f := range facts {
		if !f.Ready || f.Proof.ValidUntil.Sub(f.Proof.CheckedAt) != 60*time.Second {
			t.Fatal("policy freshness not enforced")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	facts = collectDNSReadinessFacts(ctx, &plan, &policy, func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		t.Error("canceled scan probed")
		return routeprobe.Proof{}, nil
	})
	if s := summarizeDNSReadiness(&plan, &policy, facts, "digest", time.Now(), time.Now()); s.ReadyProbes != 0 || s.ReadyRecords != 0 {
		t.Fatal("unobserved probes fabricated readiness")
	}
}

func TestDNSReadinessCacheRechecksProofsWithoutRenewingLease(t *testing.T) {
	plan, policy := readinessTestPlan()
	now := time.Now().UTC()
	expiry := now.Add(15 * time.Second)
	digest, _ := platformconfig.Digest(plan)
	candidate := dnsPlatformCandidate{Artifact: model.PlatformArtifact{ID: "dns-artifact", ContentHash: "digest", Content: map[string]any{"readiness_plan": plan, "policy": platformconfig.PolicySnapshot{DNSReadiness: &policy}}}}
	assignment := model.PlatformConsumerAssignment{ReleaseSetID: "release-set", ExpectedConsumerSetID: "expected", FencingToken: 4}
	cached := dnsReadinessReceipt{ArtifactID: candidate.Artifact.ID, ArtifactDigest: candidate.Artifact.ContentHash, ReleaseSetID: assignment.ReleaseSetID, ExpectedConsumerSetID: assignment.ExpectedConsumerSetID, FencingToken: assignment.FencingToken, NodeID: "dns-a", Status: DNSReadinessStatus{PlanDigest: digest, CheckedAt: now, ReadyRecords: 999, ReadyProbes: 999}}
	for _, p := range plan.Probes {
		cached.Facts = append(cached.Facts, dnsReadinessFact{ProbeID: p.ID, Ready: true, Proof: routeprobe.Proof{Digest: p.RouteDigest, Version: "bundle", EdgeID: p.EdgeID, GroupID: p.EdgeGroupID, CheckedAt: now, ValidUntil: expiry}})
	}
	service := &Service{Config: config.DNSConfig{DNSNodeID: "dns-a", CachePath: filepath.Join(t.TempDir(), "dns-cache")}}
	path := service.Config.CachePath + ".platform-dns-readiness.json"
	write := func() {
		raw, _ := json.Marshal(cached)
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	result, err := service.observePlatformDNSReadiness(context.Background(), candidate, assignment)
	if err != nil || result.Status.ReadyRecords != 1 || result.Status.ReadyProbes != 4 || !result.Status.FreshUntil.Equal(expiry) || !result.Status.CheckedAt.Equal(now) || result.Status.Serving {
		t.Fatalf("cached summary was trusted or renewed: %+v %v", result, err)
	}
	cached.Facts[0].Proof.EdgeID = "wrong"
	write()
	result, err = service.observePlatformDNSReadiness(context.Background(), candidate, assignment)
	if err != nil || result.Status.ReadyRecords != 0 {
		t.Fatal("cached wrong identity accepted", err)
	}
	for i := range cached.Facts {
		cached.Facts[i].Proof.ValidUntil = now.Add(-time.Second)
	}
	write()
	result, err = service.observePlatformDNSReadiness(context.Background(), candidate, assignment)
	if err != nil || result.Status.ReadyProbes != 0 || result.Status.ReadyRecords != 0 {
		t.Fatal("expired cache acquired readiness", err)
	}
	// A changed release must collect its own facts even before the old interval
	// elapses. A canceled collection leaves every requirement unobserved.
	assignment.FencingToken++
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = service.observePlatformDNSReadiness(ctx, candidate, assignment)
	if err != nil || result.FencingToken != assignment.FencingToken || result.Status.ReadyProbes != 0 {
		t.Fatal("new release reused old facts", err)
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := service.observePlatformDNSReadiness(context.Background(), candidate, assignment); err == nil {
		t.Fatal("corrupt readiness cache silently reset")
	}
}
