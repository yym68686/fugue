package dnsserver

import (
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
	"strings"
	"testing"
	"time"
)

func TestStaticDNSProofCannotImpersonatePlatformOrOtherApps(t *testing.T) {
	now := time.Now().UTC()
	r := platformconfig.DNSReadinessProbe{EdgeID: "static-a", EdgeGroupID: platformconfig.StaticEdgeDNSGroup("static-a"), Hostname: "app.example.test", Address: "1.1.1.1", Path: "/health", RouteDigest: "sha256:" + strings.Repeat("c", 64), StaticEdge: &platformconfig.StaticEdgeDNSProbe{StatusCode: 200}}
	r.ID, _ = platformconfig.DNSReadinessProbeID(r)
	policy := &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 10, ProbeTimeoutSeconds: 2, FactFreshnessSeconds: 30, MaxConcurrency: 2, MaxProbes: 10}
	proof := routeprobe.Proof{Kind: routeprobe.StaticEdgeProofKind, Version: model.StaticEdgeDNSVersion, Digest: r.RouteDigest, EdgeID: r.EdgeID, GroupID: r.EdgeGroupID, CheckedAt: now.Add(-time.Second), ValidUntil: now.Add(20 * time.Second)}
	fact := evaluateDNSReadinessFact(r, policy, proof, nil, now)
	if !fact.Ready {
		t.Fatal("valid static proof rejected", fact)
	}
	payload := dnsServingPayload{Plan: &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{r}}, Policy: platformconfig.PolicySnapshot{}}
	if !dnsFactMatchesRelease(fact, model.PlatformArtifact{}, dnsPlatformCandidate{}, "", payload) {
		t.Fatal("static readiness demanded a platform release")
	}
	for name, mutate := range map[string]func(*platformconfig.DNSReadinessProbe, *routeprobe.Proof){
		"platform proof":       func(_ *platformconfig.DNSReadinessProbe, p *routeprobe.Proof) { p.Kind = "" },
		"platform requirement": func(r *platformconfig.DNSReadinessProbe, _ *routeprobe.Proof) { r.StaticEdge = nil },
		"other app digest": func(_ *platformconfig.DNSReadinessProbe, p *routeprobe.Proof) {
			p.Digest = "sha256:" + strings.Repeat("d", 64)
		},
		"other edge": func(_ *platformconfig.DNSReadinessProbe, p *routeprobe.Proof) { p.EdgeID = "static-b" },
		"expired":    func(_ *platformconfig.DNSReadinessProbe, p *routeprobe.Proof) { p.ValidUntil = now.Add(-time.Second) },
		"managed release": func(_ *platformconfig.DNSReadinessProbe, p *routeprobe.Proof) {
			p.TrafficRelease = &model.TrafficReleaseBinding{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			req, bad := r, proof
			mutate(&req, &bad)
			f := evaluateDNSReadinessFact(req, policy, bad, nil, now)
			if f.Ready {
				t.Fatal("confused static proof accepted")
			}
			f.Ready = true
			if len(validDNSReadinessFacts(&platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{req}}, policy, []dnsReadinessFact{f}, now)) != 0 {
				t.Fatal("cached invalid static proof accepted")
			}
		})
	}
	payload.Policy.PublicationRole = platformconfig.PublicationRoleCellDNS
	if dnsFactMatchesRelease(fact, model.PlatformArtifact{}, dnsPlatformCandidate{}, "", payload) {
		t.Fatal("unsupported cell authority accepted")
	}
}
