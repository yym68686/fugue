package agentedge

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func grantFixture(t *testing.T) (Grant, ed25519.PrivateKey, map[string]TrustKey, time.Time) {
	t.Helper()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	publication := Publication{ReleaseSetID: "parent-one", ReleaseSetDigest: digest, RouteArtifactID: "route-one", RouteArtifactDigest: digest,
		PolicyDigest: digest, IntentDigest: digest, InputSnapshotDigest: digest, TopologyDigest: digest,
		ScopeKey: "global", ReleaseID: "release-one", Channel: "full", FencingToken: 12, PublishedAt: now.Add(-time.Minute)}
	g := Grant{Schema: GrantSchema, Purpose: GrantPurpose, Audience: "runtime-test", Origin: "https://api.example.test", Mode: "active",
		PolicyReference: PolicyReference{ArtifactID: "policy-one", ArtifactDigest: digest, ReleaseID: "policy-release-one", Channel: "full", FencingToken: 1, PublishedAt: now.Add(-time.Hour)},
		Policy: Policy{ProbeIntervalSeconds: 10, ProbeTimeoutMilliseconds: 500, FactMaxAgeSeconds: 120, FailureThreshold: 3,
			BetterSampleThreshold: 3, SwitchImprovementPercent: 15, SwitchCooldownSeconds: 60, StandbyCount: 1, DesiredDistinctCells: 2, MaxCandidates: 8},
		MinimumCandidates: 1, MinDistinctCells: 2, MinDistinctDomains: map[string]int{"host": 2}, IssuedAt: now.Add(-time.Second), ValidUntil: now.Add(40 * time.Second)}
	for i, address := range []string{"8.8.8.8", "9.9.9.9"} {
		id := string(rune('a' + i))
		g.Candidates = append(g.Candidates, Candidate{Publication: publication, EdgeID: "edge-" + id, AuthorityCellID: "cell-" + id, Address: address,
			RouteDigests: []string{digest}, FailureDomains: map[string]string{"host": "host-" + id}, EvidenceDigest: digest,
			EvidenceObservedAt: now.Add(-3 * time.Second), EvidenceValidUntil: now.Add(time.Minute)})
	}
	for i := range g.Candidates {
		g.Candidates[i].Publication.ServingGroupID = "edge-group-" + string(rune('a'+i))
	}
	keys := map[string]TrustKey{"key-one": {PublicKey: pub, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}}
	return g, private, keys, now
}

func encodeGrant(t *testing.T, g Grant, key ed25519.PrivateKey) []byte {
	t.Helper()
	s, err := Sign(g, "key-one", key)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestGrantVerificationBindsAudienceOriginAndAbsoluteEvidenceExpiry(t *testing.T) {
	g, private, keys, now := grantFixture(t)
	raw := encodeGrant(t, g, private)
	verified, err := Verify(raw, keys, g.Audience, g.Origin, nil, now)
	if err != nil || !verified.Live(keys, now) {
		t.Fatal("fresh independently signed grant rejected", err)
	}
	for _, tc := range []struct {
		name, audience, origin string
		at                     time.Time
	}{
		{"foreign audience", "other-runtime", g.Origin, now},
		{"foreign origin", g.Audience, "https://foreign.example.test", now},
		{"future issuance", g.Audience, g.Origin, now.Add(-time.Minute)},
		{"expired", g.Audience, g.Origin, g.ValidUntil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Verify(raw, keys, tc.audience, tc.origin, nil, tc.at); err == nil {
				t.Fatal("foreign or stale authorization accepted")
			}
		})
	}
	view := verified.View()
	view.Candidates[0].Address = "1.1.1.1"
	view.Candidates[0].RouteDigests[0] = "changed"
	view.Candidates[0].FailureDomains["host"] = "changed"
	view.MinDistinctDomains["host"] = 1
	if !verified.Live(keys, now) || verified.View().Candidates[0].Address != g.Candidates[0].Address {
		t.Fatal("caller mutated the verified permission")
	}
	if verified.Live(keys, g.ValidUntil) || (VerifiedGrant{}).Live(keys, now) {
		t.Fatal("expiration or zero-value verification fabricated authorization")
	}
}

func TestGrantRejectsTamperingAmbiguousJSONAndUntrustedKeys(t *testing.T) {
	g, private, keys, now := grantFixture(t)
	raw := encodeGrant(t, g, private)
	for name, bad := range map[string][]byte{
		"candidate address": []byte(strings.Replace(string(raw), "8.8.8.8", "1.1.1.1", 1)),
		"unknown field":     append([]byte(`{"extra":true,`), raw[1:]...),
		"duplicate key":     append([]byte(`{"key_id":"key-one",`), raw[1:]...),
		"trailing JSON":     append(append([]byte(nil), raw...), []byte(` {}`)...),
		"oversized":         []byte(strings.Repeat(" ", MaxGrantBytes+1)),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Verify(bad, keys, g.Audience, g.Origin, nil, now); err == nil {
				t.Fatal("malformed or modified signed permission accepted")
			}
		})
	}
	other, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"unknown", "wrong key", "revoked", "not yet valid", "expired", "grant outlives key"} {
		t.Run(scenario, func(t *testing.T) {
			key := keys["key-one"]
			trusted := map[string]TrustKey{}
			switch scenario {
			case "wrong key":
				key.PublicKey = other
			case "revoked":
				key.Revoked = true
			case "not yet valid":
				key.NotBefore = now.Add(time.Minute)
			case "expired":
				key.NotAfter = now
			case "grant outlives key":
				key.NotAfter = now.Add(time.Second)
			}
			if scenario != "unknown" {
				trusted["key-one"] = key
			}
			if _, err := Verify(raw, trusted, g.Audience, g.Origin, nil, now); err == nil {
				t.Fatal("grant supplied or outlived its own trust")
			}
		})
	}
}

func TestGrantPreservesAuthorityAndRiskConstraints(t *testing.T) {
	g, private, _, now := grantFixture(t)
	for name, change := range map[string]func(*Grant){
		"plaintext origin":   func(g *Grant) { g.Origin = "http://api.example.test" },
		"foreign port":       func(g *Grant) { g.Origin += ":8443" },
		"origin path":        func(g *Grant) { g.Origin += "/v1" },
		"business tunnel":    func(g *Grant) { g.Purpose = "business-tunnel" },
		"shadow":             func(g *Grant) { g.Candidates[0].Publication.Channel = "shadow" },
		"unknown scope":      func(g *Grant) { g.Candidates[0].Publication.ScopeKey = "other" },
		"future publication": func(g *Grant) { g.Candidates[0].Publication.PublishedAt = now },
		"private address":    func(g *Grant) { g.Candidates[0].Address = "10.0.0.2" },
		"loopback":           func(g *Grant) { g.Candidates[0].Address = "127.0.0.1" },
		"metadata address":   func(g *Grant) { g.Candidates[0].Address = "169.254.169.254" },
		"mapped IPv4":        func(g *Grant) { g.Candidates[0].Address = "::ffff:8.8.8.8" },
		"duplicate address":  func(g *Grant) { g.Candidates[0].Address = g.Candidates[1].Address },
		"duplicate edge":     func(g *Grant) { g.Candidates[1].EdgeID = g.Candidates[0].EdgeID },
		"shared cell":        func(g *Grant) { g.Candidates[1].AuthorityCellID = g.Candidates[0].AuthorityCellID },
		"shared host":        func(g *Grant) { g.Candidates[1].FailureDomains["host"] = g.Candidates[0].FailureDomains["host"] },
		"country as risk":    func(g *Grant) { g.MinDistinctDomains = map[string]int{"country": 1} },
		"proof expiry":       func(g *Grant) { g.Candidates[0].EvidenceValidUntil = now },
		"renewed proof":      func(g *Grant) { g.Candidates[0].EvidenceObservedAt = now.Add(-2 * time.Minute) },
		"future observation": func(g *Grant) { g.Candidates[0].EvidenceObservedAt = now },
		"unbounded policy":   func(g *Grant) { g.Policy.MaxCandidates = 1000 },
	} {
		t.Run(name, func(t *testing.T) {
			copy := cloneGrant(g)
			change(&copy)
			if _, err := Sign(copy, "key-one", private); err == nil {
				t.Fatal("invalid policy or unbound evidence was signed")
			}
		})
	}
}

func TestGrantReplayProtectionAllowsExplicitNewRollbackPublication(t *testing.T) {
	g, private, keys, now := grantFixture(t)
	previous, err := Verify(encodeGrant(t, g, private), keys, g.Audience, g.Origin, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"same", "fresh observation", "old issuance", "old publication", "equivocation", "same-time changed permission", "new rollback"} {
		t.Run(scenario, func(t *testing.T) {
			next := cloneGrant(g)
			next.IssuedAt = now
			want := false
			switch scenario {
			case "same":
				next = cloneGrant(g)
				want = true
			case "fresh observation":
				want = true
			case "old issuance":
				next.IssuedAt = g.IssuedAt.Add(-time.Second)
			case "old publication":
				next.Candidates[0].Publication.PublishedAt = g.Candidates[0].Publication.PublishedAt.Add(-time.Minute)
			case "equivocation":
				next.Candidates[0].Publication.ReleaseID = "foreign-release"
			case "same-time changed permission":
				next.IssuedAt = g.IssuedAt
				next.Candidates[0].Address = "1.1.1.1"
			case "new rollback":
				next.Candidates[0].Publication.PublishedAt = now.Add(-500 * time.Millisecond)
				next.Candidates[0].Publication.ReleaseID = "rollback-publication"
				next.Candidates[0].Publication.ReleaseSetID = "older-parent"
				next.Candidates[0].Publication.Channel = "gray"
				next.Candidates[0].Publication.FencingToken = 1
				want = true
			}
			_, err := Verify(encodeGrant(t, next, private), keys, g.Audience, g.Origin, &previous, now)
			if (err == nil) != want {
				t.Fatalf("publication order decision wrong: %v", err)
			}
			if !previous.Live(keys, now) {
				t.Fatal("verification replaced the previous permission")
			}
		})
	}
}

func TestIndependentCellRenewalRetainsWatermarksAcrossDegradedGrants(t *testing.T) {
	g, private, keys, now := grantFixture(t)
	g.MinDistinctCells = 1
	g.MinDistinctDomains = map[string]int{"host": 1}
	first, err := Verify(encodeGrant(t, g, private), keys, g.Audience, g.Origin, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneGrant(g)
	next.IssuedAt = now.Add(time.Second)
	next.Candidates[0].Publication.PublishedAt = now
	next.Candidates[0].Publication.FencingToken++
	next.Candidates[0].Publication.ReleaseID = "cell-a-next"
	advanced, err := Verify(encodeGrant(t, next, private), keys, g.Audience, g.Origin, &first, next.IssuedAt)
	if err != nil {
		t.Fatal("one cell could not advance independently", err)
	}
	only := cloneGrant(next)
	only.IssuedAt = now.Add(2 * time.Second)
	only.Candidates = only.Candidates[1:]
	degraded, err := Verify(encodeGrant(t, only, private), keys, g.Audience, g.Origin, &advanced, only.IssuedAt)
	if err != nil {
		t.Fatal("explicit one-candidate floor revoked a healthy surviving cell", err)
	}
	replay := cloneGrant(g)
	replay.IssuedAt = now.Add(3 * time.Second)
	if _, err := Verify(encodeGrant(t, replay, private), keys, g.Audience, g.Origin, &degraded, replay.IssuedAt); err == nil {
		t.Fatal("temporarily absent cell forgot its publication watermark")
	}
	next.IssuedAt = now.Add(4 * time.Second)
	if _, err := Verify(encodeGrant(t, next, private), keys, g.Audience, g.Origin, &degraded, next.IssuedAt); err != nil {
		t.Fatal("current cell publication could not rejoin", err)
	}
	reconfigured := cloneGrant(next)
	reconfigured.IssuedAt = now.Add(5 * time.Second)
	reconfigured.Mode = "shadow"
	if _, err := Verify(encodeGrant(t, reconfigured, private), keys, g.Audience, g.Origin, &advanced, reconfigured.IssuedAt); err == nil {
		t.Fatal("selection mode changed without a policy publication")
	}
	reconfigured.PolicyReference.PublishedAt = now.Add(4 * time.Second)
	reconfigured.PolicyReference.FencingToken++
	reconfigured.PolicyReference.ReleaseID = "policy-next"
	if _, err := Verify(encodeGrant(t, reconfigured, private), keys, g.Audience, g.Origin, &advanced, reconfigured.IssuedAt); err != nil {
		t.Fatal("independent policy publication was coupled to cell publication", err)
	}
}
