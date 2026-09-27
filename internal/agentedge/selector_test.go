package agentedge

import (
	"crypto/ed25519"
	"strings"
	"testing"
	"time"
)

func selectorFixture(t *testing.T, third bool) (*Selector, Grant, ed25519.PrivateKey, map[string]TrustKey, time.Time, string) {
	t.Helper()
	g, private, keys, now := grantFixture(t)
	g.MinDistinctCells = 1
	g.MinDistinctDomains = map[string]int{"host": 1}
	g.Policy.FactMaxAgeSeconds = 180
	g.ValidUntil = now.Add(150 * time.Second)
	for i := range g.Candidates {
		g.Candidates[i].EvidenceValidUntil = now.Add(160 * time.Second)
	}
	if third {
		c := cloneCandidate(g.Candidates[0])
		c.EdgeID, c.AuthorityCellID, c.Address, c.FailureDomains["host"] = "edge-c", "cell-c", "1.1.1.1", "host-c"
		c.Publication.ServingGroupID = "edge-group-c"
		g.Candidates = append(g.Candidates, c)
	}
	s := &Selector{}
	raw := encodeGrant(t, g, private)
	if err := s.Install(raw, keys, g.Audience, g.Origin, now); err != nil {
		t.Fatal(err)
	}
	v, err := Verify(raw, keys, g.Audience, g.Origin, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	return s, g, private, keys, now, v.Digest()
}

func measurementRound(g Grant, digest string, at time.Time, latency ...time.Duration) Round {
	r := Round{GrantDigest: digest, ObservedAt: at}
	for i, c := range g.Candidates {
		r.Measurements = append(r.Measurements, Measurement{EdgeID: c.EdgeID, Address: c.Address, Success: latency[i] > 0, Latency: latency[i]})
	}
	return r
}

func TestSelectorRequiresSustainedImprovementAndCooldown(t *testing.T) {
	s, g, _, keys, now, digest := selectorFixture(t, false)
	choice, err := s.Observe(measurementRound(g, digest, now, 100*time.Millisecond, 200*time.Millisecond), keys, now)
	if err != nil || choice.Primary.EdgeID != "edge-a" || len(choice.Standbys) != 1 || choice.Standbys[0].EdgeID != "edge-b" {
		t.Fatal("initial measured selection failed", choice, err)
	}
	for second := 10; second <= 80; second += 10 {
		at := now.Add(time.Duration(second) * time.Second)
		choice, err = s.Observe(measurementRound(g, digest, at, 100*time.Millisecond, 10*time.Millisecond), keys, at)
		if err != nil {
			t.Fatal(err)
		}
		want := "edge-a"
		if second == 80 {
			want = "edge-b"
		}
		if choice.Primary.EdgeID != want {
			t.Fatalf("switched without cooldown and independent winning rounds at %d: %+v", second, choice)
		}
		if second == 60 {
			for i := 0; i < 20; i++ {
				read, err := s.Current(keys, at)
				if err != nil || read.Primary.EdgeID != "edge-a" {
					t.Fatal("read counted as another winning sample", read, err)
				}
			}
		}
	}
	if choice.Reason != "sustained_latency_improvement" {
		t.Fatal("missing measured switch explanation")
	}
	choice.Primary.RouteDigests[0] = "modified"
	choice.Standbys[0].FailureDomains["host"] = "modified"
	if got, err := s.Current(keys, now.Add(81*time.Second)); err != nil || !strings.HasPrefix(got.Primary.RouteDigests[0], "sha256:") || got.Standbys[0].FailureDomains["host"] == "modified" {
		t.Fatal("choice exposed mutable authority", err)
	}
}

func TestSelectorFailsOverWithinCooldownAndDoesNotImmediatelyFailBack(t *testing.T) {
	s, g, _, keys, now, digest := selectorFixture(t, false)
	if _, err := s.Observe(measurementRound(g, digest, now, 100*time.Millisecond, 200*time.Millisecond), keys, now); err != nil {
		t.Fatal(err)
	}
	for second := 10; second <= 30; second += 10 {
		at := now.Add(time.Duration(second) * time.Second)
		choice, err := s.Observe(measurementRound(g, digest, at, 0, 30*time.Millisecond), keys, at)
		want := "edge-a"
		if second == 30 {
			want = "edge-b"
		}
		if err != nil || choice.Primary.EdgeID != want {
			t.Fatal("failure threshold or immediate failover violated", choice, err)
		}
		if second == 30 && !choice.Degraded {
			t.Fatal("missing standby was not reported")
		}
	}
	at := now.Add(40 * time.Second)
	choice, err := s.Observe(measurementRound(g, digest, at, time.Millisecond, 30*time.Millisecond), keys, at)
	if err != nil || choice.Primary.EdgeID != "edge-b" {
		t.Fatal("recovered primary stole traffic immediately", choice, err)
	}
}

func TestSelectorRejectsReplayMixedGrantAndForeignMeasurementWithoutMutation(t *testing.T) {
	s, g, _, keys, now, digest := selectorFixture(t, false)
	if _, err := s.Observe(measurementRound(g, digest, now, 100*time.Millisecond, 200*time.Millisecond), keys, now); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"duplicate round", "too frequent", "foreign grant", "foreign address", "unknown edge", "duplicate edge", "partial round", "future round", "invalid latency"} {
		t.Run(scenario, func(t *testing.T) {
			at := now.Add(10 * time.Second)
			round := measurementRound(g, digest, at, time.Millisecond, time.Millisecond)
			switch scenario {
			case "duplicate round":
				round.ObservedAt = now
			case "too frequent":
				round.ObservedAt = now.Add(time.Second)
			case "foreign grant":
				round.GrantDigest = "sha256:" + strings.Repeat("f", 64)
			case "foreign address":
				round.Measurements[0].Address = "1.1.1.1"
			case "unknown edge":
				round.Measurements[0].EdgeID = "edge-unknown"
			case "duplicate edge":
				round.Measurements[1] = round.Measurements[0]
			case "partial round":
				round.Measurements = round.Measurements[:1]
			case "future round":
				round.ObservedAt = at.Add(time.Second)
			case "invalid latency":
				round.Measurements[0].Latency = -time.Second
			}
			if _, err := s.Observe(round, keys, at); err == nil {
				t.Fatal("invalid local observation accepted")
			}
			if choice, err := s.Current(keys, at); err != nil || choice.Primary.EdgeID != "edge-a" || !choice.PrimarySince.Equal(now) {
				t.Fatal("rejected round changed selection", choice, err)
			}
		})
	}
}

func TestSelectorChoosesIndependentStandbyAndHonorsRemoval(t *testing.T) {
	s, g, private, keys, now, digest := selectorFixture(t, true)
	// The faster alternate shares the primary cell; prefer the independent one.
	g.Candidates[2].AuthorityCellID = g.Candidates[0].AuthorityCellID
	g.Candidates[2].Publication = g.Candidates[0].Publication
	raw := encodeGrant(t, g, private)
	// This is the initial permission in this test, not a same-time mutation.
	s = &Selector{}
	if err := s.Install(raw, keys, g.Audience, g.Origin, now); err != nil {
		t.Fatal(err)
	}
	v, err := Verify(raw, keys, g.Audience, g.Origin, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	digest = v.Digest()
	choice, err := s.Observe(measurementRound(g, digest, now, 100*time.Millisecond, 300*time.Millisecond, 110*time.Millisecond), keys, now)
	if err != nil || choice.Standbys[0].EdgeID != "edge-b" || choice.Degraded {
		t.Fatal("latency defeated backup risk diversity", choice, err)
	}
	next := cloneGrant(g)
	next.IssuedAt = now.Add(10 * time.Second)
	next.Candidates = next.Candidates[1:]
	if err := s.Install(encodeGrant(t, next, private), keys, g.Audience, g.Origin, next.IssuedAt); err != nil {
		t.Fatal(err)
	}
	choice, err = s.Current(keys, next.IssuedAt)
	if err != nil || choice.Primary.EdgeID == "edge-a" {
		t.Fatal("removed endpoint retained authority during cooldown", choice, err)
	}
	if choice.Primary.EdgeID != "edge-c" {
		t.Fatal("authorized measured failover not selected", choice)
	}
}

func TestSelectorRejectsExpiredRevokedAndTamperedPermission(t *testing.T) {
	s, g, private, keys, now, digest := selectorFixture(t, false)
	if _, err := s.Observe(measurementRound(g, digest, now, time.Millisecond, 2*time.Millisecond), keys, now); err != nil {
		t.Fatal(err)
	}
	raw := encodeGrant(t, g, private)
	bad := []byte(strings.Replace(string(raw), "8.8.8.8", "1.1.1.1", 1))
	if err := s.Install(bad, keys, g.Audience, g.Origin, now); err == nil {
		t.Fatal("tampered permission installed")
	}
	if _, err := s.Current(keys, now); err != nil {
		t.Fatal("rejected grant discarded existing live permission")
	}
	key := keys["key-one"]
	key.Revoked = true
	if _, err := s.Current(map[string]TrustKey{"key-one": key}, now); err == nil {
		t.Fatal("revoked trust continued serving")
	}
	if _, err := s.Current(keys, g.ValidUntil); err == nil {
		t.Fatal("selector continued after absolute permission expiry")
	}
}

func TestSignedFloorPermitsDegradedSurvivorWithoutPretendingBackupExists(t *testing.T) {
	g, private, keys, now := grantFixture(t)
	g.MinDistinctCells = 1
	g.MinDistinctDomains = map[string]int{"host": 1}
	g.Candidates = g.Candidates[1:]
	raw := encodeGrant(t, g, private)
	s := &Selector{}
	if err := s.Install(raw, keys, g.Audience, g.Origin, now); err != nil {
		t.Fatal(err)
	}
	v, err := Verify(raw, keys, g.Audience, g.Origin, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	choice, err := s.Observe(measurementRound(g, v.Digest(), now, time.Millisecond), keys, now)
	if err != nil || choice.Primary.EdgeID != "edge-b" || !choice.Degraded || len(choice.Standbys) != 0 {
		t.Fatal("survivor did not retain explicitly authorized degraded operation", choice, err)
	}
	shadow := cloneGrant(g)
	shadow.Mode = "shadow"
	shadow.IssuedAt = now.Add(time.Second)
	shadow.PolicyReference.PublishedAt = now
	shadow.PolicyReference.FencingToken++
	shadow.PolicyReference.ReleaseID = "policy-shadow"
	if err := s.Install(encodeGrant(t, shadow, private), keys, g.Audience, g.Origin, shadow.IssuedAt); err != nil {
		t.Fatal(err)
	}
	client, err := NewHTTPClient(s, func() map[string]TrustKey { return keys }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.Transport.(*transport).now = func() time.Time { return shadow.IssuedAt }
	if _, err := client.Get(g.Origin + "/v1/agent/operations"); err == nil || !strings.Contains(err.Error(), "shadow") {
		t.Fatal("shadow observation granted production transport", err)
	}
}

func TestSelectorEnforcesHardFloorsAfterLocalFailure(t *testing.T) {
	for _, constraint := range []string{"count", "cell", "host"} {
		t.Run(constraint, func(t *testing.T) {
			g, private, keys, now := grantFixture(t)
			g.MinDistinctCells = 1
			g.MinDistinctDomains = map[string]int{"host": 1}
			g.Policy.FailureThreshold = 1
			switch constraint {
			case "count":
				g.MinimumCandidates = 2
			case "cell":
				g.MinDistinctCells = 2
			case "host":
				g.MinDistinctDomains["host"] = 2
			}
			raw := encodeGrant(t, g, private)
			s := &Selector{}
			if err := s.Install(raw, keys, g.Audience, g.Origin, now); err != nil {
				t.Fatal(err)
			}
			v, err := Verify(raw, keys, g.Audience, g.Origin, nil, now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Observe(measurementRound(g, v.Digest(), now, time.Millisecond, 2*time.Millisecond), keys, now); err != nil {
				t.Fatal(err)
			}
			at := now.Add(10 * time.Second)
			if _, err = s.Observe(measurementRound(g, v.Digest(), at, 0, 2*time.Millisecond), keys, at); err == nil {
				t.Fatal("local failure silently weakened a signed hard floor")
			}
			if _, err = s.Current(keys, at); err == nil {
				t.Fatal("transport recovered a policy-ineligible choice")
			}
		})
	}
}
