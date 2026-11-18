package passiveasn

import (
	"net/netip"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/edgequality"
	"fugue/internal/model"
)

func TestPassiveASNObservationAssignmentAndExploration(t *testing.T) {
	now := time.Now().UTC()
	prefix := netip.MustParsePrefix("203.0.113.0/24")
	a := edgequality.PassiveEndpoint{EdgeID: "a", GroupID: "g1", Address: "8.8.8.8"}
	b := edgequality.PassiveEndpoint{EdgeID: "b", GroupID: "g2", Address: "9.9.9.9"}
	c := edgequality.PassiveEndpoint{EdgeID: "c", GroupID: "g3", Address: "1.1.1.1"}
	d := edgequality.PassiveEndpoint{EdgeID: "d", GroupID: "g4", Address: "8.8.4.4"}
	p := Publication{
		Mode: "active", DNSArtifactID: "artifact", DNSDigest: "digest", ReleaseSetID: "release", ConsumerSetID: "set",
		FencingToken: 1, NodeID: "authority", ObservedAt: now, ExpiresAt: now.Add(PublicationTTL),
		ResolverSourceAt: now.Add(-time.Hour), Resolvers: []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")},
		Bindings: []Binding{{prefix, 64496}}, Pairs: []edgequality.PassiveASNComparison{{
			ASN: 64496, Family: 4, Left: a, Right: b, Preferred: "a", ObservedAt: now.Add(-7 * 24 * time.Hour),
			SharedWindows: 3, LeftWinWindows: 3, Prefixes: []netip.Prefix{prefix},
		}},
	}
	assignment := model.PlatformConsumerAssignment{ArtifactID: p.DNSArtifactID, ContentHash: p.DNSDigest,
		ReleaseSetID: p.ReleaseSetID, ExpectedConsumerSetID: p.ConsumerSetID, FencingToken: p.FencingToken}
	keys := bundleauth.NewKeyring("observation-key", "key-id", "", "", nil)
	artifact, err := Sign(p, keys)
	if err != nil {
		t.Fatal(err)
	}
	view, err := Verify(artifact, assignment, p.NodeID, keys, now)
	if err != nil {
		t.Fatal(err)
	}
	order := []edgequality.PassiveEndpoint{a, b, c, d}
	if choice := view.Choose(64496, 4, order, 0); choice.Edge != a || !choice.EvidenceAt.Equal(p.Pairs[0].ObservedAt) {
		t.Fatal("lost original comparison", choice)
	}
	if view.Choose(64496, 4, order, 1).Edge != c || view.Choose(64496, 4, order, 2).Edge != d {
		t.Fatal("exploration retried the proven loser or failed to rotate")
	}
	if _, err := Verify(artifact, assignment, p.NodeID, keys, p.ExpiresAt); err == nil {
		t.Fatal("expired observation accepted")
	}
	assignment.FencingToken++
	if _, err := Verify(artifact, assignment, p.NodeID, keys, now); err == nil {
		t.Fatal("another fence accepted the observation")
	}
	assignment.FencingToken--
	artifact.Content["mode"] = "shadow"
	if _, err := Verify(artifact, assignment, p.NodeID, keys, now); err == nil {
		t.Fatal("accepted unsigned observation modification")
	}
}
