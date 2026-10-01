// Package dnsroutesource authenticates routing source observations authorized
// by an independent signed DNS artifact. It does not collect readiness or select
// a DNS answer. Mutable selection must be rechecked by the observation caller.
package dnsroutesource

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/cellpublication"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func VerifyPublication(p model.PlatformDNSRouteSourcePublication, approvals []platformconfig.DNSRouteSourceAuthorization, keys bundleauth.Keyring, now time.Time) error {
	fail := func() error { return fmt.Errorf("DNS routing source publication binding invalid") }
	for _, a := range []model.PlatformArtifact{p.Parent, p.Route, p.TLS, p.ProducerPolicy} {
		if a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
			return fail()
		}
	}
	approved := slices.ContainsFunc(approvals, func(a platformconfig.DNSRouteSourceAuthorization) bool {
		return a.ScopeKey == p.Parent.ScopeKey && a.PolicyArtifactID == p.ProducerPolicy.ID && a.PolicyDigest == p.ProducerPolicy.ContentHash
	})
	policy, err := platformproducer.Decode(p.ProducerPolicy)
	role := platformconfig.PublicationRoleCellRoutes
	if p.Parent.ScopeKey == platformconfig.GlobalScopeKey {
		role = ""
	}
	if !approved || err != nil || policy.TargetScope != p.Parent.ScopeKey || policy.PublicationRole != role || policy.Mode != "serving" || policy.Serving.SinglePublication {
		return fail()
	}
	producer := p.ProducerRelease
	if producer.ArtifactID != p.ProducerPolicy.ID || producer.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || producer.ScopeKey != p.ProducerPolicy.ScopeKey || producer.Generation != p.ProducerPolicy.Generation || producer.FencingToken <= 0 || producer.ReleaseChannel != "shadow" || producer.ReleasedAt.IsZero() || producer.ReleasedAt.After(now) || producer.LaneKey != platformsafety.ReleaseLaneKey(producer.ArtifactKind, producer.ScopeKey, producer.ReleaseChannel) || (producer.Status != model.PlatformArtifactReleaseStatusActive && producer.Status != model.PlatformArtifactReleaseStatusSuperseded) || p.Parent.Metadata[platformproducer.PolicyReleaseMetadata] != producer.ID {
		return fail()
	}
	if p.Parent.ArtifactKind != model.PlatformArtifactKindReleaseSet || p.Release.ID == "" || p.Release.ArtifactID != p.Parent.ID || p.Release.ArtifactKind != p.Parent.ArtifactKind || p.Release.Generation != p.Parent.Generation || p.Release.ScopeKey != p.Parent.ScopeKey || p.Release.FencingToken <= 0 || p.Release.ReleasedAt.IsZero() || p.Release.ReleasedAt.After(now) || p.Release.ReleasedAt.Before(producer.ReleasedAt) || p.Release.VerificationState == model.PlatformArtifactVerificationStateFailed || p.Release.Status == model.PlatformArtifactReleaseStatusRolledBack || p.Release.LaneKey != platformsafety.ReleaseLaneKey(p.Release.ArtifactKind, p.Release.ScopeKey, p.Release.ReleaseChannel) {
		return fail()
	}
	var set platformconfig.ReleaseSet
	raw, _ := json.Marshal(p.Parent.Content)
	_, compositionErr := platformconfig.ValidateReleaseComposition(p.Parent)
	if json.Unmarshal(raw, &set) != nil || compositionErr != nil || set.PublicationRole != role || set.Scope != p.Parent.ScopeKey || set.Generation != p.Parent.Generation || !reflect.DeepEqual(set.Lineage, platformconfig.LineageFromArtifact(p.Parent)) {
		return fail()
	}
	for _, a := range []model.PlatformArtifact{p.Route, p.TLS} {
		index := slices.Index(set.ArtifactKinds, a.ArtifactKind)
		if index < 0 || set.ArtifactIDs[index] != a.ID || a.ScopeKey != p.Parent.ScopeKey || a.Metadata["release_set_generation"] != p.Parent.Generation || !reflect.DeepEqual(set.Lineage, platformconfig.LineageFromArtifact(a)) || platformconfig.ValidateTrafficCohortProjection(p.Parent, a) != nil {
			return fail()
		}
		var payload struct {
			Policy  platformconfig.PolicySnapshot `json:"policy"`
			Lineage platformconfig.Lineage        `json:"lineage"`
		}
		raw, _ = json.Marshal(a.Content)
		if json.Unmarshal(raw, &payload) != nil || !reflect.DeepEqual(payload.Lineage, set.Lineage) || platformconfig.ValidatePolicySnapshot(payload.Policy) != nil {
			return fail()
		}
		digest, err := platformconfig.Digest(payload.Policy)
		if err != nil || digest != set.Lineage.PolicyDigest {
			return fail()
		}
	}
	if p.Route.ArtifactKind != model.PlatformArtifactKindEdgeRouteBundle || p.TLS.ArtifactKind != model.PlatformArtifactKindCaddyRouteConfig {
		return fail()
	}
	switch p.Release.ReleaseChannel {
	case "full":
		if p.Release.CanaryRuleRef != "" {
			return fail()
		}
	case "gray":
		if _, err := platformconfig.ResolveTrafficCanary(p.Parent, p.Release.CanaryRuleRef); err != nil {
			return fail()
		}
	default:
		return fail()
	}
	if len(p.Selections) < 1 || len(p.Selections) > 3 {
		return fail()
	}
	seen := map[string]bool{}
	for _, selection := range p.Selections {
		if seen[selection] {
			return fail()
		}
		seen[selection] = true
		switch selection {
		case "gray", "full":
			if p.Release.ReleaseChannel != selection || p.Release.Status != model.PlatformArtifactReleaseStatusActive {
				return fail()
			}
		case "lkg":
			if p.Release.ReleaseChannel != "full" || p.LKG == nil || p.LKG.VerifiedByReleaseID != p.Release.ID || p.Release.VerificationState != model.PlatformArtifactVerificationStateVerified || p.Release.VerifiedLKGGeneration != p.Parent.Generation || !platformsafety.EvaluatePlatformLKGSnapshot(*p.LKG, p.Parent, keys, now).Pass || (p.Release.Status != model.PlatformArtifactReleaseStatusActive && p.Release.Status != model.PlatformArtifactReleaseStatusSuperseded) {
				return fail()
			}
		default:
			return fail()
		}
	}
	if !seen["lkg"] && p.LKG != nil {
		return fail()
	}
	return nil
}

func SelectionDigest(snapshot model.PlatformDNSRouteSourceSnapshot) (string, error) {
	snapshot.ObservedAt = time.Time{}
	snapshot.SelectionDigest = ""
	return platformconfig.Digest(snapshot)
}

// VerifySnapshot authenticates immutable inputs and checks declared selection
// bindings. The digest is not a signature of the mutable release ledger.
// Currentness still requires an authenticated API observation and a
// repeated selection-digest check after probing; this function cannot establish
// live authority from a persisted response or its timestamp alone.
func VerifySnapshot(snapshot model.PlatformDNSRouteSourceSnapshot, dns model.PlatformArtifact, keys bundleauth.Keyring, now time.Time) error {
	fail := func() error { return fmt.Errorf("DNS routing source snapshot binding invalid") }
	if dns.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || dns.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(dns, keys).Pass || snapshot.DNSArtifactID != dns.ID || snapshot.DNSArtifactDigest != dns.ContentHash || snapshot.ObservedAt.IsZero() {
		return fail()
	}
	if _, err := cellpublication.VerifyDNSArtifact(dns, keys); err != nil {
		return fail()
	}
	approvals, err := platformconfig.DNSRouteSourceAuthorizations(dns)
	if err != nil || len(approvals) == 0 || len(snapshot.Scopes) > 17 {
		return fail()
	}
	expected := map[string]bool{}
	for _, source := range approvals {
		expected[source.ScopeKey] = true
	}
	if len(snapshot.Scopes) != len(expected) {
		return fail()
	}
	for _, scope := range snapshot.Scopes {
		if !expected[scope.ScopeKey] || len(scope.Lanes) > 2 || len(scope.Publications) < 1 || len(scope.Publications) > 3 {
			return fail()
		}
		delete(expected, scope.ScopeKey)
		lanes := map[string]model.PlatformDNSRouteSourceLane{}
		for _, lane := range scope.Lanes {
			if lane.ReleaseChannel != "gray" && lane.ReleaseChannel != "full" || lane.FencingToken <= 0 || lane.Version <= 0 {
				return fail()
			}
			if _, exists := lanes[lane.ReleaseChannel]; exists {
				return fail()
			}
			lanes[lane.ReleaseChannel] = lane
		}
		seen := map[string]bool{}
		selected := map[string]bool{}
		for _, p := range scope.Publications {
			if p.Parent.ScopeKey != scope.ScopeKey || seen[p.Release.ID] || VerifyPublication(p, approvals, keys, now) != nil {
				return fail()
			}
			seen[p.Release.ID] = true
			for _, selection := range p.Selections {
				if selected[selection] {
					return fail()
				}
				selected[selection] = true
				if selection == "lkg" {
					continue
				}
				lane, exists := lanes[selection]
				if !exists || lane.ActiveReleaseID != p.Release.ID || lane.FencingToken != p.Release.FencingToken {
					return fail()
				}
			}
		}
	}
	digest, err := SelectionDigest(snapshot)
	if err != nil || digest != snapshot.SelectionDigest {
		return fail()
	}
	return nil
}
