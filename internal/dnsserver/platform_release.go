package dnsserver

import (
	"encoding/json"
	"errors"
	"reflect"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/routeprobe"
)

type dnsServingPayload struct {
	Schema     string                           `json:"schema_version"`
	Generation string                           `json:"generation"`
	Records    []platformconfig.DNSIntent       `json:"records"`
	Views      []platformconfig.DNSConsumerView `json:"consumer_views"`
	Plan       *platformconfig.DNSReadinessPlan `json:"readiness_plan"`
	Queries    []platformconfig.DNSQueryView    `json:"query_views"`
	Policy     platformconfig.PolicySnapshot    `json:"policy"`
	Lineage    platformconfig.Lineage           `json:"lineage"`
}

func (s *Service) verifyDNSServingRelease(parent model.PlatformArtifact, c dnsPlatformCandidate) (dnsServingPayload, string, error) {
	fail := func() (dnsServingPayload, string, error) {
		return dnsServingPayload{}, "", errors.New("DNS serving ReleaseSet binding invalid")
	}
	a, child, r := c.Assignment, c.Artifact, c.Release
	if a.ReleaseChannel != "gray" && a.ReleaseChannel != "full" {
		return fail()
	}
	if _, err := s.verifyPlatformDNSCandidate(c, a); err != nil {
		return dnsServingPayload{}, "", err
	}
	keys := s.platformDNSKeys()
	if parent.ArtifactKind != model.PlatformArtifactKindReleaseSet || parent.ID != a.ReleaseSetID || parent.Status != model.PlatformArtifactStatusValidated || parent.ScopeKey != a.ScopeKey || parent.ScopeKey != r.ScopeKey || parent.Generation != r.Generation || !platformsafety.EvaluateArtifactIntegrity(parent, keys).Pass {
		return fail()
	}
	var set platformconfig.ReleaseSet
	raw, _ := json.Marshal(parent.Content)
	if json.Unmarshal(raw, &set) != nil || set.SchemaVersion != platformconfig.SchemaVersion || set.Generation != parent.Generation || set.Scope != parent.ScopeKey || len(set.ArtifactIDs) != 3 || len(set.ArtifactKinds) != 3 {
		return fail()
	}
	routeID := ""
	seen := map[string]bool{}
	ids := map[string]bool{}
	member := false
	for i, id := range set.ArtifactIDs {
		kind := set.ArtifactKinds[i]
		if id == "" || ids[id] || seen[kind] {
			return fail()
		}
		ids[id], seen[kind] = true, true
		switch kind {
		case model.PlatformArtifactKindEdgeRouteBundle:
			routeID = id
		case child.ArtifactKind:
			member = id == child.ID
		case model.PlatformArtifactKindCaddyRouteConfig:
		default:
			return fail()
		}
	}
	if routeID == "" || !member || !seen[model.PlatformArtifactKindCaddyRouteConfig] || !reflect.DeepEqual(set.Lineage, platformconfig.LineageFromArtifact(child)) || !reflect.DeepEqual(set.Lineage, platformconfig.LineageFromArtifact(parent)) || platformconfig.ValidateTrafficCohortProjection(parent, child) != nil {
		return fail()
	}
	if r.ReleaseChannel == "gray" {
		groups, err := platformconfig.ResolveTrafficCanary(parent, r.CanaryRuleRef)
		if err != nil || !platformconfig.TrafficCanaryContains(groups, s.Config.EdgeGroupID) {
			return fail()
		}
	} else if r.CanaryRuleRef != "" {
		return fail()
	}
	var p dnsServingPayload
	raw, _ = json.Marshal(child.Content)
	if json.Unmarshal(raw, &p) != nil || !reflect.DeepEqual(p.Lineage, set.Lineage) || p.Plan == nil || p.Policy.DNSReadiness == nil || len(p.Queries) == 0 || len(p.Policy.DNSAuthorities) == 0 {
		return fail()
	}
	consumers := map[string]*platformconfig.DNSConsumerIntent{}
	for _, v := range p.Views {
		c := consumers[v.NodeID]
		if c == nil {
			c = &platformconfig.DNSConsumerIntent{NodeID: v.NodeID}
			consumers[v.NodeID] = c
		}
		c.Zones = append(c.Zones, v.Zone)
	}
	declared := []platformconfig.DNSConsumerIntent{}
	for _, c := range consumers {
		declared = append(declared, *c)
	}
	if platformconfig.ValidateDNSAuthorityOwnership(p.Policy.DNSAuthorities, declared) != nil {
		return fail()
	}
	return p, routeID, nil
}
func (s *Service) platformDNSKeys() bundleauth.Keyring {
	return bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)
}

func dnsProofMatchesRelease(proof routeprobe.Proof, parent model.PlatformArtifact, c dnsPlatformCandidate, routeID string) bool {
	b := proof.TrafficRelease
	return b != nil && b.ReleaseSetID == parent.ID && b.ReleaseSetDigest == parent.ContentHash && b.RouteArtifactID == routeID && b.PolicyDigest == c.Artifact.Metadata["policy_digest"] && b.IntentDigest == c.Artifact.Metadata["intent_digest"] && b.InputSnapshotDigest == c.Artifact.Metadata["input_snapshot_digest"] && b.ReleaseID == c.Release.ID && b.ReleaseChannel == c.Release.ReleaseChannel && b.FencingToken == c.Release.FencingToken && b.ScopeKey == c.Assignment.ScopeKey
}
