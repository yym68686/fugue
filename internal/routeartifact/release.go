package routeartifact

import (
	"bytes"
	"encoding/json"
	"errors"
	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/trafficbinding"
	"io"
	"reflect"
	"sort"
)

// ProjectRelease verifies both immutable signatures and exact parent/child
// membership before attaching provenance. The mutable release is authenticated
// by assignment transport; consumers must re-read it before reporting/applying.
func ProjectRelease(parent, child model.PlatformArtifact, a model.PlatformConsumerAssignment, r model.PlatformArtifactRelease, keys bundleauth.Keyring) (model.EdgeRouteIntentSnapshot, error) {
	fail := func() (model.EdgeRouteIntentSnapshot, error) {
		return model.EdgeRouteIntentSnapshot{}, errors.New("traffic ReleaseSet or route binding invalid")
	}
	if parent.ArtifactKind != model.PlatformArtifactKindReleaseSet || child.ArtifactKind != model.PlatformArtifactKindEdgeRouteBundle || parent.ID == "" || child.ID == "" || parent.ID == child.ID || parent.Status != model.PlatformArtifactStatusValidated || child.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(parent, keys).Pass || !platformsafety.EvaluateArtifactIntegrity(child, keys).Pass {
		return fail()
	}
	if parent.ID != a.ReleaseSetID || child.ID != a.ArtifactID || a.ArtifactKind != child.ArtifactKind || parent.ScopeKey != child.ScopeKey || parent.ScopeKey != a.ScopeKey || a.ContentHash != child.ContentHash || a.ExpectedGeneration != child.Generation || a.GenerationSequence != child.GenerationSequence || a.ExpectedConsumerSetID == "" || a.Revision <= 0 || a.FencingToken <= 0 {
		return fail()
	}
	if r.ID != a.ArtifactReleaseID || r.ArtifactID != parent.ID || r.ArtifactKind != parent.ArtifactKind || r.ScopeKey != parent.ScopeKey || r.Generation != parent.Generation || r.Status != model.PlatformArtifactReleaseStatusActive || r.ReleaseChannel != a.ReleaseChannel || r.FencingToken != a.FencingToken {
		return fail()
	}
	if r.ReleaseChannel != model.PlatformArtifactReleaseChannelGray && r.CanaryRuleRef != "" {
		return fail()
	}
	var set platformconfig.ReleaseSet
	raw, err := json.Marshal(parent.Content)
	if err != nil {
		return fail()
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&set) != nil || decoder.Decode(&struct{}{}) != io.EOF || set.SchemaVersion != platformconfig.SchemaVersion || set.Generation != parent.Generation || set.Scope != parent.ScopeKey || len(set.ArtifactIDs) != 3 || len(set.ArtifactKinds) != 3 {
		return fail()
	}
	seenID, seenKind := map[string]bool{}, map[string]bool{}
	for i, id := range set.ArtifactIDs {
		kind := set.ArtifactKinds[i]
		if id == "" || id == parent.ID || seenID[id] || seenKind[kind] {
			return fail()
		}
		seenID[id], seenKind[kind] = true, true
		if kind == child.ArtifactKind && id != child.ID {
			return fail()
		}
	}
	if !seenID[child.ID] || !seenKind[model.PlatformArtifactKindEdgeRouteBundle] || !seenKind[model.PlatformArtifactKindDNSAnswerBundle] || !seenKind[model.PlatformArtifactKindCaddyRouteConfig] {
		return fail()
	}
	lineage := platformconfig.LineageFromArtifact(child)
	if !reflect.DeepEqual(set.Lineage, lineage) || !reflect.DeepEqual(platformconfig.LineageFromArtifact(parent), lineage) || child.Metadata["release_set_generation"] != parent.Generation {
		return fail()
	}
	var payload struct {
		Policy  platformconfig.PolicySnapshot `json:"policy"`
		Lineage platformconfig.Lineage        `json:"lineage"`
	}
	raw, err = json.Marshal(child.Content)
	if err != nil || json.Unmarshal(raw, &payload) != nil || !reflect.DeepEqual(payload.Lineage, lineage) || platformconfig.ValidatePolicySnapshot(payload.Policy) != nil {
		return fail()
	}
	policyDigest, err := platformconfig.Digest(payload.Policy)
	if err != nil || policyDigest != lineage.PolicyDigest || payload.Policy.Generation != lineage.PolicyGeneration || payload.Policy.Scope != a.ScopeKey || platformconfig.ValidateTrafficCohortProjection(parent, child) != nil {
		return fail()
	}
	snapshot, err := Project(child)
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	binding := &model.TrafficReleaseBinding{Schema: trafficbinding.Schema, ReleaseSetID: parent.ID, ReleaseSetDigest: parent.ContentHash, ReleaseSetGeneration: parent.Generation, RouteArtifactID: child.ID, RouteArtifactDigest: child.ContentHash, RouteArtifactGeneration: child.Generation, RouteArtifactSequence: child.GenerationSequence, ReleaseID: r.ID, ReleaseChannel: r.ReleaseChannel, FencingToken: r.FencingToken, ScopeKey: a.ScopeKey, IntentDigest: lineage.IntentDigest, PolicyDigest: lineage.PolicyDigest, InputSnapshotDigest: lineage.InputSnapshotDigest, CompilerVersion: lineage.CompilerVersion, ProjectionDigest: trafficbinding.ProjectionDigest(snapshot)}
	if r.ReleaseChannel == model.PlatformArtifactReleaseChannelGray {
		binding.EdgeGroupIDs, err = platformconfig.ResolveTrafficCanary(parent, r.CanaryRuleRef)
		if err != nil {
			return fail()
		}
		sort.Strings(binding.EdgeGroupIDs)
		binding.CanaryRuleRef = r.CanaryRuleRef
	}
	snapshot.TrafficRelease = binding
	if err := trafficbinding.ValidateProjection(snapshot); err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	return snapshot, nil
}
