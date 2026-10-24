package celldns

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// AdvanceSourcePolicy supplies a newly signed publication from the same
// authorized producer, including coherent parent/child policy lineage.
func AdvanceSourcePolicy(t testing.TB, input model.PlatformDNSRouteSourceSnapshot, mutate func(*platformconfig.PolicySnapshot)) model.PlatformDNSRouteSourceSnapshot {
	t.Helper()
	raw, _ := json.Marshal(input)
	var snapshot model.PlatformDNSRouteSourceSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	for i := range snapshot.Scopes {
		scope := &snapshot.Scopes[i]
		pub := &scope.Publications[0]
		var policy platformconfig.PolicySnapshot
		raw, _ = json.Marshal(pub.Route.Content["policy"])
		if err := json.Unmarshal(raw, &policy); err != nil {
			t.Fatal(err)
		}
		policy.Generation += "-next"
		mutate(&policy)
		if err := platformconfig.ValidatePolicySnapshot(policy); err != nil {
			t.Fatal(err)
		}
		lineage := platformconfig.LineageFromArtifact(pub.Parent)
		lineage.PolicyGeneration = policy.Generation
		lineage.PolicyDigest, _ = platformconfig.Digest(policy)
		pub.Parent.Generation += "-next"
		oldIDs := map[string]string{}
		for _, a := range []*model.PlatformArtifact{&pub.Route, &pub.TLS} {
			oldID := a.ID
			a.Generation += "-next"
			a.Content["policy"], a.Content["lineage"] = policy, lineage
			for k, v := range platformconfig.LineageMetadata(lineage) {
				a.Metadata[k] = v
			}
			a.Metadata["release_set_generation"] = pub.Parent.Generation
			raw, _ = json.Marshal(a.Content)
			var content map[string]any
			if err := json.Unmarshal(raw, &content); err != nil {
				t.Fatal(err)
			}
			a.Content = content

			*a = Sign(t, *a, oldID+"-next", a.GenerationSequence+1)
			oldIDs[oldID] = a.ID
		}
		var set platformconfig.ReleaseSet
		raw, _ = json.Marshal(pub.Parent.Content)
		if err := json.Unmarshal(raw, &set); err != nil {
			t.Fatal(err)
		}
		set.Generation, set.Lineage = pub.Parent.Generation, lineage
		for j, id := range set.ArtifactIDs {
			if next := oldIDs[id]; next != "" {
				set.ArtifactIDs[j] = next
			} else {
				set.ArtifactIDs[j] = id + "-next"
			}
		}
		parent := platformconfig.BuildReleaseSetArtifact(set, set.ArtifactIDs, pub.Parent.CreatedAt)
		for k, v := range pub.Parent.Metadata {
			if _, ok := parent.Metadata[k]; !ok {
				parent.Metadata[k] = v
			}
		}
		pub.Parent = Sign(t, parent, pub.Parent.ID+"-next", pub.Parent.GenerationSequence+1)

		pub.Release.ID += "-next"
		pub.Release.ArtifactID, pub.Release.Generation = pub.Parent.ID, pub.Parent.Generation
		pub.Release.FencingToken++
		scope.Lanes[0].ActiveReleaseID = pub.Release.ID
		scope.Lanes[0].FencingToken = pub.Release.FencingToken
		scope.Lanes[0].Version++
	}
	snapshot.SelectionDigest = ""
	digestInput := snapshot
	digestInput.ObservedAt = time.Time{}
	snapshot.SelectionDigest, _ = platformconfig.Digest(digestInput)
	return snapshot
}
