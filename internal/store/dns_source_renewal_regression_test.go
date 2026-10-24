package store

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

// Reproduce a publisher renewing an identical full artifact while DNS has an
// explicit dynamic source authorization and retains its immutable baseline.
func TestDNSDynamicSourceIdenticalVerifiedRenewal(t *testing.T) {
	state, child := dnsSourceObservationFixture(t, true)
	previous, err := platformconfig.DecodePreviousTrafficPublication(child)
	if err != nil {
		t.Fatal(err)
	}
	state.PlatformArtifacts = append(state.PlatformArtifacts, previous.DNS)
	if err = validateCellDNSReferencesInState(&state, child, celldns.Keys()); err != nil {
		t.Fatal("baseline invalid", err)
	}
	index := platformArtifactReleaseIndex(state.PlatformArtifactReleases, previous.Reference.ReleaseID)
	if index < 0 {
		t.Fatal("original full release missing")
	}
	old := state.PlatformArtifactReleases[index]
	next := old
	next.ID += "-renewed"
	next.FencingToken++
	next.ReleasedAt = old.ReleasedAt.Add(time.Minute)
	next.VerificationState = model.PlatformArtifactVerificationStateServingUnverified
	next.VerifiedLKGGeneration = ""
	state.PlatformArtifactReleases[index].Status = model.PlatformArtifactReleaseStatusSuperseded
	state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, next)
	for i := range state.PlatformReleaseLanes {
		lane := &state.PlatformReleaseLanes[i]
		if lane.LaneKey == next.LaneKey {
			lane.ActiveReleaseID = next.ID
			lane.FencingToken = next.FencingToken
			lane.Version++
		}
	}
	err = validateCellDNSReferencesInState(&state, child, celldns.Keys())
	if err != nil {
		t.Fatalf("same immutable verified artifact renewal blocked DNS: %v", err)
	}
}

// Advance a signed source to a different immutable parent and route/TLS
// artifacts under the same approved producer. No DNS content is recompiled.
func advancedDNSRouteSource(t *testing.T, p platformconfig.PreviousTrafficPublicationInput) ([]model.PlatformArtifact, model.PlatformArtifactRelease) {
	t.Helper()
	raw, _ := json.Marshal(p)
	var next platformconfig.PreviousTrafficPublicationInput
	if err := json.Unmarshal(raw, &next); err != nil {
		t.Fatal(err)
	}
	generation := p.Parent.Generation + "-next"
	for _, a := range []*model.PlatformArtifact{&next.Route, &next.TLS, &next.DNS} {
		a.Metadata["release_set_generation"] = generation
		a.Generation += "-next"
		if _, ok := a.Content["generation"]; ok {
			a.Content["generation"] = a.Generation
		}
		*a = celldns.Sign(t, *a, a.ID+"-next", a.GenerationSequence+1)
	}
	next.Parent.Generation = generation
	next.Parent.Content["generation"] = generation
	next.Parent.Content["artifact_ids"] = []string{next.Route.ID, next.DNS.ID, next.TLS.ID}
	next.Parent = celldns.Sign(t, next.Parent, next.Parent.ID+"-next", next.Parent.GenerationSequence+1)
	r := celldns.PreviousPublication(p)
	r.ID += "-next"
	r.ArtifactID, r.Generation = next.Parent.ID, next.Parent.Generation
	r.FencingToken++
	r.ReleasedAt = time.Now().UTC().Add(-time.Second)
	r.VerificationState, r.VerifiedLKGGeneration = model.PlatformArtifactVerificationStateServingUnverified, ""
	return []model.PlatformArtifact{next.Parent, next.Route, next.TLS, next.DNS}, r
}

func TestDNSDynamicSourceAdvanceAndHistoricalBoundaries(t *testing.T) {
	for _, scenario := range []string{"authorized advance", "static reference", "failed source retains LKG", "missing history", "rolled back history", "failed history", "unverified previous", "wrong history fence", "wrong history parent", "future history", "frozen source", "failed only source", "rolled back source", "foreign producer", "tampered current route", "expired only LKG"} {
		t.Run(scenario, func(t *testing.T) {
			state, child := dnsSourceObservationFixture(t, true)
			previous, err := platformconfig.DecodePreviousTrafficPublication(child)
			if err != nil {
				t.Fatal(err)
			}
			state.PlatformArtifacts = append(state.PlatformArtifacts, previous.DNS)
			artifacts, next := advancedDNSRouteSource(t, *previous)
			state.PlatformArtifacts = append(state.PlatformArtifacts, artifacts...)
			i := platformArtifactReleaseIndex(state.PlatformArtifactReleases, previous.Reference.ReleaseID)
			state.PlatformArtifactReleases[i].Status = model.PlatformArtifactReleaseStatusSuperseded
			state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, next)
			for j := range state.PlatformReleaseLanes {
				lane := &state.PlatformReleaseLanes[j]
				if lane.LaneKey == next.LaneKey {
					lane.ActiveReleaseID, lane.FencingToken = next.ID, next.FencingToken
					lane.Version++
				}
			}
			if err := validateCellDNSReferencesInState(&state, child, celldns.Keys()); err != nil {
				t.Fatal("approved producer advance blocked DNS", err)
			}
			original := &state.PlatformArtifactReleases[i]
			current := &state.PlatformArtifactReleases[len(state.PlatformArtifactReleases)-1]
			switch scenario {
			case "authorized advance":
				return
			case "static reference":
				req, _, _ := celldns.SourceAuthorizedRequest(t, true)
				req.Intent.DNSRouteSources = nil
				for i := range req.CellRoutePublications {
					req.CellRoutePublications[i].ProducerPolicy = nil
				}
				req.PreviousTrafficPublication.ProducerPolicy = nil
				child = celldns.Compile(t, req).DNSArtifact
			case "failed source retains LKG":
				current.VerificationState = model.PlatformArtifactVerificationStateFailed
				if err := validateCellDNSReferencesInState(&state, child, celldns.Keys()); err != nil {
					t.Fatal("failed candidate invalidated positive LKG", err)
				}
				return
			case "missing history":
				original.ID = "missing"
			case "rolled back history":
				original.Status = model.PlatformArtifactReleaseStatusRolledBack
			case "failed history":
				original.VerificationState = model.PlatformArtifactVerificationStateFailed
			case "unverified previous":
				original.VerificationState = model.PlatformArtifactVerificationStateServingUnverified
			case "wrong history fence":
				original.FencingToken++
			case "wrong history parent":
				original.ArtifactID = "foreign"
			case "future history":
				original.ReleasedAt = time.Now().Add(time.Hour)
			case "frozen source":
				for j := range state.PlatformReleaseLanes {
					if state.PlatformReleaseLanes[j].LaneKey == next.LaneKey {
						state.PlatformReleaseLanes[j].Frozen = true
					}
				}
			case "failed only source":
				current.VerificationState = model.PlatformArtifactVerificationStateFailed
				state.PlatformLKGSnapshots = nil
			case "rolled back source":
				current.Status = model.PlatformArtifactReleaseStatusRolledBack
			case "foreign producer":
				j := platformArtifactIndex(state.PlatformArtifacts, next.ArtifactID)
				state.PlatformArtifacts[j].Metadata[platformconfig.ProducerPolicyReleaseMetadata] = "producer-release-authority-cell-cell-a"
				state.PlatformArtifacts[j] = celldns.Sign(t, state.PlatformArtifacts[j], next.ArtifactID, artifacts[0].GenerationSequence)
				state.PlatformLKGSnapshots = nil
			case "tampered current route":
				j := platformArtifactIndex(state.PlatformArtifacts, artifacts[1].ID)
				state.PlatformArtifacts[j].Provenance.Signature = "forged"
			case "expired only LKG":
				for j := range state.PlatformReleaseLanes {
					if state.PlatformReleaseLanes[j].LaneKey == next.LaneKey {
						state.PlatformReleaseLanes[j].ActiveReleaseID = ""
					}
				}
				for j := range state.PlatformLKGSnapshots {
					state.PlatformLKGSnapshots[j].ExpiresAt = time.Now().Add(-time.Second)
				}
			}
			if err := validateCellDNSReferencesInState(&state, child, celldns.Keys()); err == nil {
				t.Fatal("invalid authority accepted")
			}
		})
	}
}
