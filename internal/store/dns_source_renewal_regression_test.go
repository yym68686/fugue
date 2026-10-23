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

func TestDNSIdenticalSourceRenewalRejectsChangedAuthority(t *testing.T) {
	for _, scenario := range []string{"static", "different artifact", "unverified", "old fence", "wrong channel", "missing history", "rolled back history", "unverified history", "wrong original fence", "time regression"} {
		t.Run(scenario, func(t *testing.T) {
			state, child := dnsSourceObservationFixture(t, true)
			previous, err := platformconfig.DecodePreviousTrafficPublication(child)
			if err != nil {
				t.Fatal(err)
			}
			dependencies, err := platformconfig.DNSRouteDependencies(child)
			if err != nil {
				t.Fatal(err)
			}
			var dependency platformconfig.DNSRouteDependency
			for _, d := range dependencies {
				if d.ReleaseID == previous.Reference.ReleaseID {
					dependency = d
					break
				}
			}
			index := platformArtifactReleaseIndex(state.PlatformArtifactReleases, dependency.ReleaseID)
			original := &state.PlatformArtifactReleases[index]
			selected := *original
			selected.ID += "-renewed"
			selected.FencingToken++
			selected.ReleasedAt = original.ReleasedAt.Add(time.Minute)
			original.Status = model.PlatformArtifactReleaseStatusSuperseded
			if !dnsIdenticalSourceRenewal(&state, child, dependency, selected) {
				t.Fatal("valid renewal fixture rejected")
			}
			switch scenario {
			case "static":
				var source platformconfig.CellDNSPlanSource
				raw, _ := json.Marshal(child.Content["cell_dns_source"])
				json.Unmarshal(raw, &source)
				source.Intent.DNSRouteSources = nil
				child.Content["cell_dns_source"] = source
			case "different artifact":
				selected.ArtifactID = "foreign"
			case "unverified":
				selected.VerificationState = model.PlatformArtifactVerificationStateServingUnverified
			case "old fence":
				selected.FencingToken = dependency.FencingToken
			case "wrong channel":
				selected.ReleaseChannel = "gray"
			case "missing history":
				original.ID = "foreign"
			case "rolled back history":
				original.Status = model.PlatformArtifactReleaseStatusRolledBack
			case "unverified history":
				original.VerificationState = model.PlatformArtifactVerificationStateFailed
			case "wrong original fence":
				original.FencingToken++
			case "time regression":
				selected.ReleasedAt = original.ReleasedAt
			}
			if dnsIdenticalSourceRenewal(&state, child, dependency, selected) {
				t.Fatal("changed authority accepted", scenario)
			}
		})
	}
}
