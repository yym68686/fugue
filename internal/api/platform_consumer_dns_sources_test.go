package api

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestDNSRouteSourceReadRechecksAssignmentAfterObservation(t *testing.T) {
	for _, scenario := range []string{"unchanged", "fence", "expected set", "release", "revoked", "foreign snapshot", "foreign digest", "admission rejected"} {
		t.Run(scenario, func(t *testing.T) {
			current := consumerArtifactLookup{Artifact: model.PlatformArtifact{ID: "dns", ContentHash: "digest"}, Assignment: model.PlatformConsumerAssignment{ArtifactID: "dns", FencingToken: 1, ExpectedConsumerSetID: "expected"}, Release: model.PlatformArtifactRelease{ID: "release"}}
			initial := current
			revoked := false
			var steps []string
			lookup := func() (consumerArtifactLookup, error) {
				steps = append(steps, "lookup")
				if revoked {
					return consumerArtifactLookup{}, store.ErrNotFound
				}
				return current, nil
			}
			observe := func(_ context.Context, owner consumerArtifactLookup) (model.PlatformDNSRouteSourceSnapshot, error) {
				steps = append(steps, "observe")
				if !reflect.DeepEqual(owner, initial) {
					t.Fatalf("observation must admit the original exact owner: got %+v", owner)
				}
				snapshot := model.PlatformDNSRouteSourceSnapshot{DNSArtifactID: "dns", DNSArtifactDigest: "digest"}
				switch scenario {
				case "fence":
					current.Assignment.FencingToken++
				case "expected set":
					current.Assignment.ExpectedConsumerSetID = "replacement"
				case "release":
					current.Release.ID = "next-release"
				case "revoked":
					revoked = true
				case "foreign snapshot":
					snapshot.DNSArtifactID = "foreign"
				case "foreign digest":
					snapshot.DNSArtifactDigest = "different"
				case "admission rejected":
					return model.PlatformDNSRouteSourceSnapshot{}, store.ErrConflict
				}
				return snapshot, nil
			}
			response, err := observeAssignedDNSRouteSources(context.Background(), lookup, observe)
			wantSteps := []string{"lookup", "observe", "lookup"}
			if scenario == "admission rejected" {
				wantSteps = wantSteps[:2]
			}
			if !reflect.DeepEqual(steps, wantSteps) {
				t.Fatalf("authority read order = %v, want %v", steps, wantSteps)
			}
			if scenario == "unchanged" {
				if err != nil || response.Snapshot.DNSArtifactID != "dns" {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, store.ErrConflict) || response.Snapshot.DNSArtifactID != "" {
				t.Fatal("changed assignment disclosed source snapshot", err)
			}
		})
	}
}
