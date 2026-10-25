package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestRetainedDNSRouteSourceObservationRechecksLKGAndOwnership(t *testing.T) {
	for _, scenario := range []string{"unchanged", "lkg renewed", "lkg replaced", "release changed", "assignment changed", "membership revoked", "foreign snapshot"} {
		t.Run(scenario, func(t *testing.T) {
			owner := consumerArtifactLookup{Artifact: model.PlatformArtifact{ID: "dns", ContentHash: "sha256:dns"}, Assignment: model.PlatformConsumerAssignment{ArtifactID: "dns", ExpectedConsumerSetID: "set", FencingToken: 2}, Release: model.PlatformArtifactRelease{ID: "full", FencingToken: 2}}
			lkg := model.PlatformLKGSnapshot{ID: "lkg", ArtifactID: "parent", VerifiedByReleaseID: "full", ExpiresAt: time.Now().Add(time.Hour)}
			revoked := false
			lookup := func() (consumerArtifactLookup, model.PlatformLKGSnapshot, error) {
				if revoked {
					return consumerArtifactLookup{}, model.PlatformLKGSnapshot{}, store.ErrNotFound
				}
				return owner, lkg, nil
			}
			observe := func(context.Context, model.PlatformArtifact) (model.PlatformDNSRouteSourceSnapshot, error) {
				snapshot := model.PlatformDNSRouteSourceSnapshot{DNSArtifactID: "dns", DNSArtifactDigest: "sha256:dns"}
				switch scenario {
				case "lkg renewed":
					lkg.ExpiresAt = lkg.ExpiresAt.Add(time.Hour)
				case "lkg replaced":
					lkg.ID = "replacement"
				case "release changed":
					owner.Release.FencingToken++
				case "assignment changed":
					owner.Assignment.ExpectedConsumerSetID = "next-set"
				case "membership revoked":
					revoked = true
				case "foreign snapshot":
					snapshot.DNSArtifactID = "other"
				}
				return snapshot, nil
			}
			response, err := observeRetainedDNSRouteSources(context.Background(), lookup, observe)
			if scenario == "unchanged" {
				if err != nil || response.RetainedLKG.ID != "lkg" {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, store.ErrConflict) || response.RetainedLKG.ID != "" {
				t.Fatalf("raced retained authorization disclosed source: %+v %v", response, err)
			}
		})
	}
}
