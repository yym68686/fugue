package api

import (
	"context"
	"reflect"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

func consumerInputReadSelection(item consumerArtifactLookup, parent model.PlatformArtifact) store.PlatformInputReadSelection {
	return store.PlatformInputReadSelection{ArtifactID: item.Artifact.ID, ContentHash: item.Artifact.ContentHash,
		ParentID: parent.ID, ParentHash: parent.ContentHash, ReleaseID: item.Release.ID, ExpectedConsumerSetID: item.Assignment.ExpectedConsumerSetID}
}

// A DNS source observation returns dynamic publications in addition to the
// signed DNS artifact's fixed dependencies. Bind EVERY returned publication to
// its original digest/release in one complete input proof; never concatenate
// closure fragments from the observer's independent snapshots.
func dnsSourceInputReadSelections(snapshot model.PlatformDNSRouteSourceSnapshot) []store.PlatformInputReadSelection {
	reads := []store.PlatformInputReadSelection{}
	for _, scope := range snapshot.Scopes {
		for _, p := range scope.Publications {
			parent := store.PlatformInputReadSelection{ArtifactID: p.Parent.ID, ContentHash: p.Parent.ContentHash, ReleaseID: p.Release.ID}
			if p.LKG != nil {
				parent.LKGKind, parent.LKGScope, parent.LKG = p.LKG.ArtifactKind, p.LKG.ScopeKey, p.LKG
			}
			reads = append(reads, parent,
				store.PlatformInputReadSelection{ArtifactID: p.Route.ID, ContentHash: p.Route.ContentHash},
				store.PlatformInputReadSelection{ArtifactID: p.TLS.ID, ContentHash: p.TLS.ContentHash},
				store.PlatformInputReadSelection{ArtifactID: p.ProducerPolicy.ID, ContentHash: p.ProducerPolicy.ContentHash, ReleaseID: p.ProducerRelease.ID})
		}
	}
	return reads
}

// Check only the actually selected responses, after serving/canary filtering.
// The shared assignment resolver remains metadata-only: a cold shadow cannot
// turn a valid serving-only request into a blanket outage. A fresh resolver,
// including a fresh immutable-body cache, then rechecks ordinary authority.
func (s *Server) checkConsumerInputReads(ctx context.Context, claims platformcontrol.PlatformComponentIdentityClaims, selected []consumerArtifactLookup, read func(string) (model.PlatformArtifact, error), servingOnly bool) error {
	reads := make([]store.PlatformInputReadSelection, 0, len(selected))
	parents := make([]model.PlatformArtifact, 0, len(selected))
	for _, item := range selected {
		parent, err := read(item.Assignment.ReleaseSetID)
		if err != nil {
			return err
		}
		parents = append(parents, parent)
		reads = append(reads, consumerInputReadSelection(item, parent))
	}
	if err := s.store.CheckPlatformInputRead(ctx, reads); err != nil {
		return err
	}
	freshRead := newConsumerArtifactReader(s.store.GetPlatformArtifact)
	current, err := s.resolvePlatformConsumerAssignmentsWithReader(claims, freshRead)
	if err != nil {
		return err
	}
	for i, before := range selected {
		found := false
		for _, after := range current {
			if reflect.DeepEqual(before, after) {
				parent, err := freshRead(after.Assignment.ReleaseSetID)
				if err != nil || !reflect.DeepEqual(parent, parents[i]) {
					return store.ErrConflict
				}
				if servingOnly {
					set, err := s.store.GetPlatformExpectedConsumerSet(after.Assignment.ExpectedConsumerSetID)
					if err != nil {
						return err
					}
					group := ""
					for _, c := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
						if platformcontrol.ExpectedConsumerIdentityMatches(c, claims) {
							group = c.Cohort
							break
						}
					}
					ok, err := s.consumerAssignmentIsSelected(after, group, freshRead)
					if err != nil || !ok {
						return store.ErrConflict
					}
				}
				found = true
				break
			}
		}
		if !found {
			return store.ErrConflict
		}
	}
	return nil
}
