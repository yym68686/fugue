package api

import (
	"context"
	"errors"
	"reflect"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

func consumerInputReadAuthorityChanged(ctx context.Context, err error) bool {
	detail := consumerInputReadFailureFromContext(ctx)
	return errors.Is(err, store.ErrConflict) && detail != nil &&
		(detail.Stage == "assignment_changed" || detail.Stage == "parent_changed")
}

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
	return s.checkConsumerInputReadsWithLoader(ctx, claims, selected, read, servingOnly, s.store.GetPlatformArtifact)
}

func (s *Server) checkConsumerInputReadsWithLoader(ctx context.Context, claims platformcontrol.PlatformComponentIdentityClaims, selected []consumerArtifactLookup, read func(string) (model.PlatformArtifact, error), servingOnly bool, load func(string) (model.PlatformArtifact, error)) error {
	reads := make([]store.PlatformInputReadSelection, 0, len(selected))
	parents := make([]model.PlatformArtifact, 0, len(selected))
	for _, item := range selected {
		parent, err := read(item.Assignment.ReleaseSetID)
		if err != nil {
			observeConsumerInputReadFailure(ctx, "selected_parent_read", store.PlatformInputReadFailure{}, nil, nil)
			return err
		}
		parents = append(parents, parent)
		reads = append(reads, consumerInputReadSelection(item, parent))
	}
	if detail, err := s.store.CheckPlatformInputReadObserved(ctx, reads); err != nil {
		observeConsumerInputReadFailure(ctx, "store_input_read", detail, nil, nil)
		return err
	}
	freshRead := newConsumerArtifactReader(load)
	current, err := s.resolveConsumerAssignmentLanes(ctx, claims, freshRead, servingOnly)
	if err != nil {
		observeConsumerInputReadFailure(ctx, "fresh_authority_resolve", store.PlatformInputReadFailure{}, nil, nil)
		return err
	}
	for i, before := range selected {
		found := false
		for _, after := range current {
			// Request-scoped immutable bodies share map identity only after a
			// fresh database digest and mutation check. DeepEqual still checks
			// every fresh envelope, assignment and release field.
			if reflect.DeepEqual(before, after) {
				parent, err := freshRead(after.Assignment.ReleaseSetID)
				if err != nil || !reflect.DeepEqual(parent, parents[i]) {
					stage := "parent_changed"
					if err != nil {
						stage = "fresh_parent_read"
					}
					observeConsumerInputReadFailure(ctx, stage, store.PlatformInputReadFailure{}, nil, nil)
					return store.ErrConflict
				}
				if servingOnly {
					set, err := s.store.GetPlatformExpectedConsumerSetContext(ctx, after.Assignment.ExpectedConsumerSetID)
					if err != nil {
						observeConsumerInputReadFailure(ctx, "fresh_expected_set", store.PlatformInputReadFailure{}, nil, nil)
						return err
					}
					group := ""
					for _, c := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
						if platformcontrol.ExpectedConsumerIdentityMatches(c, claims) {
							group = c.Cohort
							break
						}
					}
					var observe consumerInputSelectionObserver
					var observation consumerSelectionRead
					var timing *consumerSelectionRead
					if claims.Component == model.PlatformConsumerComponentDNSServer {
						timing = &observation
						observe = func(comparison *capturePublicationPair, fields ...string) {
							observeConsumerInputSelectionFailure(ctx, comparison, fields...)
						}
					}
					ok, dnsPublicationChanged, err := s.checkConsumerAssignmentSelection(after, group, freshRead, nil, observe, timing)
					if err != nil || !ok {
						stage := "serving_not_selected"
						if err != nil {
							stage = "serving_selection_read"
						} else if timing != nil {
							recordConsumerSelectionRead(ctx, after.Assignment, observation, true)
						}
						observeConsumerInputReadFailure(ctx, stage, store.PlatformInputReadFailure{}, nil, nil)
						if err == nil && dnsPublicationChanged && claims.Component == model.PlatformConsumerComponentDNSServer {
							return errConsumerDNSPublicationChanged
						}
						return store.ErrConflict
					}
				}
				found = true
				break
			}
		}
		if !found {
			var sameLane *consumerArtifactLookup
			for j := range current {
				if current[j].Assignment.ArtifactKind == before.Assignment.ArtifactKind && current[j].Assignment.ReleaseChannel == before.Assignment.ReleaseChannel {
					sameLane = &current[j]
					break
				}
			}
			stage := "assignment_absent"
			if sameLane != nil {
				stage = "assignment_changed"
			}
			observeConsumerInputReadFailure(ctx, stage, store.PlatformInputReadFailure{}, &before, sameLane)
			return store.ErrConflict
		}
	}
	return nil
}
