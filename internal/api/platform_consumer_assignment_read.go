package api

import (
	"context"
	"errors"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
	"fugue/internal/store"
)

// A release can finish verification while discovery is reading it. Never
// disclose a mixed observation or relax the final DeepEqual: discard the whole
// attempt and repeat all checks once, with a new immutable-body read session.
// A cold input, database error, absent assignment or selection error is not
// evidence of this race and must not trigger a retry.
func (s *Server) readCheckedConsumerAssignments(ctx context.Context, claims platformcontrol.PlatformComponentIdentityClaims, servingOnly bool, started time.Time, newSession func() func(string) (model.PlatformArtifact, error)) ([]consumerArtifactLookup, string, error) {
	for attempt := 0; ; attempt++ {
		attemptCtx := withConsumerInputReadObservation(ctx)
		items, stage, err := s.readConsumerAssignmentAttempt(attemptCtx, claims, servingOnly, newSession())
		if err == nil {
			if attempt > 0 {
				outcome := "consumer-assignment-retry-empty"
				if len(items) > 0 {
					outcome = "consumer-assignment-retry-ok"
				}
				s.operationObservations().Observe(outcome, 0)
			}
			return items, "", nil
		}
		// Keep the original failed attempt even if its complete reread succeeds.
		// The failure ring is attempt coverage, not an HTTP failure-rate counter.
		s.retainConsumerRequestFailure(attemptCtx, started, "assignment", stage, err)
		detail := consumerInputReadFailureFromContext(attemptCtx)
		changed := stage == "input_recheck" && errors.Is(err, store.ErrConflict) && detail != nil && detail.Stage == "assignment_changed"
		if !changed || ctx.Err() != nil {
			return nil, stage, err
		}
		if attempt > 0 {
			s.operationObservations().Observe("consumer-assignment-retry-exhausted", 0)
			return nil, stage, err
		}
		s.operationObservations().Observe("consumer-assignment-retry", 0)
	}
}

func (s *Server) readConsumerAssignmentAttempt(ctx context.Context, claims platformcontrol.PlatformComponentIdentityClaims, servingOnly bool, load func(string) (model.PlatformArtifact, error)) ([]consumerArtifactLookup, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "input_recheck", err
	}
	read := newConsumerArtifactReader(load)
	bindings := routeartifact.NewRequestBindingReader()
	items, err := s.resolvePlatformConsumerAssignmentsWithReader(claims, read)
	if err != nil {
		return nil, "resolve", err
	}
	if servingOnly {
		items, err = selectServingConsumerAssignments(items, func() ([]consumerArtifactLookup, error) {
			return s.resolvePlatformConsumerAssignmentsWithReader(claims, read)
		}, func(item consumerArtifactLookup) (bool, error) {
			set, err := s.store.GetPlatformExpectedConsumerSet(item.Assignment.ExpectedConsumerSetID)
			if err != nil {
				return false, errors.New("serving topology unavailable")
			}
			group := ""
			for _, member := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
				if platformcontrol.ExpectedConsumerIdentityMatches(member, claims) {
					group = member.Cohort
					break
				}
			}
			selected, err := s.consumerAssignmentIsSelectedWithBindingReader(item, group, read, bindings)
			if err != nil {
				return false, errors.New("serving release unavailable")
			}
			return selected, nil
		})
		if err != nil {
			return nil, "serving_selection", err
		}
	}
	if len(items) == 0 {
		return nil, "", nil
	}
	if err := s.checkConsumerInputReadsWithLoader(ctx, claims, items, read, servingOnly, load); err != nil {
		return nil, "input_recheck", err
	}
	return items, "", nil
}
