package edgecontrol

import (
	"context"
	"errors"
	"time"
)

type AuthorityRuntime struct {
	RouteIntents RouteIntentSource
	Compiler     GroupShadowCompiler
	Publisher    GroupAuthorityPublisher
	Candidate    *GroupCandidatePublisher
	GroupIDs     []string
	Status       *AuthorityRuntimeState
}

type AuthorityRuntimeBatch struct {
	Compiled  GroupShadowBatch    `json:"compiled"`
	Published GroupAuthorityBatch `json:"published"`
	Candidate GroupCandidateBatch `json:"candidate,omitempty"`
}

func (runtime AuthorityRuntime) RunOnce(ctx context.Context) (AuthorityRuntimeBatch, error) {
	if runtime.RouteIntents == nil {
		return AuthorityRuntimeBatch{}, errors.New("edge-control authority RouteIntent source is nil")
	}
	snapshot, err := runtime.RouteIntents.FetchRouteIntents(ctx)
	if err != nil {
		return AuthorityRuntimeBatch{}, err
	}
	compiled, err := runtime.Compiler.Reconcile(ctx, snapshot, runtime.GroupIDs)
	if err != nil {
		return AuthorityRuntimeBatch{}, err
	}
	if runtime.Candidate != nil {
		candidate, err := runtime.Candidate.Publish(ctx, compiled)
		if err != nil {
			return AuthorityRuntimeBatch{}, err
		}
		return AuthorityRuntimeBatch{Compiled: compiled, Candidate: candidate}, nil
	}
	published, err := runtime.Publisher.Publish(ctx, compiled)
	if err != nil {
		return AuthorityRuntimeBatch{}, err
	}
	return AuthorityRuntimeBatch{Compiled: compiled, Published: published}, nil
}

type AuthorityRuntimeObservation struct {
	RouteIntentGeneration string `json:"route_intent_generation,omitempty"`
	Published             int    `json:"published"`
	Failed                int    `json:"failed"`
	CandidatePublished    int    `json:"candidate_published,omitempty"`
	FailureCode           string `json:"failure_code,omitempty"`
}

func (runtime AuthorityRuntime) Run(ctx context.Context, interval time.Duration, observe func(AuthorityRuntimeObservation)) error {
	if ctx == nil {
		return errors.New("edge-control authority runtime context is nil")
	}
	if interval <= 0 {
		return errors.New("edge-control authority runtime interval must be positive")
	}
	if observe == nil {
		observe = func(AuthorityRuntimeObservation) {}
	}
	if err := ctx.Err(); err != nil {
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		batch, err := runtime.RunOnce(ctx)
		observation := AuthorityRuntimeObservation{}
		if err != nil {
			observation.FailureCode = RouteIntentFailureCode(err)
		} else {
			if runtime.Candidate != nil {
				observation.RouteIntentGeneration = batch.Candidate.RouteIntentGeneration
				observation.CandidatePublished = batch.Candidate.Published
				observation.Failed = batch.Candidate.Failed
			} else {
				observation.RouteIntentGeneration = batch.Published.RouteIntentGeneration
				observation.Published = batch.Published.Published
				observation.Failed = batch.Published.Failed
			}
		}
		if runtime.Status != nil {
			runtime.Status.Observe(observation)
		}
		observe(observation)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
