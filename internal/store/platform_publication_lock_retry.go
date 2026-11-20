package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Only the automatic producer retries here. The same signed artifact and
// preparation proof stay in this call; the replacement transaction reruns
// all dependency, pin, keyring, authority, fence and freshness checks. No
// apply or commit error is classified at this boundary.
func (s *Store) consumePreparedPublicationInput(ctx context.Context, guard *platformProducerReleaseGuard, proof *compilerInputActivationProof, pins []compilerInputPinBinding, lock, apply func(context.Context, *sql.Tx) error) error {
	admit := func(ctx context.Context) error {
		return s.consumePreparedCompilerInput(ctx, proof, pins, false, lock, apply)
	}
	if guard == nil {
		return admit(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, platformPublicationTransactionTimeout)
	defer cancel()
	return retryPreApplyAdmissionLock(ctx, admit, func(err error) bool {
		var busy *publicationScopeLockBusy
		return errors.As(err, &busy)
	}, func(err error) {
		var busy *publicationScopeLockBusy
		if errors.As(err, &busy) {
			advancePlatformMutationTrace(ctx, "retry-"+busy.stage)
		}
	})
}

type publicationScopeLockBusy struct {
	stage string
	error
}

func (e *publicationScopeLockBusy) Error() string {
	return fmt.Sprintf("producer publication pre-apply %s: %v", e.stage, e.error)
}

func (e *publicationScopeLockBusy) Unwrap() error { return e.error }

func pgLockPublicationAdmissionScope(ctx context.Context, tx *sql.Tx, scope string, exclusive, producer bool, stage string) error {
	advancePlatformMutationTrace(ctx, stage)
	err := pgLockPromotionScope(ctx, tx, scope, exclusive)
	if !producer {
		return err
	}
	// Classify only the actual advisory-lock statement, not the preliminary
	// scope lookup (nor any later validation, mutation or commit).
	if isAdmissionLockTimeout(err) {
		return &publicationScopeLockBusy{stage: stage, error: err}
	}
	return err
}

func pgLockPublicationAdmissionPolicy(ctx context.Context, tx *sql.Tx, guard *platformProducerReleaseGuard) error {
	advancePlatformMutationTrace(ctx, "admission-read-producer-policy")
	scope, err := pgProducerPolicyScope(ctx, tx, guard)
	if err != nil {
		return err
	}
	return pgLockPublicationAdmissionScope(ctx, tx, scope, true, true, "admission-producer-policy-lock")
}

func pgLockPublicationAdmissionArtifact(ctx context.Context, tx *sql.Tx, id string, exclusive, producer bool) error {
	advancePlatformMutationTrace(ctx, "admission-read-publication-scope")
	scope, exclusive, err := pgPlatformReleaseMutationScope(ctx, tx, id, exclusive)
	if err != nil {
		return err
	}
	return pgLockPublicationAdmissionScope(ctx, tx, scope, exclusive, producer, "admission-output-scope-lock")
}
