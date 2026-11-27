package store

import (
	"context"
	"database/sql"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func dnsProducerSelection(read dnsRouteSourceReader, kind, scope, channel string, pin model.PlatformPublicationPrecondition, keys bundleauth.Keyring) (model.PlatformArtifact, model.PlatformArtifactRelease, error) {
	fail := func() (model.PlatformArtifact, model.PlatformArtifactRelease, error) {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, ErrConflict
	}
	key := platformsafety.ReleaseLaneKey(kind, scope, channel)
	lane, err := read.lane(key)
	if err != nil || lane.LaneKey != key || lane.ArtifactKind != kind || lane.ScopeKey != scope || lane.ReleaseChannel != channel || lane.Frozen || lane.ActiveReleaseID != pin.ReleaseID || lane.FencingToken != pin.FencingToken || pin.FencingToken <= 0 {
		return fail()
	}
	r, err := read.release(pin.ReleaseID)
	if err != nil || r.ArtifactID != pin.ArtifactID || r.ArtifactKind != kind || r.ScopeKey != scope || r.ReleaseChannel != channel || r.LaneKey != key || r.Status != model.PlatformArtifactReleaseStatusActive || r.FencingToken != pin.FencingToken {
		return fail()
	}
	a, err := read.artifact(pin.ArtifactID)
	if err != nil || a.ArtifactKind != kind || a.ScopeKey != scope || a.Generation != r.Generation || a.ContentHash != pin.ContentHash || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
		return fail()
	}
	return a, r, nil
}

func validateDNSProducerPolicySelection(read dnsRouteSourceReader, pin model.PlatformPublicationPrecondition, keys bundleauth.Keyring) error {
	a, _, err := dnsProducerSelection(read, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow", pin, keys)
	if err != nil {
		return err
	}
	p, err := platformproducer.Decode(a)
	if err != nil || p.TargetScope != "global" || p.PublicationRole != "" || p.Mode != "serving" || p.Serving == nil || p.Serving.SinglePublication {
		return ErrConflict
	}
	return nil
}

// Capture has one coherent read-only witness. Publication separately repeats the
// policy selection under the already established DNS/source authority locks.
func (s *Store) ValidateDNSProducerSelection(ctx context.Context, producer, full model.PlatformPublicationPrecondition) error {
	return s.validateDNSProducerSelection(ctx, producer, full, false)
}

// The ordinary DNS producer and per-host programs must never call this
// exception. It is only an input for the global producer's custody handoff.
func (s *Store) ValidateHostedFleetHandoffSelection(ctx context.Context, producer, full model.PlatformPublicationPrecondition) error {
	return s.validateDNSProducerSelection(ctx, producer, full, true)
}

func (s *Store) validateDNSProducerSelection(ctx context.Context, producer, full model.PlatformPublicationPrecondition, handoff bool) error {
	keys := s.platformArtifactSigningKeyring()
	check := func(read dnsRouteSourceReader) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validateDNSProducerPolicySelection(read, producer, keys); err != nil {
			return err
		}
		a, r, err := dnsProducerSelection(read, model.PlatformArtifactKindReleaseSet, "global", "full", full, keys)
		if err != nil || r.VerificationState != model.PlatformArtifactVerificationStateVerified || r.VerifiedLKGGeneration != a.Generation {
			return ErrConflict
		}
		lkg, err := read.lkg("global")
		if err != nil || lkg == nil || lkg.VerifiedByReleaseID != r.ID || !platformsafety.EvaluatePlatformLKGSnapshot(*lkg, a, keys, time.Now().UTC()).Pass {
			return ErrConflict
		}
		if handoff {
			return validateHostedFleetHandoff(read, producer.ReleaseID, a, r, *lkg, keys)
		}
		if a.Metadata[platformproducer.PolicyReleaseMetadata] != producer.ReleaseID {
			return ErrConflict
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = check(pgDNSRouteSourceReader(ctx, tx)); err != nil {
		return err
	}
	return tx.Commit()
}
