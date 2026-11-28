package productioncontrol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type sampleRetentionCheckpoint struct {
	Scope      Scope     `json:"scope"`
	SystemID   string    `json:"systemId"`
	Database   string    `json:"database"`
	TransferAt time.Time `json:"transferAt"`
}

func sampleRetentionBaselineVersion(scope Scope, raw []byte) (Version, error) {
	c, err := sampleRetentionBaselineCheckpoint(scope, raw)
	if err != nil {
		return Version{}, err
	}
	data, err := signedBytes("fugue.production.sample-retention-baseline.v1", c)
	if err != nil {
		return Version{}, err
	}
	return Version{Generation: 1, Digest: digest(data)}, nil
}

func sampleRetentionBaselineCheckpoint(scope Scope, raw []byte) (sampleRetentionCheckpoint, error) {
	var c sampleRetentionCheckpoint
	p := SampleRetentionProvider{Role: "sample-observer"}
	if err := decodeControlJSON(raw, &c); err != nil {
		return sampleRetentionCheckpoint{}, err
	}
	p.SystemID, p.Database = c.SystemID, c.Database
	if c.Scope != scope || scope.ResourceID != ObservationSampleRetentionResource || !scope.valid() ||
		!p.valid() || c.TransferAt.IsZero() {
		return sampleRetentionCheckpoint{}, errors.New("invalid sample retention baseline snapshot")
	}
	c.TransferAt = c.TransferAt.UTC()
	canonical, err := json.Marshal(c)
	if err != nil || !bytes.Equal(canonical, raw) {
		return sampleRetentionCheckpoint{}, errors.New("noncanonical sample retention baseline snapshot")
	}
	return c, nil
}

func checkSampleRetentionBaselineAtAppend(ctx context.Context, tx *sql.Tx, scope Scope, snapshot []byte) error {
	c, err := sampleRetentionBaselineCheckpoint(scope, snapshot)
	if err != nil {
		return err
	}
	if err := lockResource(ctx, tx, scope); err != nil {
		return err
	}
	var matched bool
	err = tx.QueryRowContext(ctx, `SELECT NOT pg_catalog.pg_is_in_recovery()
		AND system.system_identifier::text=$1 AND pg_catalog.current_database()=$2
		AND counts.total=3 AND counts.active=3 AND counts.targets=3
		FROM pg_catalog.pg_control_system() system CROSS JOIN (
			SELECT count(*) AS total,
				count(DISTINCT target) FILTER (WHERE target IN ('network','performance','resource-usage')) AS targets,
				count(*) FILTER (WHERE owner='worker' AND fence_enforced AND completed_passes>0
					AND transfer_at=$3 AND completed_at >= $3) AS active
			FROM fugue_observation_maintenance.sample_worker_progress
		) counts`, c.SystemID, c.Database, c.TransferAt).Scan(&matched)
	if err != nil || !matched {
		return errors.Join(ErrResourceDrift, err)
	}
	return nil
}

// DraftSampleRetentionBaseline requires every target to have made durable
// progress since transfer. The signed checkpoint is stable across later passes.
func DraftSampleRetentionBaseline(scope Scope, observed SampleRetentionObservation, authority, witnessKey string, now time.Time) (ExecutionWitness, error) {
	w, _, err := draftSampleRetentionBaseline(scope, observed, authority, witnessKey, now)
	return w, err
}

func draftSampleRetentionBaseline(scope Scope, observed SampleRetentionObservation, authority, witnessKey string, now time.Time) (ExecutionWitness, []byte, error) {
	o, err := sampleRetentionObservation(observed.Provider, observed.TransferAt, true, observed.Targets)
	if err != nil || o.State != SampleRetentionWorkerObserved || o.Resource != observed.Resource ||
		!observed.Provider.valid() || now.IsZero() || now.Before(o.TransferAt) ||
		!tokenPattern.MatchString(witnessKey) || !digestPattern.MatchString(authority) {
		return ExecutionWitness{}, nil, errors.New("sample retention baseline requires worker progress on all targets and a revocation evidence digest")
	}
	for _, target := range o.Targets {
		if target.CompletedAt.After(now) {
			return ExecutionWitness{}, nil, errors.New("sample retention pass completes after the observation time")
		}
	}
	c := sampleRetentionCheckpoint{scope, o.Provider.SystemID, o.Provider.Database, o.TransferAt.UTC()}
	snapshot, err := json.Marshal(c)
	if err != nil {
		return ExecutionWitness{}, nil, err
	}
	version, err := sampleRetentionBaselineVersion(scope, snapshot)
	if err != nil {
		return ExecutionWitness{}, nil, err
	}
	result, err := signedBytes("fugue.production.sample-retention-live.v1", c)
	if err != nil {
		return ExecutionWitness{}, nil, err
	}
	return ExecutionWitness{
		Kind: BaselineWitness, KeyID: witnessKey, ClusterID: scope.ClusterID, ResourceID: scope.ResourceID,
		Generation: version.Generation, VerifiedDigest: version.Digest, ObservationDigest: digest(result),
		AuthorityDigest: authority, ObservedAt: now.Unix(),
	}, snapshot, nil
}

func (j *Journal) ImportSampleRetentionBaseline(ctx context.Context, baseline Baseline, provider SampleRetentionProvider,
	db *sql.DB, authority string, source WitnessReader, witnessID string, witnessKey ed25519.PublicKey) (bool, error) {
	if j == nil || baseline.ResourceID != ObservationSampleRetentionResource || !provider.valid() || db == nil ||
		!digestPattern.MatchString(authority) || !digestPattern.MatchString(baseline.EvidenceDigest) {
		return false, errors.New("invalid sample retention baseline import")
	}
	stored, err := j.ReadResource(ctx, baseline.Scope)
	if err == nil {
		if stored.Baseline != baseline {
			return false, ErrBaselineConflict
		}
		_, err := readBaselineSnapshot(ctx, j.db, baseline.Scope, stored.Verified)
		return false, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	var snapshot []byte
	e, err := NewObservedExecutionJournal(j, source,
		map[string]TrustedWitness{witnessID: {Scope: baseline.Scope, PublicKey: witnessKey}},
		func(ctx context.Context, signed ExecutionWitness) error {
			live, err := provider.Observe(ctx, db)
			if err != nil {
				return err
			}
			current, raw, err := draftSampleRetentionBaseline(baseline.Scope, live, authority, witnessID, j.now())
			if err != nil {
				return err
			}
			if signed.Kind != BaselineWitness || signed.version() != current.version() ||
				signed.ObservationDigest != current.ObservationDigest || signed.AuthorityDigest != authority ||
				signed.ObservedAt < live.TransferAt.Unix() {
				return errors.New("sample retention witness differs from live transfer or reviewed legacy revocation")
			}
			snapshot = raw
			return nil
		})
	if err != nil {
		return false, err
	}
	return e.anchorResource(ctx, "sample-retention-observer", baseline, func() []byte { return snapshot })
}
